#!/usr/bin/env python3
"""
P0 —— 横截面趋势可选性检验（Cross-Sectional Momentum 证据层）。

回答一个决定性问题：把多标的按「趋势强度」排名，做多最强的，到底赚不赚钱？
  · Rank-IC > 0  → 越强越接着强 → 该做【横截面动量】(做多头部)
  · Rank-IC < 0  → 越强越回落   → 该做【横截面反转】(做多尾部/做空头部)
  · Rank-IC ≈ 0  → 趋势排名对未来无区分力

同时对比：
  · 原始趋势分 vs 对市场(等权均值)残差化后的【特质趋势分】(解决标的高度相关)
  · Top 分位 − Bottom 分位 的未来收益价差(可直接交易的多空价差)
  · 分时段的 Rank-IC(哪个时段横截面有效)

趋势分定义(专业做法，风险调整 × 质量加权)：
  对 log(价格) 的最近 L 根做线性回归 → 斜率 β / 残差波动 σ × R²
  = 「每单位噪音走多远」 × 「多像一条直线」

纯离线，用 .run/bt_cache 的历史样本，不碰实盘。
用法： /opt/anaconda3/bin/python3 scripts/xsection_trend.py
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

CACHE_DIR = ROOT / ".run" / "bt_cache"
RTH_OPEN_MIN = 9 * 60 + 30
LOOKBACK = 12          # 趋势分回看根数（12×5m = 60分钟）
HORIZONS = [1, 3, 6, 12]   # 未来收益周期（根）
MIN_SYMS = 5           # 一个时点至少多少标的才算一次横截面
IC_HORIZON = 6         # 分时段/分位报告用的主周期（30分钟）


# ── 数据加载 ──────────────────────────────────────────────
def load_all_cache() -> dict:
    out = {}
    if not CACHE_DIR.exists():
        return out
    for f in sorted(CACHE_DIR.glob("*.json")):
        try:
            out[f.stem] = json.loads(f.read_text())
        except Exception:
            continue
    return out


# ── 统计工具 ──────────────────────────────────────────────
def _mean(xs): return sum(xs) / len(xs) if xs else 0.0
def _std(xs):
    n = len(xs)
    if n < 2: return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
def _t(xs):
    s = _std(xs)
    return (_mean(xs) / (s / math.sqrt(len(xs)))) if s > 0 and len(xs) > 1 else 0.0


def _rank(vals):
    """平均秩（处理并列）。"""
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(vals):
        j = i
        while j + 1 < len(vals) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _pearson(xs, ys):
    n = len(xs)
    if n < 2: return 0.0
    mx, my = _mean(xs), _mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return (cov / (sx * sy)) if sx > 0 and sy > 0 else 0.0


def _spearman(xs, ys):
    return _pearson(_rank(xs), _rank(ys))


def trend_score(series: list[float], is_log: bool) -> float:
    """风险调整斜率 × R²。series 为价格(is_log=False)或残差累积(is_log=True 表已是对数量纲)。"""
    n = len(series)
    if n < 3:
        return 0.0
    y = series if is_log else [math.log(v) for v in series if v > 0]
    if len(y) < 3:
        return 0.0
    n = len(y)
    xs = list(range(n))
    mx, my = _mean(xs), _mean(y)
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 0:
        return 0.0
    slope = sum((xs[i] - mx) * (y[i] - my) for i in range(n)) / sxx
    intercept = my - slope * mx
    ss_res = sum((y[i] - (slope * xs[i] + intercept)) ** 2 for i in range(n))
    ss_tot = sum((v - my) ** 2 for v in y)
    r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    sd_res = math.sqrt(ss_res / (n - 2)) if n > 2 else 0.0
    radj = slope / sd_res if sd_res > 0 else 0.0
    return radj * max(r2, 0.0)


# ── 核心：逐日构建横截面，采集 (趋势分, 未来收益) ──────────────
def build(days: dict, symbols: list[str]):
    # 每条记录：{t, et_min, scores_raw:{sym}, scores_res:{sym}, fwd:{H:{sym}}}
    records = []
    for date in sorted(days):
        bysym = {}
        for sym, bars in days[date].items():
            if symbols and sym not in symbols:
                continue
            bars = sorted(bars, key=lambda b: int(b["time"]))
            closes = [float(b["close"]) for b in bars]
            times = [int(b["time"]) for b in bars]
            idx = {t: i for i, t in enumerate(times)}
            # EMA20（用户 spec，span=20，seed=首根收盘）
            k = 2.0 / (20 + 1)
            ema20, e = [], None
            for c in closes:
                e = c if e is None else (c - e) * k + e
                ema20.append(e)
            # 逐根 2 票离散分：ST 方向(+1/-1) + EMA20 位置(+1/-1)
            logic_bar = []
            for i, b in enumerate(bars):
                sd = int(b.get("st_dir", 0)) or (1 if closes[i] >= float(b.get("st_value", closes[i])) else -1)
                ep = 1 if closes[i] >= ema20[i] else -1
                logic_bar.append((1 if sd > 0 else -1) + ep)
            bysym[sym] = {"closes": closes, "times": times, "idx": idx,
                          "ema20": ema20, "logic_bar": logic_bar}
        if not bysym:
            continue

        # 每根 bar 的横截面等权平均收益 → 特质(残差)收益序列
        all_times = sorted({t for d in bysym.values() for t in d["times"]})
        # 逐 symbol 计算每根收益，并累积「原始 log 价」与「残差累积」
        for d in bysym.values():
            c = d["closes"]
            d["ret"] = [0.0] + [(c[i] / c[i - 1] - 1) for i in range(1, len(c))]
        # 市场每时点等权平均收益
        mkt = {}
        for t in all_times:
            rs = []
            for d in bysym.values():
                i = d["idx"].get(t)
                if i is not None and i >= 1:
                    rs.append(d["ret"][i])
            if rs:
                mkt[t] = _mean(rs)
        # 每 symbol 的残差累积序列（对齐自身 times）
        for d in bysym.values():
            resid = []
            acc = 0.0
            for i, t in enumerate(d["times"]):
                if i == 0:
                    resid.append(0.0); continue
                acc += d["ret"][i] - mkt.get(t, 0.0)
                resid.append(acc)
            d["resid"] = resid

        for t in all_times:
            et_min = (t % 86400) // 60
            scores_raw, scores_res, scores_logic = {}, {}, {}
            fwd = {H: {} for H in HORIZONS}
            for sym, d in bysym.items():
                i = d["idx"].get(t)
                if i is None or i < LOOKBACK - 1:
                    continue
                win_px = d["closes"][i - LOOKBACK + 1: i + 1]
                win_res = d["resid"][i - LOOKBACK + 1: i + 1]
                scores_raw[sym] = trend_score(win_px, is_log=False)
                scores_res[sym] = trend_score(win_res, is_log=True)
                # 用户逻辑分：12 根 2 票累加 / 24 → [-1,1]
                scores_logic[sym] = sum(d["logic_bar"][i - LOOKBACK + 1: i + 1]) / (2.0 * LOOKBACK)
                for H in HORIZONS:
                    j = i + H
                    if j < len(d["closes"]):
                        fwd[H][sym] = d["closes"][j] / d["closes"][i] - 1
            if len(scores_raw) >= MIN_SYMS:
                records.append({"t": t, "et_min": et_min, "raw": scores_raw,
                                "res": scores_res, "logic": scores_logic, "fwd": fwd})
    return records


def rank_ic_series(records, score_key, H, et_filter=None):
    ics = []
    for r in records:
        if et_filter and not et_filter(r["et_min"]):
            continue
        sc = r[score_key]; fw = r["fwd"][H]
        syms = [s for s in sc if s in fw]
        if len(syms) < MIN_SYMS:
            continue
        ics.append(_spearman([sc[s] for s in syms], [fw[s] for s in syms]))
    return ics


def topbottom_spread(records, score_key, H, q=0.2):
    """每时点 Top 分位均值 − Bottom 分位均值 的未来收益。"""
    spreads = []
    for r in records:
        sc = r[score_key]; fw = r["fwd"][H]
        syms = [s for s in sc if s in fw]
        if len(syms) < MIN_SYMS:
            continue
        syms.sort(key=lambda s: sc[s])
        k = max(1, int(len(syms) * q))
        bottom = _mean([fw[s] for s in syms[:k]])
        top = _mean([fw[s] for s in syms[-k:]])
        spreads.append(top - bottom)
    return spreads


def _fmt_ic(ics):
    if not ics:
        return "   —   "
    return f"{_mean(ics):+.4f} (t={_t(ics):+.2f}, +占{sum(1 for x in ics if x>0)/len(ics)*100:.0f}%)"


def report(records, days):
    print("\n" + "=" * 78)
    print("  横截面趋势可选性检验  (Rank-IC / 分位价差)")
    print("=" * 78)
    print(f"  交易日={len(days)}  横截面样本时点={len(records)}  回看={LOOKBACK}根  最少标的={MIN_SYMS}")
    if not records:
        print("  样本不足。"); return

    # 1) Rank-IC：逻辑分 / 原始 / 特质，各周期
    print("-" * 78)
    print("  Rank-IC（趋势分排名 vs 未来收益的横截面相关；>0=动量, <0=反转）")
    print(f"  {'周期':<8}{'逻辑分(ST+EMA20)':<32}{'原始回归分':<30}{'特质(残差)分':<30}")
    for H in HORIZONS:
        lg = rank_ic_series(records, "logic", H)
        raw = rank_ic_series(records, "raw", H)
        res = rank_ic_series(records, "res", H)
        print(f"  {str(H)+'根/'+str(H*5)+'分':<8}{_fmt_ic(lg):<32}{_fmt_ic(raw):<30}{_fmt_ic(res):<30}")

    # 2) Top−Bottom 分位价差（可交易的多空价差）@主周期
    print("-" * 78)
    print(f"  Top20% − Bottom20% 未来 {IC_HORIZON*5} 分钟收益价差（多最强/空最弱能否赚）：")
    for key, name in (("logic", "逻辑分"), ("raw", "原始"), ("res", "特质")):
        sp = topbottom_spread(records, key, IC_HORIZON)
        if sp:
            print(f"    {name}：均值 {_mean(sp)*1e4:+.1f} bps/次  t={_t(sp):+.2f}  "
                  f"胜率{sum(1 for x in sp if x>0)/len(sp)*100:.0f}%  样本{len(sp)}")

    # 3) 分时段 Rank-IC（逻辑分，主周期）
    print("-" * 78)
    print(f"  分时段 Rank-IC（逻辑分 @{IC_HORIZON*5}分）：")
    segs = {"开盘(9:30-10:30)": (570, 630), "午盘(10:30-14:00)": (630, 840),
            "尾盘(14:00-15:45)": (840, 945)}
    for name, (a, b) in segs.items():
        ics = rank_ic_series(records, "logic", IC_HORIZON, et_filter=lambda m, a=a, b=b: a <= m < b)
        print(f"    {name:<20}{_fmt_ic(ics)}")

    # 4) 大白话结论
    print("=" * 78)
    print("  结论（大白话）：")

    def verdict(key, label):
        ics = rank_ic_series(records, key, IC_HORIZON)
        m, t = _mean(ics), _t(ics)
        if abs(t) < 2:
            print(f"    ✗ {label}：Rank-IC={m:+.4f} t={t:+.2f}（|t|<2 不显著）→ 排名对未来无区分力。")
        elif m > 0:
            print(f"    ✓ {label}：Rank-IC={m:+.4f} t={t:+.2f} → 【动量】成立，做多分最高的标的。")
        else:
            print(f"    ⚠ {label}：Rank-IC={m:+.4f} t={t:+.2f} → 【反转】占优，做多分最高反而亏，应做多分最低/空最高。")
    verdict("logic", "逻辑分(ST+EMA20)")
    verdict("raw", "原始回归分")
    verdict("res", "特质趋势分")
    print("=" * 78 + "\n")


def main():
    global LOOKBACK
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", default="")
    ap.add_argument("--lookback", type=int, default=LOOKBACK)
    args = ap.parse_args()
    LOOKBACK = args.lookback
    syms = [s.strip() for s in args.symbols.split(",") if s.strip()]
    days = load_all_cache()
    if not days:
        print("无缓存数据。先 main.py --mode backtest 加载历史。")
        sys.exit(1)
    all_syms = syms or sorted({s for d in days.values() for s in d})
    print(f"横截面检验：{len(days)}日 {sorted(days)} | 标的 {len(all_syms)} 个")
    records = build(days, syms)
    report(records, days)


if __name__ == "__main__":
    main()
