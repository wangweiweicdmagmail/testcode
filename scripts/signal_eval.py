#!/usr/bin/env python3
"""
信号评估层 —— 量信号【本身】的预测力，与执行/风控/止盈止损彻底解耦。

为什么需要它（区别于 backtest_alpha.py）：
  backtest_alpha.py 量的是「信号 → 风控筛选 → 模拟成交 → 止损/止盈」后的最终 PnL，
  是验证一个【已知有效】策略的工具。但要【发现】alpha，必须先回答更原始的问题：

      这个信号特征本身，对未来收益到底有没有预测力？

  本脚本对每个信号触发点，记录其后 N 根 M5 的「按方向修正」收益，统计：
    - 各持仓周期的平均收益 / 命中率 / t 统计量（预测力 + 显著性）
    - 收益衰减曲线（edge 在第几根最大、何时消失 → 决定持仓周期）
    - 强度(|ST-DEMA|/ATR) 与未来收益的相关性 IC + 分桶单调性（强度能否用于择优）
    - 制度拆分：波动率档 / 时段 / 大盘(SPY)同向 → 信号在什么条件下才有效
    - 多空拆分、Walk-forward 前后半一致性（防过拟合 / 防运气）

  全程不带任何止损/止盈/仓位——纯特征 → 未来收益，便宜、快、能枪毙没料的想法。

数据来源：与 backtest_alpha.py 共用 .run/bt_cache/*.json（main.py --mode backtest 加载）。

用法：
  /opt/anaconda3/bin/python3 scripts/signal_eval.py
  /opt/anaconda3/bin/python3 scripts/signal_eval.py --symbols NVDA,AMD
  /opt/anaconda3/bin/python3 scripts/signal_eval.py --loose   # 关闭收盘确认/间距过滤，看裸信号
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import redis as _redis
from nautilus_trader.model.enums import OrderSide

from signals.base import BarContext, IntentAction, PositionContext
from signals.st_dema_m5 import StDemaM5Config, StDemaM5Engine

CACHE_DIR = ROOT / ".run" / "bt_cache"
RTH_OPEN_MIN = 9 * 60 + 30
HORIZONS = [1, 3, 5, 10, 20]
FLAT = PositionContext(open_units=0, is_long=None, all_breakeven=False, unit_sides=())


# ─────────────────────────────────────────────────────────────────────────
# 数据加载（与 backtest_alpha 共用缓存目录）
# ─────────────────────────────────────────────────────────────────────────
def _session_date_of(bars: list[dict]) -> str:
    for b in bars:
        t = int(b.get("time", 0))
        if (t % 86400) // 60 >= RTH_OPEN_MIN:
            return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    if bars:
        return datetime.fromtimestamp(int(bars[0]["time"]), tz=timezone.utc).strftime("%Y-%m-%d")
    return "unknown"


def load_from_redis(r, symbols: list[str]) -> dict[str, list[dict]]:
    data: dict[str, list[dict]] = {}
    for sym in symbols:
        try:
            raw = r.lrange(f"bars:5m:{sym}", 0, -1)
        except Exception:
            continue
        bars = [json.loads(x) for x in raw if x]
        if bars:
            data[sym] = bars
    return data


def snapshot_to_cache(data: dict[str, list[dict]]):
    if not data:
        return None
    date = _session_date_of(next(iter(data.values())))
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    try:
        (CACHE_DIR / f"{date}.json").write_text(json.dumps(data))
    except Exception:
        return None
    return date


def load_all_cache() -> dict[str, dict[str, list[dict]]]:
    out: dict[str, dict[str, list[dict]]] = {}
    if not CACHE_DIR.exists():
        return out
    for f in sorted(CACHE_DIR.glob("*.json")):
        try:
            out[f.stem] = json.loads(f.read_text())
        except Exception:
            continue
    return out


# ─────────────────────────────────────────────────────────────────────────
# 触发样本
# ─────────────────────────────────────────────────────────────────────────
@dataclass
class Trigger:
    sym: str
    date: str
    bar_time: int
    et_min: int
    side: int               # +1 多 / -1 空
    close: float
    atr: float
    spread_atr: float       # |ST-DEMA|/ATR  —— 信号强度
    atr_pct: float          # ATR/close      —— 波动率
    fwd_r: dict = field(default_factory=dict)    # horizon -> 按方向修正的 R 收益
    fwd_bps: dict = field(default_factory=dict)  # horizon -> 按方向修正的 bps 收益
    spy_align: int = 0      # +1 同向 / -1 逆向 / 0 未知


# ─────────────────────────────────────────────────────────────────────────
# 统计小工具
# ─────────────────────────────────────────────────────────────────────────
def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def _tstat(xs):
    n = len(xs)
    if n < 2:
        return 0.0
    s = _std(xs)
    return (_mean(xs) / (s / math.sqrt(n))) if s > 0 else 0.0


def _hit(xs):
    return (sum(1 for x in xs if x > 0) / len(xs)) if xs else 0.0


def _pearson(xs, ys):
    n = len(xs)
    if n < 2:
        return 0.0
    mx, my = _mean(xs), _mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    sx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    sy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return (cov / (sx * sy)) if sx > 0 and sy > 0 else 0.0


def _quantile_buckets(items, key, n=5):
    """按 key 升序分 n 桶，返回 [(label, sublist), ...]。"""
    s = sorted(items, key=key)
    out = []
    m = len(s)
    if m == 0:
        return out
    for i in range(n):
        a = i * m // n
        b = (i + 1) * m // n
        if a < b:
            out.append((f"Q{i+1}", s[a:b]))
    return out


# ─────────────────────────────────────────────────────────────────────────
# 核心：回放所有 bar，采集触发样本（始终空仓 → 评估每个信号实例）
# ─────────────────────────────────────────────────────────────────────────
def collect(days: dict, symbols: list[str], cfg: StDemaM5Config, atr_mult: float) -> list[Trigger]:
    # 大盘参照：SPY 每个时点的 st_dir
    spy_dir: dict[tuple, int] = {}
    for date, bysym in days.items():
        for b in bysym.get("SPY", []):
            spy_dir[(date, int(b["time"]))] = int(b.get("st_dir", 0) or 0)

    engines: dict[str, StDemaM5Engine] = {}
    triggers: list[Trigger] = []

    for date in sorted(days):
        for sym, bars in days[date].items():
            if symbols and sym not in symbols:
                continue
            bars = sorted(bars, key=lambda x: int(x["time"]))
            closes = [float(b["close"]) for b in bars]
            eng = engines.get(sym)
            if eng is None:
                eng = StDemaM5Engine(cfg, None, lambda *_: None)
                eng.register_symbol(sym)
                engines[sym] = eng

            for idx, b in enumerate(bars):
                bt = int(b["time"])
                et_min = (bt % 86400) // 60
                ctx = BarContext(symbol=sym, bar=b, bar_time=bt, et_min=et_min, mode="observe")
                out = eng.on_bar(ctx, FLAT)
                for intent in out.intents:
                    if intent.action != IntentAction.ENTER:
                        continue
                    side = 1 if intent.side == OrderSide.BUY else -1
                    atr = float(intent.atr_ref) or 1e-9
                    dema = eng._dema[sym]._dema.value
                    st = float(b.get("st_value", 0.0) or 0.0)
                    c0 = closes[idx]
                    risk = atr * atr_mult
                    trg = Trigger(
                        sym=sym, date=date, bar_time=bt, et_min=et_min, side=side,
                        close=c0, atr=atr, spread_atr=abs(st - dema) / atr,
                        atr_pct=atr / c0 if c0 else 0.0,
                        spy_align=(1 if spy_dir.get((date, bt), 0) == side else
                                   (-1 if spy_dir.get((date, bt), 0) == -side else 0)),
                    )
                    for h in HORIZONS:
                        j = idx + h
                        if j < len(closes):   # 只在同日内度量，不跨夜
                            trg.fwd_r[h] = side * (closes[j] - c0) / risk
                            trg.fwd_bps[h] = side * (closes[j] - c0) / c0 * 1e4
                    triggers.append(trg)
    return triggers


# ─────────────────────────────────────────────────────────────────────────
# 报告
# ─────────────────────────────────────────────────────────────────────────
def _fwd_list(trgs, h, field="fwd_r"):
    return [getattr(t, field)[h] for t in trgs if h in getattr(t, field)]


def report(trgs: list[Trigger], days: dict) -> None:
    n = len(trgs)
    print("\n" + "=" * 74)
    print("  信号评估报告  (st_dema_m5 裸信号预测力，无止损/止盈/仓位)")
    print("=" * 74)
    print(f"  样本触发数      : {n}   交易日数: {len(days)}")
    if n == 0:
        print("  无触发样本。"); return

    # 1) 收益衰减曲线
    print("-" * 74)
    print("  未来收益（按方向修正；R=每股风险倍数，bps=万分之）")
    print(f"  {'持仓周期':<10}{'样本':>6}{'平均R':>9}{'命中率':>9}{'t统计':>9}{'平均bps':>10}")
    best_h, best_abs = HORIZONS[0], -1.0
    for h in HORIZONS:
        rs = _fwd_list(trgs, h)
        if not rs:
            continue
        bps = _fwd_list(trgs, h, "fwd_bps")
        m, hit, t = _mean(rs), _hit(rs), _tstat(rs)
        print(f"  {str(h)+'根('+str(h*5)+'分)':<10}{len(rs):>6}{m:>9.3f}"
              f"{hit*100:>8.1f}%{t:>9.2f}{_mean(bps):>10.1f}")
        if len(rs) >= 20 and abs(m) > best_abs:
            best_abs, best_h = abs(m), h
    H = best_h
    print(f"\n  → 最强预测力在 {H} 根（{H*5}分钟）持仓周期，以下分析以此为准")

    rs_all = _fwd_list(trgs, H)
    print("-" * 74)
    # 2) 强度 IC + 分桶单调性
    pairs = [(t.spread_atr, t.fwd_r[H]) for t in trgs if H in t.fwd_r]
    ic = _pearson([p[0] for p in pairs], [p[1] for p in pairs])
    print(f"  强度IC (|ST-DEMA|/ATR vs 未来R@{H}) : {ic:+.3f}")
    print("  强度分桶（弱→强，看平均R是否单调递增 = 强度是否可用于择优）：")
    bks = _quantile_buckets([t for t in trgs if H in t.fwd_r], key=lambda t: t.spread_atr, n=5)
    for label, sub in bks:
        rs = [t.fwd_r[H] for t in sub]
        sp = _mean([t.spread_atr for t in sub])
        print(f"    {label} 强度均={sp:>5.2f}  {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%")

    # 3) 制度拆分
    print("-" * 74)
    print(f"  制度拆分（均以 {H} 根持仓的平均R / 命中率）：")
    # 波动率档
    print("   · 波动率(ATR/价) 三档：")
    vbk = _quantile_buckets([t for t in trgs if H in t.fwd_r], key=lambda t: t.atr_pct, n=3)
    vlabels = ["低波动", "中波动", "高波动"]
    for (lab, sub), name in zip(vbk, vlabels):
        rs = [t.fwd_r[H] for t in sub]
        print(f"       {name:<6} {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%")
    # 时段
    print("   · 时段：")
    seg = {"开盘(9:30-10:30)": (570, 630), "午盘(10:30-14:00)": (630, 840),
           "尾盘(14:00-15:45)": (840, 945)}
    for name, (a, b) in seg.items():
        sub = [t for t in trgs if H in t.fwd_r and a <= t.et_min < b]
        rs = [t.fwd_r[H] for t in sub]
        if rs:
            print(f"       {name:<18} {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%")
    # 大盘同向
    has_spy = any(t.spy_align != 0 for t in trgs)
    if has_spy:
        print("   · 大盘(SPY)方向：")
        for name, flag in (("同向", 1), ("逆向", -1)):
            sub = [t for t in trgs if H in t.fwd_r and t.spy_align == flag]
            rs = [t.fwd_r[H] for t in sub]
            if rs:
                print(f"       {name:<6} {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%")

    # 4) 多空
    print("-" * 74)
    print("  多空拆分：")
    for name, sd in (("LONG", 1), ("SHORT", -1)):
        sub = [t for t in trgs if H in t.fwd_r and t.side == sd]
        rs = [t.fwd_r[H] for t in sub]
        if rs:
            print(f"    {name:<6} {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%  t={_tstat(rs):>5.2f}")

    # 5) Walk-forward 前后半
    print("-" * 74)
    dates = sorted(days)
    half = len(dates) // 2
    d1, d2 = set(dates[:half]), set(dates[half:])
    print(f"  Walk-forward 一致性（前半 {len(d1)}天 vs 后半 {len(d2)}天，@{H}根）：")
    for name, ds in (("前半", d1), ("后半", d2)):
        rs = [t.fwd_r[H] for t in trgs if H in t.fwd_r and t.date in ds]
        if rs:
            print(f"    {name}  {len(rs):>4}笔  平均R={_mean(rs):>7.3f}  命中={_hit(rs)*100:>5.1f}%  t={_tstat(rs):>5.2f}")

    # 6) 大白话结论
    print("=" * 74)
    m_all, t_all, hit_all = _mean(rs_all), _tstat(rs_all), _hit(rs_all)
    print("  结论（大白话）：")
    if abs(t_all) < 2:
        print(f"    ✗ 裸信号【没有显著方向性预测力】：{H}根后平均R={m_all:+.3f}，"
              f"t={t_all:+.2f}（|t|<2 = 统计上跟掷硬币无异），属噪音。")
    elif m_all > 0:
        print(f"    ✓ 裸信号有正向预测力：{H}根后平均R={m_all:+.3f}，t={t_all:+.2f}，命中{hit_all*100:.0f}%。")
    else:
        print(f"    ✗ 裸信号方向【系统性做反了】：{H}根后平均R={m_all:+.3f}，t={t_all:+.2f}（反向才赚）。")
    if ic > 0.05:
        print(f"    ✓ 强度IC={ic:+.3f} > 0.05：信号越强未来越好，【可用强度排序择优】(解决组合位先到先得问题)。")
    elif ic < -0.05:
        print(f"    ⚠ 强度IC={ic:+.3f} < 0：越强反而越差，过滤逻辑可能反了。")
    else:
        print(f"    ✗ 强度IC={ic:+.3f}≈0：强度对收益无区分力，靠它择优无意义。")
    print("    → 若某个制度/时段/方向的平均R明显为正且 t 显著，可据此加过滤；否则该信号需重做。")
    print("=" * 74 + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="信号预测力评估（裸信号，无执行/风控）")
    ap.add_argument("--symbols", default="")
    ap.add_argument("--dema-period", type=int, default=21)
    ap.add_argument("--atr-period", type=int, default=14)
    ap.add_argument("--atr-mult", type=float, default=1.5)
    ap.add_argument("--min-spread-atr", type=float, default=0.30)
    ap.add_argument("--no-close-confirm", action="store_true")
    ap.add_argument("--loose", action="store_true", help="关闭收盘确认+间距过滤，看裸信号上限")
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "localhost"))
    ap.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    args = ap.parse_args()
    syms = [s.strip() for s in args.symbols.split(",") if s.strip()]

    r = _redis.Redis(host=args.redis_host, port=args.redis_port, decode_responses=True)
    all_syms = syms or sorted({k.split(":")[-1] for k in r.scan_iter("bars:5m:*")})
    today = load_from_redis(r, all_syms)

    if args.no_cache:
        days = {_session_date_of(next(iter(today.values()))): today} if today else {}
    else:
        if today:
            snapshot_to_cache(today)
        days = load_all_cache()
    if not days:
        print("无数据。先 main.py --mode backtest 加载历史，再跑本脚本。")
        sys.exit(1)

    cfg = StDemaM5Config(
        dema_period=args.dema_period, atr_period=args.atr_period, atr_mult=args.atr_mult,
        min_st_dema_spread_atr=(0.0 if args.loose else args.min_spread_atr),
        require_close_confirm=not (args.no_close_confirm or args.loose),
    )
    mode = "裸信号(loose: 无收盘确认/无间距过滤)" if args.loose else "默认过滤"
    print(f"信号评估：{len(days)} 个交易日 {sorted(days)} | 标的={all_syms} | 模式={mode}")
    trgs = collect(days, syms, cfg, args.atr_mult)
    report(trgs, days)


if __name__ == "__main__":
    main()
