#!/usr/bin/env python3
"""
精简信号回测：MTF SuperTrend 对齐入场（用户指定）。

信号定义：
  · 5m SuperTrend 定方向（大周期趋势过滤），参数 (10, 3.5)
  · 1m SuperTrend 发生翻转、且翻转后方向与 5m ST 同向 → 该 1m 收盘入场，参数 (10, 3.0)
  · 止损 = 入场时的 1m ST 值；之后随 1m ST 移动（trailing），价格触及即离场
  · 日内信号，收盘平仓

ST 用指定参数从 OHLC 重算（不消费 Redis 里 production 存的 st_dir，因参数不同）。
  · --source redis （默认）：读当前 Redis（仅当日，冒烟测试用）
  · --source cache          ：读 .run/bt_cache（5m）+ .run/bt_cache_1m（1m）多日
  · --snapshot              ：把当前 Redis 的 bars:1m:* 快照到 .run/bt_cache_1m/{date}.json

用法：
  /opt/anaconda3/bin/python3 scripts/test_st_mtf.py --source redis
  /opt/anaconda3/bin/python3 scripts/test_st_mtf.py --snapshot
  /opt/anaconda3/bin/python3 scripts/test_st_mtf.py --source cache
"""
from __future__ import annotations

import argparse
import bisect
import json
import math
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from nautilus_trader.indicators import AverageTrueRange  # noqa: E402
from nautilus_trader.indicators.averages import MovingAverageType  # noqa: E402


class STState:
    """与 strategy._STState 完全一致：鹦鹉螺 AverageTrueRange(WILDER) + 上一根 band 转向。"""

    def __init__(self, period: int = 10, mult: float = 3.5):
        self.period = period
        self.mult = mult
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close = 0.0
        self._prev_upper_b = 0.0
        self._prev_lower_b = 0.0
        self._prev_dir = -1
        self._initialized = False

    def update(self, o: float, h: float, lo: float, c: float) -> tuple[float, int]:
        self._atr.update_raw(h, lo, c)
        if not self._atr.initialized:
            self._prev_close = c
            return 0.0, 1
        atr = self._atr.value
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr
        flip_upper, flip_lower = self._prev_upper_b, self._prev_lower_b
        if not self._initialized:
            upper_b, lower_b = basic_upper, basic_lower
            self._initialized = True
        else:
            pu, pl, pc = self._prev_upper_b, self._prev_lower_b, self._prev_close
            upper_b = basic_upper if (basic_upper < pu or pc > pu) else pu
            lower_b = basic_lower if (basic_lower > pl or pc < pl) else pl
        if self._prev_dir == -1 and c > flip_upper:
            st_dir = 1
        elif self._prev_dir == 1 and c < flip_lower:
            st_dir = -1
        else:
            st_dir = self._prev_dir
        st_val = lower_b if st_dir == 1 else upper_b
        self._prev_close, self._prev_upper_b, self._prev_lower_b, self._prev_dir = c, upper_b, lower_b, st_dir
        return round(st_val, 4), st_dir

CACHE_5M = ROOT / ".run" / "bt_cache"
CACHE_1M = ROOT / ".run" / "bt_cache_1m"
RTH_OPEN_MIN = 9 * 60 + 30      # 09:30
RTH_ENTRY_END = 15 * 60 + 45    # 15:45 之后不再开新仓
RTH_CLOSE_MIN = 15 * 60 + 58    # 收盘平仓


# ── 工具 ─────────────────────────────────────────────
def _session_date_of(bars: list[dict]) -> str:
    for b in bars:
        t = int(b.get("time", 0))
        if (t % 86400) // 60 >= RTH_OPEN_MIN:
            return datetime.fromtimestamp(t, tz=timezone.utc).strftime("%Y-%m-%d")
    if bars:
        return datetime.fromtimestamp(int(bars[0]["time"]), tz=timezone.utc).strftime("%Y-%m-%d")
    return "unknown"


def _redis():
    import redis
    return redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)


def _load_redis_list(r, key: str) -> list[dict]:
    out = []
    for x in r.lrange(key, 0, -1):
        try:
            out.append(json.loads(x))
        except Exception:
            continue
    return out


def snapshot_1m() -> None:
    r = _redis()
    syms = sorted(k.split(":")[-1] for k in r.scan_iter("bars:1m:*"))
    data = {}
    for s in syms:
        bars = _load_redis_list(r, f"bars:1m:{s}")
        if bars:
            data[s] = bars
    if not data:
        print("Redis 无 bars:1m 数据。"); return
    date = _session_date_of(next(iter(data.values())))
    CACHE_1M.mkdir(parents=True, exist_ok=True)
    (CACHE_1M / f"{date}.json").write_text(json.dumps(data))
    print(f"已快照 1m: {date}  标的 {len(data)} 个 → {CACHE_1M / (date + '.json')}")


def load_source(source: str):
    """返回 {date: {'1m': {sym:bars}, '5m': {sym:bars}}}"""
    days: dict[str, dict[str, dict]] = {}
    if source == "redis":
        r = _redis()
        syms1 = sorted(k.split(":")[-1] for k in r.scan_iter("bars:1m:*"))
        d1 = {s: _load_redis_list(r, f"bars:1m:{s}") for s in syms1}
        d1 = {s: b for s, b in d1.items() if b}
        syms5 = sorted(k.split(":")[-1] for k in r.scan_iter("bars:5m:*"))
        d5 = {s: _load_redis_list(r, f"bars:5m:{s}") for s in syms5}
        d5 = {s: b for s, b in d5.items() if b}
        if not d1:
            return days
        date = _session_date_of(next(iter(d1.values())))
        days[date] = {"1m": d1, "5m": d5}
        return days
    # cache：交集日期
    c1 = {f.stem: json.loads(f.read_text()) for f in CACHE_1M.glob("*.json")} if CACHE_1M.exists() else {}
    c5 = {f.stem: json.loads(f.read_text()) for f in CACHE_5M.glob("*.json")} if CACHE_5M.exists() else {}
    for date in sorted(set(c1) & set(c5)):
        days[date] = {"1m": c1[date], "5m": c5[date]}
    return days


# ── 交易与回测 ────────────────────────────────────────
@dataclass
class Trade:
    sym: str
    date: str
    side: int            # +1 多 / -1 空
    entry_t: int
    entry: float
    stop0: float         # 入场止损
    exit_t: int = 0
    exit: float = 0.0
    reason: str = ""
    hold_min: int = 0
    mae_r: float = 0.0   # 最大不利 (R)
    mfe_r: float = 0.0   # 最大有利 (R)
    r: float = 0.0       # 净 R
    r_gross: float = 0.0


def _et_min(t: int) -> int:
    return (t % 86400) // 60


def compute_st(bars: list[dict], period: int, mult: float):
    """用指定参数从 OHLC 重算 ST，返回与 bars 等长的 [(st_val, st_dir)]。
    warmup 期返回 (0.0, 0) 表示未初始化。"""
    st = STState(period, mult)
    out = []
    for b in bars:
        val, d = st.update(float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]))
        out.append((val, d) if st._initialized and val > 0 else (0.0, 0))
    return out


def align_5m_dir(bars5: list[dict], period: int, mult: float):
    """返回 (times_sorted, dirs)：用重算的 5m 方向，供 1m 按时间回看。"""
    bars5 = sorted(bars5, key=lambda b: int(b["time"]))
    st5 = compute_st(bars5, period, mult)
    times = [int(b["time"]) for b in bars5]
    dirs = [d for _, d in st5]
    return times, dirs


def dir5_at(times5, dirs5, t: int) -> int:
    """最近一根已收盘 5m bar 的方向（桶 time+300 须 <= t，无前视）。"""
    i = bisect.bisect_right(times5, t - 300) - 1
    return dirs5[i] if i >= 0 else 0


def backtest_symbol(sym: str, date: str, bars1: list[dict], bars5: list[dict],
                    cost_ps: float, period: int, mult1: float, mult5: float) -> list[Trade]:
    bars1 = sorted((b for b in bars1 if all(k in b for k in ("open", "high", "low", "close"))),
                   key=lambda b: int(b["time"]))
    if len(bars1) < period + 2 or not bars5:
        return []
    times5, dirs5 = align_5m_dir(bars5, period, mult5)
    st1 = compute_st(bars1, period, mult1)
    trades: list[Trade] = []
    prev_dir = None
    pos: Trade | None = None
    stop = 0.0

    for k, b in enumerate(bars1):
        t = int(b["time"]); em = _et_min(t)
        c = float(b["close"]); hi = float(b["high"]); lo = float(b["low"])
        stv, d1 = st1[k]
        premkt = bool(b.get("premarket", False))

        # ── 持仓管理（trailing 1m ST 止损 + 收盘平仓）──
        if pos is not None:
            risk = abs(pos.entry - pos.stop0) or 1e-9
            # 记录 MAE/MFE
            adverse = (pos.entry - lo) if pos.side > 0 else (hi - pos.entry)
            favor = (hi - pos.entry) if pos.side > 0 else (pos.entry - lo)
            pos.mae_r = min(pos.mae_r, -max(adverse, 0) / risk)
            pos.mfe_r = max(pos.mfe_r, max(favor, 0) / risk)
            hit = (lo <= stop) if pos.side > 0 else (hi >= stop)
            eod = em >= RTH_CLOSE_MIN
            if hit or eod:
                ex = stop if hit else c
                pos.exit_t = t; pos.exit = round(ex, 4)
                pos.reason = "stop_1mST" if hit else "eod"
                pos.hold_min = max(1, (t - pos.entry_t) // 60)
                gross = (ex - pos.entry) * pos.side
                net = gross - cost_ps
                pos.r_gross = gross / risk
                pos.r = net / risk
                trades.append(pos)
                pos = None
            else:
                # trailing：多头止损上移到 1m ST(下轨)，空头下移到上轨
                if pos.side > 0 and d1 == 1 and stv > stop:
                    stop = stv
                elif pos.side < 0 and d1 == -1 and (stop == 0 or stv < stop):
                    stop = stv

        # ── 入场判定 ──
        if pos is None and prev_dir is not None and d1 != 0 and d1 != prev_dir:
            d5 = dir5_at(times5, dirs5, t)
            tradable = (not premkt) and (RTH_OPEN_MIN <= em < RTH_ENTRY_END)
            if tradable and d5 == d1 and stv > 0:
                # 入场：1m 翻转且与 5m 同向
                pos = Trade(sym=sym, date=date, side=d1, entry_t=t, entry=c, stop0=round(stv, 4))
                stop = stv
        if d1 != 0:
            prev_dir = d1

    return trades


# ── 报告 ─────────────────────────────────────────────
def _mean(xs): return sum(xs) / len(xs) if xs else 0.0
def _std(xs):
    n = len(xs)
    if n < 2: return 0.0
    m = _mean(xs); return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
def _t(xs):
    s = _std(xs); return (_mean(xs) / (s / math.sqrt(len(xs)))) if s > 0 and len(xs) > 1 else 0.0


def report(trades: list[Trade], days, cost_ps: float):
    print("\n" + "=" * 74)
    print("  MTF SuperTrend 对齐入场  回测报告")
    print("=" * 74)
    print(f"  交易日={len(days)}  成本={cost_ps}/股(往返)  总交易={len(trades)}")
    if not trades:
        print("  无交易。"); print("=" * 74); return
    rs = [x.r for x in trades]
    wins = [x for x in trades if x.r > 0]
    print("-" * 74)
    print(f"  净期望 {_mean(rs):+.3f}R/笔   t={_t(rs):+.2f}   总计 {sum(rs):+.1f}R")
    print(f"  胜率 {len(wins)/len(trades)*100:.1f}%   毛期望 {_mean([x.r_gross for x in trades]):+.3f}R")
    print(f"  平均持仓 {_mean([x.hold_min for x in trades]):.0f} 分   "
          f"平均MAE {_mean([x.mae_r for x in trades]):+.2f}R   平均MFE {_mean([x.mfe_r for x in trades]):+.2f}R")
    if wins:
        print(f"  盈利笔均 {_mean([x.r for x in wins]):+.2f}R   "
              f"亏损笔均 {_mean([x.r for x in trades if x.r<=0]):+.2f}R")
    # 出场原因
    print("-" * 74)
    for reason in ("stop_1mST", "eod"):
        sub = [x for x in trades if x.reason == reason]
        if sub:
            print(f"  出场[{reason:<9}] {len(sub):>3}笔  均 {_mean([x.r for x in sub]):+.3f}R  "
                  f"计 {sum(x.r for x in sub):+.1f}R")
    # 多空
    print("-" * 74)
    for side, name in ((1, "多"), (-1, "空")):
        sub = [x for x in trades if x.side == side]
        if sub:
            w = sum(1 for x in sub if x.r > 0)
            print(f"  {name} {len(sub):>3}笔  胜率{w/len(sub)*100:>4.0f}%  "
                  f"均 {_mean([x.r for x in sub]):+.3f}R  计 {sum(x.r for x in sub):+.1f}R")
    # 每标的
    print("-" * 74)
    bysym: dict[str, list[Trade]] = {}
    for x in trades:
        bysym.setdefault(x.sym, []).append(x)
    ranked = sorted(bysym.items(), key=lambda kv: sum(t.r for t in kv[1]))
    print("  每标的净R（最差5 / 最好5）：")
    for s, ts in ranked[:5]:
        print(f"    {s:<6} {len(ts):>2}笔 {sum(t.r for t in ts):+.1f}R")
    if len(ranked) > 5:
        for s, ts in ranked[-5:]:
            print(f"    {s:<6} {len(ts):>2}笔 {sum(t.r for t in ts):+.1f}R")
    print("=" * 74 + "\n")


def validate_st(period: int) -> None:
    """用 production 1m 参数(mult=3.5)重算，对比 Redis 存的 st_dir，验证 ST 实现。"""
    r = _redis()
    syms = sorted(k.split(":")[-1] for k in r.scan_iter("bars:1m:*"))
    tot = match = 0
    for s in syms:
        bars = sorted(_load_redis_list(r, f"bars:1m:{s}"), key=lambda b: int(b["time"]))
        bars = [b for b in bars if "st_dir" in b]
        st = compute_st(bars, period, 3.5)
        for b, (_, d) in zip(bars, st):
            if d == 0:
                continue
            tot += 1
            if d == int(b.get("st_dir", 0) or 0):
                match += 1
    if tot:
        print(f"ST 实现校验（1m mult=3.5 vs Redis 存值）：{match}/{tot} = {match/tot*100:.1f}% 匹配")
    else:
        print("无数据可校验。")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["redis", "cache"], default="redis")
    ap.add_argument("--snapshot", action="store_true")
    ap.add_argument("--validate", action="store_true",
                    help="用 production 参数(1m=3.5)重算 ST 对比 Redis 存值，验证 ST 实现忠实度")
    ap.add_argument("--symbols", default="")
    ap.add_argument("--cost-ps", type=float, default=0.02, help="往返成本 $/股")
    ap.add_argument("--st-period", type=int, default=10)
    ap.add_argument("--st1-mult", type=float, default=3.0, help="1m ST 乘数")
    ap.add_argument("--st5-mult", type=float, default=3.5, help="5m ST 乘数")
    ap.add_argument("--show-trades", type=int, default=0)
    args = ap.parse_args()

    if args.snapshot:
        snapshot_1m(); return
    if args.validate:
        validate_st(args.st_period); return

    only = {s.strip() for s in args.symbols.split(",") if s.strip()}
    days = load_source(args.source)
    if not days:
        print("无数据。redis 源需引擎在跑；cache 源需先 --snapshot 及历史 1m 缓存。")
        sys.exit(1)

    all_trades: list[Trade] = []
    for date in sorted(days):
        d1 = days[date]["1m"]; d5 = days[date]["5m"]
        for sym in sorted(d1):
            if only and sym not in only:
                continue
            if sym not in d5:
                continue
            all_trades += backtest_symbol(sym, date, d1[sym], d5[sym], args.cost_ps,
                                          args.st_period, args.st1_mult, args.st5_mult)

    print(f"数据源={args.source}  日期={sorted(days)}  标的={len(days[sorted(days)[0]]['1m'])}  "
          f"ST: 1m({args.st_period},{args.st1_mult}) 5m({args.st_period},{args.st5_mult})")
    report(all_trades, days, args.cost_ps)

    if args.show_trades:
        print("样例交易：")
        for x in all_trades[:args.show_trades]:
            et = datetime.fromtimestamp(x.entry_t, tz=timezone.utc).strftime("%m-%d %H:%M")
            print(f"  {et} {x.sym:<5} {'多' if x.side>0 else '空'} "
                  f"in {x.entry:.2f} stop {x.stop0:.2f} out {x.exit:.2f} "
                  f"[{x.reason}] {x.r:+.2f}R hold{x.hold_min}m")


if __name__ == "__main__":
    main()
