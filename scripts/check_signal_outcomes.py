#!/usr/bin/env python3
"""
回填 signal_logs 的 outcome / SPY benchmark。

范围见 docs/SCOPE_FIXED.md §1.2、§3。
- 仅 touch（touch_time 与 bars:1m 同坐标系）；auto 信号 skip
- Redis 无目标 bar → skip（不拉 IBKR historical）
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import redis

from measurement.signal_store import pending_outcomes, stats_summary, update_outcome

BENCHMARK_SYMBOL = "SPY"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


def _side_sign(side: str | None) -> float:
    if side and side.upper() in ("SHORT", "SELL"):
        return -1.0
    return 1.0


def _close_after_ts(r: redis.Redis, symbol: str, target_ts: int) -> float | None:
    key = f"bars:1m:{symbol}"
    raw_list = r.lrange(key, 0, -1)
    if not raw_list:
        return None
    best = None
    for raw in raw_list:
        try:
            bar = json.loads(raw)
            t = int(bar.get("time") or 0)
            if t >= target_ts:
                c = float(bar.get("close"))
                if best is None or t < best[0]:
                    best = (t, c)
        except (json.JSONDecodeError, TypeError, ValueError):
            continue
    return best[1] if best else None


def _pct(from_px: float, to_px: float, side: str | None) -> float:
    if from_px <= 0:
        return 0.0
    raw = (to_px - from_px) / from_px * 100.0
    return round(raw * _side_sign(side), 4)


def main() -> int:
    parser = argparse.ArgumentParser(description="回填 signal outcome + SPY benchmark")
    parser.add_argument("--min-age-days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=500)
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pending = pending_outcomes(min_age_days=args.min_age_days, limit=args.limit)
    updated = 0
    skipped = 0

    for row in pending:
        sig_px = float(row["signal_price"])
        # touch 用 bar 的 ET fake-UTC 时间轴；auto 信号无 touch_time 时跳过（坐标系不一致）
        touch_ts = row.get("touch_time")
        if row.get("source") == "auto" and not touch_ts:
            skipped += 1
            continue
        base_ts = int(touch_ts or row["created_at"])
        target_ts = base_ts + args.min_age_days * 86400
        sym = row["symbol"]
        side = row.get("side")

        close = _close_after_ts(r, sym, target_ts)
        if close is None:
            skipped += 1
            continue

        outcome = _pct(sig_px, close, side)
        spy_base = _close_after_ts(r, BENCHMARK_SYMBOL, base_ts)
        spy_close = _close_after_ts(r, BENCHMARK_SYMBOL, target_ts)
        bench_pct = None
        if spy_base and spy_close and spy_base > 0:
            bench_pct = round((spy_close - spy_base) / spy_base * 100.0, 4)

        update_outcome(
            row["id"],
            outcome_7d_pct=outcome if args.min_age_days >= 7 else None,
            outcome_1d_pct=outcome if args.min_age_days < 7 else None,
            benchmark_7d_pct=bench_pct,
        )
        updated += 1

    summary = stats_summary()
    print(
        f"pending={len(pending)} updated={updated} skipped={skipped} "
        f"db={summary['db_path']} labeled_7d={summary['labeled_7d']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
