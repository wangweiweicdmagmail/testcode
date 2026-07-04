"""从 Redis M1/M5 历史 K 线回放触线（引擎 flush 后补打当天信号）。"""
from __future__ import annotations

import json
from typing import Any, Optional

import redis as _redis

from signals.touch_detector import TouchEvent, dedup_key, detect_m1_touches

MARKERS_KEY = "signals:markers:{symbol}"


def _parse_bars(raw_list: list[str]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in raw_list:
        try:
            out.append(json.loads(raw))
        except (json.JSONDecodeError, TypeError):
            continue
    return sorted(out, key=lambda b: int(b.get("time") or 0))


def _m5_active_at(m5_bars: list[dict[str, Any]], m1_time: int) -> Optional[dict[str, Any]]:
    ctx: Optional[dict[str, Any]] = None
    for b in m5_bars:
        t = int(b.get("time") or 0)
        if t <= m1_time:
            ctx = b
        else:
            break
    if not ctx:
        return None
    st_val = ctx.get("st_value")
    st_dir = ctx.get("st_dir")
    if st_val is None or st_dir is None:
        return None
    return {
        "m5_bar_time": int(ctx["time"]),
        "supertrend": {"value": st_val, "dir": st_dir},
        "dema20": ctx.get("dema20"),
    }


def replay_touches_from_redis(
    r: _redis.Redis,
    symbol: str,
    *,
    rth_only: bool = True,
) -> tuple[list[TouchEvent], set[str]]:
    """回放单标的触线；返回 (事件列表, dedup_keys)。"""
    sym = symbol.upper()
    m1_bars = _parse_bars(r.lrange(f"bars:1m:{sym}", 0, -1))
    m5_bars = _parse_bars(r.lrange(f"bars:5m:{sym}", 0, -1))
    if rth_only:
        RTH_OPEN, RTH_CLOSE = 9 * 3600 + 30 * 60, 16 * 3600
        m1_bars = [
            b for b in m1_bars
            if RTH_OPEN <= (int(b["time"]) % 86400) < RTH_CLOSE
        ]

    events: list[TouchEvent] = []
    seen: set[str] = set()
    prev: Optional[dict[str, Any]] = None
    for m1 in m1_bars:
        active = _m5_active_at(m5_bars, int(m1["time"]))
        for touch in detect_m1_touches(sym, m1, prev, active):
            key = dedup_key(touch)
            if key in seen:
                continue
            seen.add(key)
            events.append(touch)
        prev = m1
    return events, seen


def write_markers_list(r: _redis.Redis, symbol: str, events: list[TouchEvent]) -> int:
    """覆盖写入 chart 标记列表（按 touch_time 排序）。"""
    sym = symbol.upper()
    key = MARKERS_KEY.format(symbol=sym)
    ordered = sorted(events, key=lambda e: e.touch_time)
    pipe = r.pipeline()
    pipe.delete(key)
    for ev in ordered:
        pipe.rpush(key, json.dumps(ev.to_dict(), ensure_ascii=False))
    pipe.execute()
    return len(ordered)
