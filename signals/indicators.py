"""M5 日内指标：Session VWAP、DEMA。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from zoneinfo import ZoneInfo

_ET = ZoneInfo("America/New_York")


def bar_et_date(bar: dict[str, Any]) -> Optional[str]:
    """Session 日历日（与 strategy 写入 Redis 的 ET fake-UTC 时间戳一致）。"""
    ts = bar.get("time")
    if ts is None:
        return None
    try:
        from datetime import timezone
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except (TypeError, ValueError, OSError):
        return None


def filter_session_bars(bars: list[dict[str, Any]], session_date: str) -> list[dict[str, Any]]:
    return [b for b in bars if bar_et_date(b) == session_date]


def typical_price(bar: dict[str, Any]) -> Optional[float]:
    try:
        h = float(bar["high"])
        lo = float(bar["low"])
        c = float(bar["close"])
        return (h + lo + c) / 3.0
    except (KeyError, TypeError, ValueError):
        return None


def session_vwap(bars: list[dict[str, Any]], *, through: int = -1) -> Optional[float]:
    """累计 Session VWAP，through 为包含的最后一根 bar 下标。"""
    if not bars:
        return None
    end = len(bars) if through < 0 else min(through + 1, len(bars))
    if end <= 0:
        return None
    pv = 0.0
    vol = 0.0
    for bar in bars[:end]:
        tp = typical_price(bar)
        if tp is None:
            continue
        try:
            v = float(bar.get("volume") or 0)
        except (TypeError, ValueError):
            v = 0.0
        if v <= 0:
            continue
        pv += tp * v
        vol += v
    if vol <= 0:
        return None
    return round(pv / vol, 4)


def dema_series(closes: list[float], period: int = 20) -> list[Optional[float]]:
    """返回与 closes 等长的 DEMA 序列（预热前为 None）。"""
    if period < 2 or not closes:
        return [None] * len(closes)

    def ema(values: list[float], p: int) -> list[Optional[float]]:
        out: list[Optional[float]] = [None] * len(values)
        if len(values) < p:
            return out
        k = 2.0 / (p + 1)
        seed = sum(values[:p]) / p
        out[p - 1] = seed
        prev = seed
        for i in range(p, len(values)):
            prev = values[i] * k + prev * (1 - k)
            out[i] = prev
        return out

    e1 = ema(closes, period)
    e1_vals = [x if x is not None else 0.0 for x in e1]
    e2 = ema(e1_vals, period)
    out: list[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if e1[i] is not None and e2[i] is not None:
            out[i] = round(2 * e1[i] - e2[i], 4)
    return out
