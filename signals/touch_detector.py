"""M1 回踩触线检测（水平线来自 M5 冻结 ST/DEMA + M1 Session VWAP）。"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Optional

from signals.pullback_scanner import (
    SIGNAL_DEMA20,
    SIGNAL_ST,
    SIGNAL_VWAP,
    _CONFIDENCE,
    _safe_float,
    _touch_band,
)


@dataclass(frozen=True)
class TouchEvent:
    symbol: str
    signal_type: str
    side: str
    trigger_level: float
    touch_time: int
    m1_bar_time: int
    m5_context_bar_time: Optional[int]
    session_date: str
    m1_high: float
    m1_low: float
    m1_close: float
    reclaim: bool
    rule_confidence: float
    rule_thesis: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _session_date_from_bar(bar: dict[str, Any]) -> str:
    from signals.indicators import bar_et_date
    return bar_et_date(bar) or ""


def m5_st_dir(active: Optional[dict[str, Any]]) -> int:
    """顺势方向仅取自 M5 收盘 ST（indicators:active），不用 M1 ST。"""
    if not active:
        return 0
    return int((active.get("supertrend") or {}).get("dir") or 0)


def detect_m1_touches(
    symbol: str,
    m1_bar: dict[str, Any],
    prev_m1: Optional[dict[str, Any]],
    active: Optional[dict[str, Any]],
) -> list[TouchEvent]:
    """在 M1 收盘检测触线；reclaim 字段表示同根是否已收回（执行仍走 conditional）。"""
    return detect_m1_touch_only(symbol, m1_bar, prev_m1, active)


def detect_m1_touch_only(
    symbol: str,
    m1_bar: dict[str, Any],
    prev_m1: Optional[dict[str, Any]],
    active: Optional[dict[str, Any]],
) -> list[TouchEvent]:
    """M1 触线即报（不要求同根 reclaim，供 Agent 出审批建议）。"""
    high = _safe_float(m1_bar.get("high"))
    low = _safe_float(m1_bar.get("low"))
    close = _safe_float(m1_bar.get("close"))
    bar_time = int(m1_bar.get("time") or 0)
    session_date = _session_date_from_bar(m1_bar)
    if high is None or low is None or close is None or not bar_time or not session_date:
        return []

    out: list[TouchEvent] = []
    m5_ctx = int((active or {}).get("m5_bar_time") or 0) or None
    st_dir = m5_st_dir(active)

    # ── VWAP（M1 session 累计值；顺势方向仍看 M5 ST）────────────────
    vwap = _safe_float(m1_bar.get("vwap"))
    if vwap is None and prev_m1:
        vwap = _safe_float(prev_m1.get("vwap"))
    if vwap is not None and st_dir in (1, -1):
        prev_close = _safe_float(prev_m1.get("close")) if prev_m1 else None
        prev_vwap = _safe_float(prev_m1.get("vwap")) if prev_m1 else vwap
        touched = _touch_band(low, high, vwap)
        if touched:
            if st_dir == 1 and (prev_close is None or prev_close > prev_vwap):
                reclaimed = close >= vwap
                out.append(_event(
                    symbol, SIGNAL_VWAP, "LONG", vwap, bar_time, m5_ctx, session_date,
                    high, low, close,
                    f"{symbol} M1 碰触 VWAP {vwap:.2f}"
                    + ("，已收回上方" if reclaimed else "，待收盘站回 VWAP 后执行"),
                    reclaimed=reclaimed,
                ))
            elif st_dir == -1 and (prev_close is None or prev_close < prev_vwap):
                reclaimed = close <= vwap
                out.append(_event(
                    symbol, SIGNAL_VWAP, "SHORT", vwap, bar_time, m5_ctx, session_date,
                    high, low, close,
                    f"{symbol} M1 碰触 VWAP {vwap:.2f}"
                    + ("，已跌回下方" if reclaimed else "，待收盘跌破 VWAP 后执行"),
                    reclaimed=reclaimed,
                ))

    if not active or st_dir not in (1, -1):
        return out

    # ── SuperTrend（M5 冻结水平线）──────────────────────────────────
    st_val = _safe_float(active.get("supertrend", {}).get("value"))
    if st_val is not None and _touch_band(low, high, st_val):
        if st_dir == 1:
            reclaimed = close >= st_val
            out.append(_event(
                symbol, SIGNAL_ST, "LONG", st_val, bar_time, m5_ctx, session_date,
                high, low, close,
                f"{symbol} M1 碰触 SuperTrend {st_val:.2f}"
                + ("，已站回上方" if reclaimed else "，待收盘站回 ST 后执行"),
                reclaimed=reclaimed,
            ))
        elif st_dir == -1:
            reclaimed = close <= st_val
            out.append(_event(
                symbol, SIGNAL_ST, "SHORT", st_val, bar_time, m5_ctx, session_date,
                high, low, close,
                f"{symbol} M1 碰触 SuperTrend {st_val:.2f}"
                + ("，已跌回下方" if reclaimed else "，待收盘跌破 ST 后执行"),
                reclaimed=reclaimed,
            ))

    # ── DEMA20（M5 冻结水平线）──────────────────────────────────────
    dema20 = _safe_float(active.get("dema20"))
    if dema20 is not None and _touch_band(low, high, dema20):
        if st_dir == 1:
            reclaimed = close >= dema20
            out.append(_event(
                symbol, SIGNAL_DEMA20, "LONG", dema20, bar_time, m5_ctx, session_date,
                high, low, close,
                f"{symbol} M1 碰触 DEMA20 {dema20:.2f}"
                + ("，已企稳" if reclaimed else "，待收盘站回 DEMA20 后执行"),
                reclaimed=reclaimed,
            ))
        elif st_dir == -1:
            reclaimed = close <= dema20
            out.append(_event(
                symbol, SIGNAL_DEMA20, "SHORT", dema20, bar_time, m5_ctx, session_date,
                high, low, close,
                f"{symbol} M1 碰触 DEMA20 {dema20:.2f}"
                + ("，已承压" if reclaimed else "，待收盘跌破 DEMA20 后执行"),
                reclaimed=reclaimed,
            ))

    return out


def _event(
    symbol: str,
    signal_type: str,
    side: str,
    level: float,
    bar_time: int,
    m5_ctx: Optional[int],
    session_date: str,
    high: float,
    low: float,
    close: float,
    thesis: str,
    *,
    reclaimed: bool = False,
) -> TouchEvent:
    return TouchEvent(
        symbol=symbol,
        signal_type=signal_type,
        side=side,
        trigger_level=round(level, 4),
        touch_time=bar_time,
        m1_bar_time=bar_time,
        m5_context_bar_time=m5_ctx,
        session_date=session_date,
        m1_high=high,
        m1_low=low,
        m1_close=close,
        reclaim=reclaimed,
        rule_confidence=_CONFIDENCE[signal_type],
        rule_thesis=thesis,
    )


def dedup_key(event: TouchEvent) -> str:
    if event.signal_type == SIGNAL_VWAP:
        return f"{event.signal_type}:{event.session_date}"
    from signals.st_super import SIGNAL_ST_SUPER
    if event.signal_type == SIGNAL_ST_SUPER:
        return f"{event.signal_type}:{event.touch_time}"
    return f"{event.signal_type}:{event.m5_context_bar_time or event.m1_bar_time}"
