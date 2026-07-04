"""三种 M5 回踩信号：VWAP / SuperTrend / DEMA20。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from signals.indicators import bar_et_date, dema_series, filter_session_bars, session_vwap


@dataclass(frozen=True)
class PullbackCandidate:
    symbol: str
    side: str
    signal_type: str
    trigger_level: float
    bar: dict[str, Any]
    rule_confidence: float
    rule_thesis: str


SIGNAL_VWAP = "pullback_vwap"
SIGNAL_ST = "pullback_supertrend"
SIGNAL_DEMA20 = "pullback_dema20"

_CONFIDENCE = {
    SIGNAL_VWAP: 0.72,
    SIGNAL_ST: 0.68,
    SIGNAL_DEMA20: 0.66,
}


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _touch_band(low: float, high: float, level: float) -> bool:
    return low <= level <= high


def _scan_vwap(symbol: str, session_bars: list[dict[str, Any]]) -> list[PullbackCandidate]:
    if len(session_bars) < 2:
        return []
    out: list[PullbackCandidate] = []
    cur = session_bars[-1]
    prev = session_bars[-2]

    high = _safe_float(cur.get("high"))
    low = _safe_float(cur.get("low"))
    close = _safe_float(cur.get("close"))
    prev_close = _safe_float(prev.get("close"))
    st_dir = int(cur.get("st_dir") or 0)
    if high is None or low is None or close is None or prev_close is None:
        return out

    vwap_cur = session_vwap(session_bars)
    vwap_prev = session_vwap(session_bars, through=len(session_bars) - 2)
    if vwap_cur is None or vwap_prev is None:
        return out

    # 多头：前一根在 VWAP 上方 → 本根回踩触及 → 收盘站回
    if st_dir == 1 and prev_close > vwap_prev and _touch_band(low, high, vwap_cur) and close >= vwap_cur:
        out.append(PullbackCandidate(
            symbol=symbol,
            signal_type=SIGNAL_VWAP,
            side="LONG",
            bar=cur,
            trigger_level=vwap_cur,
            rule_confidence=_CONFIDENCE[SIGNAL_VWAP],
            rule_thesis=(
                f"{symbol} M5 VWAP 回踩 reclaim：前棒站上 VWAP，本棒触及 "
                f"{vwap_cur:.2f} 后收盘收回上方。"
            ),
        ))
    # 空头：对称
    if st_dir == -1 and prev_close < vwap_prev and _touch_band(low, high, vwap_cur) and close <= vwap_cur:
        out.append(PullbackCandidate(
            symbol=symbol,
            signal_type=SIGNAL_VWAP,
            side="SHORT",
            bar=cur,
            trigger_level=vwap_cur,
            rule_confidence=_CONFIDENCE[SIGNAL_VWAP],
            rule_thesis=(
                f"{symbol} M5 VWAP 回踩 reclaim：前棒位于 VWAP 下，本棒触及 "
                f"{vwap_cur:.2f} 后收盘跌回下方。"
            ),
        ))
    return out


def _scan_supertrend(symbol: str, bar: dict[str, Any]) -> list[PullbackCandidate]:
    out: list[PullbackCandidate] = []
    high = _safe_float(bar.get("high"))
    low = _safe_float(bar.get("low"))
    close = _safe_float(bar.get("close"))
    st_value = _safe_float(bar.get("st_value"))
    st_dir = int(bar.get("st_dir") or 0)
    if high is None or low is None or close is None or st_value is None or st_dir not in (1, -1):
        return out

    if _touch_band(low, high, st_value):
        if st_dir == 1 and close >= st_value:
            out.append(PullbackCandidate(
                symbol=symbol,
                signal_type=SIGNAL_ST,
                side="LONG",
                bar=bar,
                trigger_level=st_value,
                rule_confidence=_CONFIDENCE[SIGNAL_ST],
                rule_thesis=f"{symbol} M5 回踩 SuperTrend {st_value:.2f} 后收盘站回上方。",
            ))
        elif st_dir == -1 and close <= st_value:
            out.append(PullbackCandidate(
                symbol=symbol,
                signal_type=SIGNAL_ST,
                side="SHORT",
                bar=bar,
                trigger_level=st_value,
                rule_confidence=_CONFIDENCE[SIGNAL_ST],
                rule_thesis=f"{symbol} M5 回踩 SuperTrend {st_value:.2f} 后收盘跌回下方。",
            ))
    return out


def _scan_dema20(symbol: str, session_bars: list[dict[str, Any]]) -> list[PullbackCandidate]:
    if len(session_bars) < 22:
        return []
    closes = []
    for b in session_bars:
        c = _safe_float(b.get("close"))
        if c is None:
            return []
        closes.append(c)
    dema = dema_series(closes, period=20)
    cur = session_bars[-1]
    dema20 = dema[-1]
    if dema20 is None:
        return []

    high = _safe_float(cur.get("high"))
    low = _safe_float(cur.get("low"))
    close = _safe_float(cur.get("close"))
    st_dir = int(cur.get("st_dir") or 0)
    if high is None or low is None or close is None or st_dir not in (1, -1):
        return []

    out: list[PullbackCandidate] = []
    if _touch_band(low, high, dema20):
        if st_dir == 1 and close >= dema20:
            out.append(PullbackCandidate(
                symbol=symbol,
                signal_type=SIGNAL_DEMA20,
                side="LONG",
                bar=cur,
                trigger_level=dema20,
                rule_confidence=_CONFIDENCE[SIGNAL_DEMA20],
                rule_thesis=f"{symbol} M5 回踩 DEMA20 {dema20:.2f} 后收盘企稳（顺势多）。",
            ))
        elif st_dir == -1 and close <= dema20:
            out.append(PullbackCandidate(
                symbol=symbol,
                signal_type=SIGNAL_DEMA20,
                side="SHORT",
                bar=cur,
                trigger_level=dema20,
                rule_confidence=_CONFIDENCE[SIGNAL_DEMA20],
                rule_thesis=f"{symbol} M5 回踩 DEMA20 {dema20:.2f} 后收盘承压（顺势空）。",
            ))
    return out


def scan_pullback_signals(symbol: str, bars: list[dict[str, Any]]) -> list[PullbackCandidate]:
    """
    扫描单标的 M5 回踩信号（最新一根 bar 为 bars[-1]）。

    返回 0～3 条候选（VWAP / SuperTrend / DEMA20 各自独立）。
    """
    if not bars:
        return []
    cur = bars[-1]
    session_date = bar_et_date(cur)
    if not session_date:
        return []
    session_bars = filter_session_bars(bars, session_date)
    if not session_bars:
        return []

    out: list[PullbackCandidate] = []
    out.extend(_scan_vwap(symbol, session_bars))
    out.extend(_scan_supertrend(symbol, cur))
    out.extend(_scan_dema20(symbol, session_bars))
    return out
