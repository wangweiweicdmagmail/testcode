"""回踩结构定价：止损（极值±1分）、半仓止盈（前高/前低）、盈亏比。"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


STOP_OFFSET = 0.01
DEFAULT_LOOKBACK = 15


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class PullbackPricing:
    pullback_extreme: float
    prior_swing: float
    stop_price: float
    tp_half_price: float
    entry_price_est: float
    risk_est: float
    reward_half_est: float
    rr_half_est: Optional[float]


def compute_pullback_pricing(
    *,
    side: str,
    m1_bars: list[dict[str, Any]],
    touch_bar_time: int,
    entry_est: Optional[float] = None,
    lookback: int = DEFAULT_LOOKBACK,
) -> Optional[PullbackPricing]:
    """
    从 M1 序列估算：
      - 做多：pullback low = lookback 内最低 low；前高 = touch 之前 swing high
      - 做空：对称
    """
    if not m1_bars:
        return None
    idx = next((i for i, b in enumerate(m1_bars) if int(b.get("time") or 0) == touch_bar_time), -1)
    if idx < 0:
        idx = len(m1_bars) - 1
    window = m1_bars[max(0, idx - lookback + 1): idx + 1]
    if not window:
        return None

    lows = [_safe_float(b.get("low")) for b in window]
    highs = [_safe_float(b.get("high")) for b in window]
    if any(v is None for v in lows + highs):
        return None

    touch_bar = window[-1]
    entry = entry_est if entry_est is not None else _safe_float(touch_bar.get("close"))
    if entry is None:
        return None

    is_long = side.upper() == "LONG"
    if is_long:
        extreme = min(lows)  # type: ignore[type-var]
        prior = max(highs[:-1]) if len(highs) > 1 else max(highs)  # type: ignore[type-var]
        stop = round(extreme - STOP_OFFSET, 2)
        tp_half = round(prior, 2)
        risk = round(entry - stop, 4)
        reward = round(tp_half - entry, 4)
    else:
        extreme = max(highs)  # type: ignore[type-var]
        prior = min(lows[:-1]) if len(lows) > 1 else min(lows)  # type: ignore[type-var]
        stop = round(extreme + STOP_OFFSET, 2)
        tp_half = round(prior, 2)
        risk = round(stop - entry, 4)
        reward = round(entry - tp_half, 4)

    if risk <= 0:
        return None
    rr = round(reward / risk, 2) if reward > 0 else None

    return PullbackPricing(
        pullback_extreme=round(extreme, 2),
        prior_swing=round(prior, 2),
        stop_price=stop,
        tp_half_price=tp_half,
        entry_price_est=round(entry, 2),
        risk_est=risk,
        reward_half_est=max(reward, 0),
        rr_half_est=rr,
    )


def reclaim_rule_for_side(side: str) -> str:
    return "close >= trigger_level" if side.upper() == "LONG" else "close <= trigger_level"


def reclaim_label(side: str) -> str:
    if side.upper() == "LONG":
        return "收盘价站上触发线（如 VWAP）后执行"
    return "收盘价跌破触发线后执行"
