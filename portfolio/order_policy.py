"""
订单类型决策 — 纯函数、无 broker 依赖。

延迟行情下 IBKR 会拒 bare MARKET（error 10349），需升级为 marketable LIMIT。
参考 ibkr-trader-core OrderPolicy，适配 Nautilus 路径。
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class DataState(str, Enum):
    REALTIME = "realtime"
    DELAYED = "delayed"


BRACKET_ENTRY_PREMIUM_PCT = 0.5
CLOSE_SLIPPAGE_PCT = 0.5


@dataclass(frozen=True)
class OrderDecision:
    use_limit: bool
    limit_price: Optional[float]
    reason: str


def is_delayed_market_data() -> bool:
    """单一配置源：main.py 行情类型与 OrderPolicy 均读此函数。"""
    mode = os.environ.get("MARKET_DATA_MODE", "").strip().lower()
    if mode in ("delayed", "delayed_frozen", "delayed-frozen"):
        return True
    if os.environ.get("MARKET_DATA_DELAYED", "").strip().lower() in ("1", "true", "yes"):
        return True
    return False


def data_state_from_env() -> DataState:
    if is_delayed_market_data():
        return DataState.DELAYED
    if os.environ.get("MARKET_DATA_REALTIME", "").strip().lower() in ("1", "true", "yes"):
        return DataState.REALTIME
    return DataState.REALTIME


def marketable_limit(price: float, side: str, slippage_pct: float) -> float:
    slip = slippage_pct / 100.0
    if side.upper() == "BUY":
        return round(price * (1 + slip), 2)
    return round(price * (1 - slip), 2)


def decide_entry_order(
    *,
    data_state: DataState,
    side: str,
    ref_price: float,
    slippage_pct: float = BRACKET_ENTRY_PREMIUM_PCT,
    force_limit: bool = False,
) -> OrderDecision:
    if ref_price <= 0:
        raise ValueError(f"ref_price must be > 0, got {ref_price}")
    if data_state == DataState.DELAYED or force_limit:
        lp = marketable_limit(ref_price, side, slippage_pct)
        reason = "delayed_data_marketable_limit" if data_state == DataState.DELAYED else "force_limit"
        return OrderDecision(use_limit=True, limit_price=lp, reason=reason)
    return OrderDecision(use_limit=False, limit_price=None, reason="realtime_market")


def decide_close_order(
    *,
    data_state: DataState,
    side: str,
    ref_price: float,
    slippage_pct: float = CLOSE_SLIPPAGE_PCT,
    force_limit: bool = False,
) -> OrderDecision:
    """平仓/反手与入场共用逻辑（延迟行情拒 bare MARKET）。"""
    return decide_entry_order(
        data_state=data_state,
        side=side,
        ref_price=ref_price,
        slippage_pct=slippage_pct,
        force_limit=force_limit,
    )
