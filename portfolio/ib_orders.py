"""Nautilus 下单辅助 — OrderPolicy 与 order_factory 桥接。"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable, Optional

from nautilus_trader.model.enums import TimeInForce

from portfolio.order_policy import OrderDecision, data_state_from_env, decide_close_order


def build_marketable_order(
    order_factory,
    *,
    instrument,
    instrument_id,
    side,
    qty: int,
    ref_price: float,
    tags: Optional[list[str]],
    log_fn: Optional[Callable[[str], None]] = None,
) -> tuple[object, OrderDecision]:
    """按行情类型创建 MARKET 或 marketable LIMIT 单（入场/平仓通用）。"""
    decision = decide_close_order(
        data_state=data_state_from_env(),
        side=side.name if hasattr(side, "name") else str(side),
        ref_price=ref_price,
    )
    q = instrument.make_qty(Decimal(str(qty)))
    if decision.use_limit:
        if log_fn:
            log_fn(f"{decision.reason} LMT @ {decision.limit_price} (ref={ref_price})")
        order = order_factory.limit(
            instrument_id=instrument_id,
            order_side=side,
            quantity=q,
            price=instrument.make_price(Decimal(str(decision.limit_price))),
            time_in_force=TimeInForce.DAY,
            tags=tags,
        )
    else:
        order = order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=q,
            time_in_force=TimeInForce.DAY,
            tags=tags,
        )
    return order, decision
