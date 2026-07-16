"""Nautilus 下单辅助 — OrderPolicy 与 order_factory 桥接。"""
from __future__ import annotations

from decimal import Decimal
from typing import Callable, Optional

from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId

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
    client_order_id: Optional[ClientOrderId] = None,
) -> tuple[object, OrderDecision]:
    """按行情类型创建 MARKET 或 marketable LIMIT 单（入场/平仓通用）。

    client_order_id 显式传入时用它（编码仓位分组键前缀，= IBKR orderRef）；
    为 None 则由 factory 自动生成（旧行为）。
    """
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
            client_order_id=client_order_id,
        )
    else:
        order = order_factory.market(
            instrument_id=instrument_id,
            order_side=side,
            quantity=q,
            time_in_force=TimeInForce.DAY,
            tags=tags,
            client_order_id=client_order_id,
        )
    return order, decision


def build_resting_limit(
    order_factory,
    *,
    instrument,
    instrument_id,
    side,
    qty: int,
    limit_price: float,
    tags: Optional[list[str]],
    log_fn: Optional[Callable[[str], None]] = None,
    client_order_id: Optional[ClientOrderId] = None,
) -> object:
    """按指定价挂 GTC LIMIT（手动/EMA/SuperTrend 限价进场用，resting until filled）。

    与 build_marketable_order 的区别：后者按行情类型决定 MARKET 或 marketable DAY-LMT；
    本函数始终以用户/方法给定的精确价位挂 GTC 限价，等触及才成交。
    """
    q = instrument.make_qty(Decimal(str(qty)))
    p = instrument.make_price(Decimal(str(limit_price)))
    if log_fn:
        log_fn(f"resting_limit GTC @ {limit_price}")
    return order_factory.limit(
        instrument_id=instrument_id,
        order_side=side,
        quantity=q,
        price=p,
        time_in_force=TimeInForce.GTC,
        tags=tags,
        client_order_id=client_order_id,
    )
