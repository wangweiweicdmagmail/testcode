"""执行层数据模型。"""
from dataclasses import dataclass
from enum import Enum

from nautilus_trader.model.enums import OrderSide


class UnitState(str, Enum):
    PENDING_ENTRY = "pending_entry"
    ACTIVE = "active"
    BREAKEVEN = "breakeven"
    CLOSED = "closed"


@dataclass
class Unit:
    sym: str
    seq: int
    side: OrderSide
    state: UnitState
    qty: int = 0
    atr_ref: float = 0.0
    entry_px: float = 0.0
    hard_stop_px: float = 0.0
    risk_per_share: float = 0.0
    tp_px: float = 0.0
    entry_coid: str = ""
    stop_coid: str = ""
    tp_coid: str = ""
    entry_filled: int = 0
    tp_filled: int = 0
    planned_stop_px: float = 0.0
    planned_tp_rr: float = 0.0
    manual_remainder: bool = False
