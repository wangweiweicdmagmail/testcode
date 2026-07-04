"""
信号层契约 — Alpha 引擎只输出 TradeIntent，不接触订单 API。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from nautilus_trader.model.enums import OrderSide


class IntentAction(str, Enum):
    ENTER = "enter"
    ADD = "add"
    EXIT = "exit"


@dataclass(frozen=True)
class BarContext:
    """单根 M5 bar 收盘时的市场上下文。"""
    symbol: str
    bar: dict[str, Any]
    bar_time: int
    et_min: int
    mode: str          # off | observe | live


@dataclass(frozen=True)
class PositionContext:
    """执行层提供的持仓快照，供 Alpha 判断退出/加仓。"""
    open_units: int
    is_long: Optional[bool]
    all_breakeven: bool
    unit_sides: tuple[OrderSide, ...]


@dataclass(frozen=True)
class TradeIntent:
    """标准化交易意图 — 执行层唯一输入。"""
    profile: str
    symbol: str
    action: IntentAction
    side: OrderSide
    ref_price: float
    atr_ref: float
    bar_time: int
    stop_px: Optional[float] = None
    tp_px: Optional[float] = None
    exit_reason: Optional[str] = None
    seq: int = 0
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalReject:
    symbol: str
    reason: str
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class SignalBarOutput:
    intents: list[TradeIntent] = field(default_factory=list)
    rejects: list[SignalReject] = field(default_factory=list)
    observe_note: Optional[str] = None


class SignalEngine(ABC):
    """可插拔 Alpha 引擎基类。"""

    PROFILE: str = ""

    @abstractmethod
    def preheat(self, symbol: str) -> None: ...

    @abstractmethod
    def on_bar(self, ctx: BarContext, pos: PositionContext) -> SignalBarOutput: ...

    @abstractmethod
    def reset_symbol(self, symbol: str) -> None: ...

    @abstractmethod
    def set_last_close(self, symbol: str, price: float) -> None: ...
