"""
进场方法（EntryMethod）— 控制台发起的进场意图构造器。

与 SignalEngine 的区别：SignalEngine 是 bar 驱动、每根 bar 主动产出 TradeIntent（Alpha）；
EntryMethod 是用户按需发起的"进场意图构造器"，把"想怎么进场"翻译成 TradeIntent + 订单指令，
统一经 TradeIntent → RiskGate → AutoPM 现有管道，复用全部风控 / bracket / 恢复。

四种方法：
  manual_limit — 用户指定价位的 GTC 限价（resting）
  ema          — 以 DEMA20（≈EMA20）为挂单价位的 GTC 限价（resting）
  st_limit     — 以 SuperTrend 线价位为挂单价位的 GTC 限价（resting）
  conditional  — 不立即下单；arming 一张票，bar 驱动判定触发后转 marketable

止损 / 止盈用百分比口径（自洽，不依赖 ATR），便于用户直觉设定；RiskGate 走 size_by_stop 以损定量。
"""
from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

from nautilus_trader.model.enums import OrderSide

from signals.base import IntentAction, TradeIntent


class EntryBuildError(Exception):
    """build_intent / parse 失败 — 携带 reason + meta 供上层 publish entry_rejected。"""

    def __init__(self, reason: str, **meta: Any) -> None:
        super().__init__(reason)
        self.reason = reason
        self.meta = meta


@dataclass(frozen=True)
class OrderDirective:
    """build_intent 返回的执行指令。"""
    kind: str                                # "marketable" | "resting_limit" | "arm"
    limit_price: Optional[float] = None      # resting_limit 时的挂单价


@dataclass(frozen=True)
class EntryRequest:
    """从 auto:enter payload 解析的进场请求。"""
    method: str                              # manual_limit | ema | st_limit | conditional
    symbol: str
    side: OrderSide
    # manual_limit
    limit_price: Optional[float] = None
    # ema
    ema_period: int = 20
    # st_limit
    st_field: str = "value"                  # value | upper | lower（取自 indicators:active.supertrend）
    # 通用止损 / 止盈（以损定量：仓位 = risk_budget / |entry - stop|）
    stop_price: Optional[float] = None       # 直接指定止损价（核心；缺省回退 1% 结构止损）
    tp_rr: float = 2.0                       # 半仓止盈的 RR（默认 2）
    # conditional
    trigger: Optional[dict[str, Any]] = None  # {type: price_cross, direction: up|down, level: float}
    expire_ts: Optional[int] = None
    bypass_window: bool = False              # 是否允许在 RTH 黑窗外进场（熔断不可绕过）
    operator: str = "console"


def _side_sign(side: OrderSide) -> int:
    return 1 if side == OrderSide.BUY else -1


def _build_intent(
    req: EntryRequest, ref_price: float, bar_time: int, profile: str, *,
    resting: bool, extra_meta: Optional[dict[str, Any]] = None,
) -> TradeIntent:
    """按百分比口径构造带结构止损/止盈的 TradeIntent（long: stop<ref<tp；short 反之）。"""
    sign = _side_sign(req.side)
    # 止损价：用户直接指定（以损定量的核心）；缺省回退 1% 结构止损
    if req.stop_price and float(req.stop_price) > 0:
        stop_px = round(float(req.stop_price), 2)
    else:
        stop_px = round(ref_price - sign * ref_price * 0.01, 2)
    # 止损侧校验：必须在错误侧之外（否则 STOP 即时触发 / 裸仓）
    if (sign == 1 and stop_px >= ref_price) or (sign == -1 and stop_px <= ref_price):
        raise EntryBuildError("stop_wrong_side", side=req.side.name, stop=stop_px, ref=ref_price)
    risk = abs(ref_price - stop_px)
    tp_px = round(ref_price + sign * risk * req.tp_rr, 2)
    meta: dict[str, Any] = {
        "profile": profile,
        "method": req.method,
        "stop_price": stop_px,
        "tp_rr": req.tp_rr,
        "operator": req.operator,
    }
    if resting:
        meta["resting_limit_price"] = ref_price     # → AutoPM 挂 GTC 限价于此
    if extra_meta:
        meta.update(extra_meta)
    return TradeIntent(
        profile=profile,
        symbol=req.symbol,
        action=IntentAction.ENTER,
        side=req.side,
        ref_price=ref_price,
        atr_ref=risk,                               # 每股风险，供 AutoPM fallback 口径
        bar_time=bar_time,
        stop_px=stop_px,
        tp_px=tp_px,
        meta=meta,
    )


def _ema_level(levels: dict[str, Any]) -> Optional[float]:
    val = levels.get("dema20")
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _st_level(levels: dict[str, Any], field: str) -> Optional[float]:
    st = levels.get("supertrend")
    if not isinstance(st, dict):
        return None
    val = st.get(field)
    if val is None and field != "value":
        val = st.get("value")
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


class EntryMethod(ABC):
    """可插拔进场方法基类。"""

    PROFILE: str = ""

    @abstractmethod
    def build_intent(
        self, req: EntryRequest, levels: dict[str, Any], last_close: float, bar_time: int,
    ) -> tuple[Optional[TradeIntent], OrderDirective]:
        """返回 (intent, directive)。
        非条件方法：intent 有值，directive 为 marketable / resting_limit（level 缺失时抛 EntryBuildError）。
        条件方法：intent=None，directive.kind="arm"（触发时再由 build_trigger_intent 组装）。
        """

    def is_conditional(self) -> bool:
        return False

    def build_trigger_intent(
        self, req: EntryRequest, trigger_close: float, bar_time: int,
    ) -> tuple[TradeIntent, OrderDirective]:
        """仅 conditional 实现：触发时以触发价 ref 组装 marketable intent。"""
        raise NotImplementedError

    def check_trigger(self, req: EntryRequest, bar: dict[str, Any], levels: dict[str, Any]) -> bool:
        """仅 conditional 实现：当前 bar 是否满足触发条件。"""
        return False


class ManualLimitEntry(EntryMethod):
    PROFILE = "manual_entry"

    def build_intent(self, req, levels, last_close, bar_time):
        if not req.limit_price or req.limit_price <= 0:
            raise EntryBuildError("invalid_limit_price", limit_price=req.limit_price)
        price = float(req.limit_price)
        intent = _build_intent(req, price, bar_time, self.PROFILE, resting=True,
                               extra_meta={"limit_price": price})
        return intent, OrderDirective(kind="resting_limit", limit_price=price)


class EmaEntry(EntryMethod):
    PROFILE = "ema_entry"

    def build_intent(self, req, levels, last_close, bar_time):
        level = _ema_level(levels)
        if level is None or level <= 0:
            raise EntryBuildError("level_unavailable", which="dema20")
        intent = _build_intent(req, level, bar_time, self.PROFILE, resting=True,
                               extra_meta={"ema_source": "indicators:active",
                                           "ema_period": req.ema_period})
        return intent, OrderDirective(kind="resting_limit", limit_price=level)


class StLimitEntry(EntryMethod):
    PROFILE = "st_entry"

    def build_intent(self, req, levels, last_close, bar_time):
        level = _st_level(levels, req.st_field)
        if level is None or level <= 0:
            raise EntryBuildError("level_unavailable", which=f"supertrend.{req.st_field}")
        intent = _build_intent(req, level, bar_time, self.PROFILE, resting=True,
                               extra_meta={"st_field": req.st_field})
        return intent, OrderDirective(kind="resting_limit", limit_price=level)


class MarketEntry(EntryMethod):
    PROFILE = "market_entry"

    def build_intent(self, req, levels, last_close, bar_time):
        if not last_close or last_close <= 0:
            raise EntryBuildError("no_last_price")
        intent = _build_intent(req, float(last_close), bar_time, self.PROFILE, resting=False,
                               extra_meta={"entry_kind": "market"})
        return intent, OrderDirective(kind="marketable")


class ConditionalEntry(EntryMethod):
    PROFILE = "conditional_entry"

    def is_conditional(self) -> bool:
        return True

    def build_intent(self, req, levels, last_close, bar_time):
        t = req.trigger or {}
        if str(t.get("type", "")).lower() != "price_cross" or not t.get("level"):
            raise EntryBuildError("invalid_trigger", trigger=t)
        if str(t.get("direction", "")).lower() not in ("up", "down"):
            raise EntryBuildError("invalid_trigger_direction", trigger=t)
        return None, OrderDirective(kind="arm")

    def check_trigger(self, req, bar, levels):
        t = req.trigger or {}
        try:
            close = float(bar.get("close"))
            level = float(t.get("level"))
        except (TypeError, ValueError):
            return False
        if close <= 0 or level <= 0:
            return False
        direction = str(t.get("direction", "")).lower()
        if direction == "up" and close >= level:
            return True
        if direction == "down" and close <= level:
            return True
        return False

    def build_trigger_intent(self, req, trigger_close, bar_time):
        intent = _build_intent(req, trigger_close, bar_time, self.PROFILE, resting=False,
                               extra_meta={"trigger": dict(req.trigger or {}),
                                           "trigger_close": trigger_close})
        return intent, OrderDirective(kind="marketable")


ENTRY_METHODS: dict[str, EntryMethod] = {
    "market": MarketEntry(),
    "manual_limit": ManualLimitEntry(),
    "ema": EmaEntry(),
    "st_limit": StLimitEntry(),
    "conditional": ConditionalEntry(),
}


def parse_entry_request(payload: dict[str, Any]) -> EntryRequest:
    """从 auto:enter payload 解析 EntryRequest（非法时抛 EntryBuildError）。"""
    method = str(payload.get("method", "")).lower()
    if method not in ENTRY_METHODS:
        raise EntryBuildError("unknown_method", method=method)
    sym = str(payload.get("symbol", "")).upper()
    if not sym:
        raise EntryBuildError("missing_symbol")
    side_str = str(payload.get("side", "")).upper()
    if side_str in ("LONG", "BUY"):
        side = OrderSide.BUY
    elif side_str in ("SHORT", "SELL"):
        side = OrderSide.SELL
    else:
        raise EntryBuildError("invalid_side", side=side_str)
    limit_price = payload.get("limit_price")
    stop_price = payload.get("stop_price")
    expire_ts = payload.get("expire_ts")
    try:
        return EntryRequest(
            method=method,
            symbol=sym,
            side=side,
            limit_price=float(limit_price) if limit_price not in (None, "") else None,
            ema_period=int(payload.get("ema_period", 20)),
            st_field=str(payload.get("st_field", "value")).lower(),
            stop_price=float(stop_price) if stop_price not in (None, "") else None,
            tp_rr=float(payload.get("tp_rr", 2.0)),
            trigger=payload.get("trigger"),
            expire_ts=int(expire_ts) if expire_ts not in (None, "") else None,
            bypass_window=bool(payload.get("bypass_window", False)),
            operator=str(payload.get("operator", "console")),
        )
    except (TypeError, ValueError) as e:
        raise EntryBuildError("invalid_params", detail=str(e))


def req_to_dict(req: EntryRequest) -> dict[str, Any]:
    """EntryRequest → 可 JSON 序列化 dict（ticket_store 持久化用）。"""
    d = dataclasses.asdict(req)
    d["side"] = req.side.name
    return d


def req_from_dict(d: dict[str, Any]) -> EntryRequest:
    """dict → EntryRequest（恢复 armed 票时用）。"""
    raw = dict(d)
    side_name = str(raw.pop("side", ""))
    try:
        side = OrderSide[side_name]
    except KeyError:
        raise EntryBuildError("invalid_side", side=side_name)
    trigger = raw.get("trigger")
    if isinstance(trigger, str):
        import json as _json
        try:
            trigger = _json.loads(trigger)
        except (ValueError, TypeError):
            trigger = None
        raw["trigger"] = trigger
    return EntryRequest(side=side, **raw)
