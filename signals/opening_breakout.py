"""开盘突破：盘前锚定 M1 + M5 ST 共振，开盘后 5 分钟内 M1 触线触发。"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

import redis as _redis
from nautilus_trader.model.enums import OrderSide

from signals.base import (
    BarContext,
    IntentAction,
    PositionContext,
    SignalBarOutput,
    SignalEngine,
    SignalReject,
    TradeIntent,
)

RTH_OPEN_ET_MIN = 9 * 60 + 30
OPENING_WINDOW_END_ET_MIN = 9 * 60 + 35
PROFILE = "opening_breakout"
TP_RR = 2.0


@dataclass(frozen=True)
class OpeningBreakoutConfig:
    allow_long: bool = True
    allow_short: bool = True


class OpeningBreakoutEngine(SignalEngine):
    PROFILE = PROFILE

    def __init__(
        self,
        config: OpeningBreakoutConfig,
        redis: Optional[_redis.Redis],
        log_fn: Callable[[str], None],
    ) -> None:
        self._cfg = config
        self._redis = redis
        self._log = log_fn
        self._dirs: dict[str, tuple[bool, bool]] = {}

    def set_directions(self, symbol: str, allow_long: bool, allow_short: bool) -> None:
        self._dirs[symbol.upper()] = (allow_long, allow_short)

    def _allow_long(self, sym: str) -> bool:
        return self._dirs.get(sym.upper(), (self._cfg.allow_long, self._cfg.allow_short))[0]

    def _allow_short(self, sym: str) -> bool:
        return self._dirs.get(sym.upper(), (self._cfg.allow_long, self._cfg.allow_short))[1]

    def preheat(self, symbol: str) -> None:
        return

    def set_last_close(self, symbol: str, close: float) -> None:
        return

    def reset_symbol(self, symbol: str) -> None:
        return

    def on_bar(self, ctx: BarContext, pos: PositionContext) -> SignalBarOutput:
        """M5 接口未使用；由 Runner 调用 on_m1。"""
        return SignalBarOutput()

    def on_m1(self, ctx: BarContext, pos: PositionContext) -> SignalBarOutput:
        out = SignalBarOutput()
        sym = ctx.symbol
        et_min = ctx.et_min

        if et_min < RTH_OPEN_ET_MIN or et_min >= OPENING_WINDOW_END_ET_MIN:
            return out

        if pos.open_units > 0:
            out.rejects.append(SignalReject(sym, "already_in_position"))
            return out

        if not self._redis:
            out.rejects.append(SignalReject(sym, "no_redis"))
            return out

        session_date = self._session_date(ctx.bar_time)
        if self._already_fired(sym, session_date):
            out.rejects.append(SignalReject(sym, "already_fired"))
            return out

        ref = self._load_ref(sym)
        if not ref:
            out.rejects.append(SignalReject(sym, "premarket_ref_missing"))
            return out

        if str(ref.get("session_date")) != session_date:
            out.rejects.append(SignalReject(sym, "premarket_ref_stale"))
            return out

        if not ref.get("armed_short") and not ref.get("armed_long"):
            out.rejects.append(SignalReject(sym, "not_armed"))
            return out

        bar = ctx.bar
        try:
            o, h, l, c = float(bar["open"]), float(bar["high"]), float(bar["low"]), float(bar["close"])
        except (KeyError, TypeError, ValueError):
            out.rejects.append(SignalReject(sym, "bad_bar"))
            return out

        anchor = ref.get("m1_anchor") or {}
        try:
            a_low = float(anchor["low"])
            a_high = float(anchor["high"])
        except (KeyError, TypeError, ValueError):
            out.rejects.append(SignalReject(sym, "bad_anchor"))
            return out

        side: Optional[OrderSide] = None
        stop_px: Optional[float] = None
        if ref.get("armed_short") and self._allow_short(sym) and l <= a_low:
            side = OrderSide.SELL
            stop_px = h
        elif ref.get("armed_long") and self._allow_long(sym) and h >= a_high:
            side = OrderSide.BUY
            stop_px = l

        if side is None:
            out.rejects.append(SignalReject(sym, "no_breakout"))
            return out

        entry = c
        if stop_px is None or entry <= 0:
            out.rejects.append(SignalReject(sym, "invalid_stop"))
            return out

        risk = abs(entry - stop_px)
        if risk <= 0:
            out.rejects.append(SignalReject(sym, "stop_too_tight"))
            return out

        sign = 1 if side == OrderSide.BUY else -1
        tp_px = round(entry + sign * TP_RR * risk, 4)

        intent = TradeIntent(
            profile=PROFILE,
            symbol=sym,
            action=IntentAction.ENTER,
            side=side,
            ref_price=entry,
            atr_ref=risk,
            bar_time=ctx.bar_time,
            stop_px=stop_px,
            tp_px=tp_px,
            meta={
                "opening_breakout": True,
                "anchor_low": a_low,
                "anchor_high": a_high,
                "trigger_bar_time": ctx.bar_time,
                "tp_rr": TP_RR,
                "manual_remainder": True,
            },
        )
        out.intents.append(intent)
        self._log(
            f"[OpeningBreakout] {sym} 信号 {side.name} entry≈{entry} stop={stop_px} "
            f"tp_half@2R={tp_px} anchor L={a_low} H={a_high}"
        )
        return out

    def confirm_trigger(
        self,
        sym: str,
        session_date: str,
        intent: TradeIntent,
        ref: dict[str, Any],
        mode: str = "live",
    ) -> None:
        """风控/执行通过后调用：写入 fired 并推送前端。"""
        self._mark_fired(sym, session_date, intent)
        self._publish_signal(sym, intent, ref, mode)

    def _session_date(self, bar_time: int) -> str:
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(bar_time, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")

    def _ref_key(self, sym: str) -> str:
        return f"premarket:ref:{sym.upper()}"

    def _fired_key(self, sym: str, session_date: str) -> str:
        return f"opening_breakout:fired:{sym.upper()}:{session_date}"

    def _load_ref(self, sym: str) -> Optional[dict[str, Any]]:
        try:
            raw = self._redis.get(self._ref_key(sym))
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def _already_fired(self, sym: str, session_date: str) -> bool:
        try:
            return bool(self._redis.get(self._fired_key(sym, session_date)))
        except Exception:
            return False

    def _mark_fired(self, sym: str, session_date: str, intent: TradeIntent) -> None:
        payload = {
            "symbol": sym,
            "session_date": session_date,
            "side": intent.side.name,
            "entry": intent.ref_price,
            "stop": intent.stop_px,
            "tp": intent.tp_px,
            "fired_at": int(time.time()),
        }
        try:
            self._redis.setex(self._fired_key(sym, session_date), 48 * 3600, json.dumps(payload))
        except Exception:
            pass

    def _publish_signal(
        self, sym: str, intent: TradeIntent, ref: dict, mode: str = "live",
    ) -> None:
        try:
            self._redis.publish(
                "opening_breakout:signal",
                json.dumps({
                    "symbol": sym,
                    "mode": mode,
                    "side": "LONG" if intent.side == OrderSide.BUY else "SHORT",
                    "entry": intent.ref_price,
                    "stop": intent.stop_px,
                    "tp": intent.tp_px,
                    "anchor": ref.get("m1_anchor"),
                    "armed_short": ref.get("armed_short"),
                    "armed_long": ref.get("armed_long"),
                    "bar_time": intent.bar_time,
                }, ensure_ascii=False),
            )
        except Exception:
            pass
