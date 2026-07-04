"""
ST + DEMA M5 回踩 Alpha 引擎（纯信号，无下单）。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Optional

import redis as _redis
from nautilus_trader.indicators import AverageTrueRange, DoubleExponentialMovingAverage
from nautilus_trader.indicators.averages import MovingAverageType
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


class _DEMAState:
    def __init__(self, period: int = 21):
        self._dema = DoubleExponentialMovingAverage(period)
        self._prev: Optional[float] = None

    def update(self, close: float) -> tuple[Optional[float], int]:
        self._dema.update_raw(close)
        if not self._dema.initialized:
            return None, 0
        cur = self._dema.value
        slope = 0
        if self._prev is not None:
            slope = 1 if cur > self._prev else (-1 if cur < self._prev else 0)
        self._prev = cur
        return round(cur, 4), slope


@dataclass(frozen=True)
class StDemaM5Config:
    dema_period: int = 21
    atr_period: int = 14
    atr_mult: float = 1.5
    tp_rr: float = 2.0
    min_st_dema_spread_atr: float = 0.30
    require_close_confirm: bool = True


class StDemaM5Engine(SignalEngine):
    PROFILE = "st_dema_m5"

    def __init__(
        self,
        config: StDemaM5Config,
        redis: Optional[_redis.Redis],
        log_fn: Callable[[str], None],
    ) -> None:
        self._cfg = config
        self._redis = redis
        self._log = log_fn
        self._dema: dict[str, _DEMAState] = {}
        self._atr: dict[str, AverageTrueRange] = {}
        self._arm: dict[str, str] = {}
        self._last_close: dict[str, float] = {}

    def register_symbol(self, symbol: str) -> None:
        self._dema[symbol] = _DEMAState(self._cfg.dema_period)
        self._atr[symbol] = AverageTrueRange(self._cfg.atr_period, MovingAverageType.WILDER)
        self._arm[symbol] = "init"

    def preheat(self, symbol: str) -> None:
        if not self._redis:
            return
        try:
            raw_list = self._redis.lrange(f"bars:5m:{symbol}", 0, -1)
        except Exception as e:
            self._log(f"[Alpha/{self.PROFILE}] {symbol}: 预热失败: {e}")
            return
        n = 0
        for raw in raw_list[:-1]:
            try:
                b = json.loads(raw)
                self._dema[symbol].update(float(b["close"]))
                self._atr[symbol].update_raw(float(b["high"]), float(b["low"]), float(b["close"]))
                n += 1
            except Exception:
                continue
        if n:
            self._log(f"[Alpha/{self.PROFILE}] {symbol}: 预热 {n} 根 M5")

    def reset_symbol(self, symbol: str) -> None:
        self._arm[symbol] = "init"

    def set_last_close(self, symbol: str, price: float) -> None:
        self._last_close[symbol] = price

    def on_bar(self, ctx: BarContext, pos: PositionContext) -> SignalBarOutput:
        sym = ctx.symbol
        bar = ctx.bar
        out = SignalBarOutput()

        try:
            c = float(bar["close"])
            h = float(bar["high"])
            lo = float(bar["low"])
        except (KeyError, TypeError, ValueError):
            return out

        self._last_close[sym] = c
        dema, slope = self._dema[sym].update(c)
        self._atr[sym].update_raw(h, lo, c)

        st = float(bar.get("st_value", 0.0) or 0.0)
        st_dir = int(bar.get("st_dir", 0) or 0)
        if dema is None or not self._atr[sym].initialized or not st:
            return out

        atr = self._atr[sym].value

        # ── 退出信号（有仓时优先）────────────────────────────────────
        if pos.open_units > 0 and pos.is_long is not None:
            reason = self._reversal_reason(pos.is_long, st_dir, lo, h, c, st, dema)
            if reason:
                side = OrderSide.BUY if pos.is_long else OrderSide.SELL
                out.intents.append(TradeIntent(
                    profile=self.PROFILE, symbol=sym, action=IntentAction.EXIT,
                    side=side, ref_price=c, atr_ref=atr, bar_time=ctx.bar_time,
                    exit_reason=reason,
                ))
                return out

        dir_long = (st_dir == 1) and (slope == 1)
        dir_short = (st_dir == -1) and (slope == -1)
        trade_dir = 1 if dir_long else (-1 if dir_short else 0)

        if lo > dema or h < dema:
            self._arm[sym] = "outside"

        touch = lo <= dema <= h
        armed = self._arm[sym] == "outside"

        if ctx.mode == "observe":
            out.observe_note = (
                f"{sym} dir={'多' if trade_dir==1 else ('空' if trade_dir==-1 else '—')} "
                f"st_dir={st_dir} slope={slope} dema={dema:.2f} st={st:.2f} "
                f"spread={abs(st-dema):.2f} arm={self._arm[sym]} touch={touch}"
            )

        if trade_dir == 0 or not touch or not armed:
            return out

        if not self._passes_trend_strength(st, dema, atr):
            out.rejects.append(SignalReject(sym, "weak_trend_spread",
                                            {"spread": round(abs(st - dema), 4), "atr": round(atr, 4)}))
            return out

        if not self._passes_close_confirm(trade_dir, c, dema):
            out.rejects.append(SignalReject(sym, "close_confirm_fail", {"close": c, "dema": dema}))
            return out

        want_side = OrderSide.BUY if trade_dir == 1 else OrderSide.SELL
        sign = 1 if want_side == OrderSide.BUY else -1
        stop_px = round(c - sign * atr * self._cfg.atr_mult, 2)
        tp_px = round(c + sign * self._cfg.tp_rr * atr * self._cfg.atr_mult, 2)

        action = IntentAction.ADD if pos.open_units > 0 else IntentAction.ENTER
        seq = pos.open_units

        out.intents.append(TradeIntent(
            profile=self.PROFILE, symbol=sym, action=action, side=want_side,
            ref_price=c, atr_ref=atr, bar_time=ctx.bar_time,
            stop_px=stop_px, tp_px=tp_px, seq=seq,
        ))
        self._arm[sym] = "touching"
        return out

    def _passes_trend_strength(self, st: float, dema: float, atr: float) -> bool:
        return abs(st - dema) >= self._cfg.min_st_dema_spread_atr * atr

    def _passes_close_confirm(self, trade_dir: int, close: float, dema: float) -> bool:
        if not self._cfg.require_close_confirm:
            return True
        if trade_dir == 1:
            return close >= dema
        if trade_dir == -1:
            return close <= dema
        return False

    def _reversal_reason(
        self, is_long: bool, st_dir: int, lo: float, h: float, c: float, st: float, dema: float,
    ) -> str:
        if is_long and st_dir == -1:
            return "st_flip_bear"
        if not is_long and st_dir == 1:
            return "st_flip_bull"
        lo_band, hi_band = min(st, dema), max(st, dema)
        if lo >= lo_band and h <= hi_band:
            if is_long and c < dema:
                return "band_close_below_dema"
            if not is_long and c > dema:
                return "band_close_above_dema"
        return ""
