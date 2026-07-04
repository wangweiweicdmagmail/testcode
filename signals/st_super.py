"""
超级信号（st_super）：5m ST 定方向 + 1m ST 翻回同向入场，止损 = 1m ST 值。

参数（与四宫格前端一致，可通过环境变量覆盖）：
  ST_SUPER_PERIOD=10  ST_SUPER_MULT_1M=3.0  ST_SUPER_MULT_5M=3.5
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from nautilus_trader.indicators import AverageTrueRange
from nautilus_trader.indicators.averages import MovingAverageType

from signals.indicators import bar_et_date
from signals.touch_detector import TouchEvent

SIGNAL_ST_SUPER = "st_super"
EXECUTION_MODE_ST_SUPER = "st_super_immediate"

ST_PERIOD = int(os.environ.get("ST_SUPER_PERIOD", "10"))
ST_MULT_1M = float(os.environ.get("ST_SUPER_MULT_1M", "3.0"))
ST_MULT_5M = float(os.environ.get("ST_SUPER_MULT_5M", "3.5"))
ST_SUPER_TP_RR = float(os.environ.get("ST_SUPER_TP_RR", "2.0"))
ST_SUPER_CONFIDENCE = float(os.environ.get("ST_SUPER_CONFIDENCE", "0.75"))

RTH_OPEN_MIN = 9 * 60 + 30
RTH_CLOSE_MIN = 16 * 60
# 与 portfolio/risk_gate 默认一致（开盘 blackout + 尾盘 blackout）
RTH_OPEN_BLACKOUT_MIN = int(os.environ.get("RTH_OPEN_BLACKOUT_MIN", "15"))
RTH_PRE_EOD_BLACKOUT_MIN = int(os.environ.get("RTH_PRE_EOD_BLACKOUT_MIN", "30"))
RTH_ENTRY_START = RTH_OPEN_MIN + RTH_OPEN_BLACKOUT_MIN   # 09:45
RTH_ENTRY_END = RTH_CLOSE_MIN - RTH_PRE_EOD_BLACKOUT_MIN  # 15:30


class STState:
    """与 strategy._STState 一致：Wilder ATR + 上一根 band 转向。"""

    def __init__(self, period: int, mult: float):
        self.period = period
        self.mult = mult
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close = 0.0
        self._prev_upper_b = 0.0
        self._prev_lower_b = 0.0
        self._prev_dir = -1
        self._initialized = False

    @property
    def initialized(self) -> bool:
        return self._initialized

    def update(self, o: float, h: float, lo: float, c: float) -> tuple[float, int]:
        self._atr.update_raw(h, lo, c)
        if not self._atr.initialized:
            self._prev_close = c
            return 0.0, 1
        atr = self._atr.value
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr
        flip_upper, flip_lower = self._prev_upper_b, self._prev_lower_b
        if not self._initialized:
            upper_b, lower_b = basic_upper, basic_lower
            self._initialized = True
        else:
            pu, pl, pc = self._prev_upper_b, self._prev_lower_b, self._prev_close
            upper_b = basic_upper if (basic_upper < pu or pc > pu) else pu
            lower_b = basic_lower if (basic_lower > pl or pc < pl) else pl
        if self._prev_dir == -1 and c > flip_upper:
            st_dir = 1
        elif self._prev_dir == 1 and c < flip_lower:
            st_dir = -1
        else:
            st_dir = self._prev_dir
        st_val = lower_b if st_dir == 1 else upper_b
        self._prev_close, self._prev_upper_b, self._prev_lower_b, self._prev_dir = c, upper_b, lower_b, st_dir
        return round(st_val, 4), st_dir


@dataclass
class StSuperSymbolState:
    st1: STState
    st5: STState
    st5_dir: int = 0
    prev1_dir: Optional[int] = None

    @classmethod
    def create(cls) -> StSuperSymbolState:
        return cls(st1=STState(ST_PERIOD, ST_MULT_1M), st5=STState(ST_PERIOD, ST_MULT_5M))


def _bar_et_minutes(bar_time: int) -> int:
    return (bar_time % 86400) // 60


def _tradable_minutes(bar_time: int) -> bool:
    em = _bar_et_minutes(bar_time)
    return RTH_ENTRY_START <= em < RTH_ENTRY_END


def warmup_st_super_state(
    state: StSuperSymbolState,
    m5_bars: list[dict[str, Any]],
    m1_bars: list[dict[str, Any]],
    *,
    rth_only: bool = True,
) -> None:
    """从 Redis 历史 K 线恢复 st5_dir / st1 / prev1_dir（不 emit touch）。"""
    rth_lo, rth_hi = 9 * 3600 + 30 * 60, 16 * 3600
    m5 = sorted(m5_bars, key=lambda b: int(b.get("time") or 0))
    m1 = sorted(m1_bars, key=lambda b: int(b.get("time") or 0))
    if rth_only:
        m5 = [b for b in m5 if rth_lo <= int(b["time"]) % 86400 < rth_hi]
        m1 = [b for b in m1 if rth_lo <= int(b["time"]) % 86400 < rth_hi]
    for b in m5:
        update_st5_from_m5_bar(b, state)
    for b in m1:
        try:
            o, h, lo, c = (
                float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
        _, st_dir = state.st1.update(o, h, lo, c)
        if state.st1.initialized and st_dir:
            state.prev1_dir = st_dir


def replay_st_super_touches(
    symbol: str,
    m5_bars: list[dict[str, Any]],
    m1_bars: list[dict[str, Any]],
) -> tuple[StSuperSymbolState, list[TouchEvent]]:
    """回放当日 RTH M1，返回 (最终状态, 超级信号列表)。"""
    state = StSuperSymbolState.create()
    warmup_st_super_state(state, m5_bars, [], rth_only=True)
    rth_lo, rth_hi = 9 * 3600 + 30 * 60, 16 * 3600
    m1 = sorted(m1_bars, key=lambda b: int(b.get("time") or 0))
    m1 = [b for b in m1 if rth_lo <= int(b["time"]) % 86400 < rth_hi]
    events: list[TouchEvent] = []
    for b in m1:
        ev = detect_st_super_flip(symbol, b, state)
        if ev:
            events.append(ev)
    return state, events


def detect_st_super_flip(
    symbol: str,
    m1_bar: dict[str, Any],
    state: StSuperSymbolState,
) -> Optional[TouchEvent]:
    """M1 收盘：若 1m ST 翻转且与当前 5m ST 同向 → 超级信号 TouchEvent。"""
    try:
        o, h, lo, c = float(m1_bar["open"]), float(m1_bar["high"]), float(m1_bar["low"]), float(m1_bar["close"])
        bar_time = int(m1_bar.get("time") or 0)
    except (KeyError, TypeError, ValueError):
        return None
    if not bar_time:
        return None

    st_val, st_dir = state.st1.update(o, h, lo, c)
    if not state.st1.initialized or st_val <= 0 or st_dir == 0:
        if st_dir != 0:
            state.prev1_dir = st_dir
        return None

    prev = state.prev1_dir
    state.prev1_dir = st_dir
    if prev is None or st_dir == prev:
        return None
    if not _tradable_minutes(bar_time):
        return None
    if state.st5_dir == 0 or state.st5_dir != st_dir:
        return None

    side = "LONG" if st_dir == 1 else "SHORT"
    session_date = bar_et_date(m1_bar) or ""
    thesis = (
        f"超级信号：1m ST 翻{'多' if st_dir == 1 else '空'}与 5m ST 同向；"
        f"入场≈{c:.2f} 止损(1m ST)={st_val:.2f}"
    )
    return TouchEvent(
        symbol=symbol.upper(),
        signal_type=SIGNAL_ST_SUPER,
        side=side,
        trigger_level=st_val,
        touch_time=bar_time,
        m1_bar_time=bar_time,
        m5_context_bar_time=None,
        session_date=session_date,
        m1_high=h,
        m1_low=lo,
        m1_close=c,
        reclaim=True,
        rule_confidence=ST_SUPER_CONFIDENCE,
        rule_thesis=thesis,
    )


def update_st5_from_m5_bar(m5_bar: dict[str, Any], state: StSuperSymbolState) -> None:
    try:
        o, h, lo, c = float(m5_bar["open"]), float(m5_bar["high"]), float(m5_bar["low"]), float(m5_bar["close"])
    except (KeyError, TypeError, ValueError):
        return
    _, d = state.st5.update(o, h, lo, c)
    if state.st5.initialized:
        state.st5_dir = d


def pricing_from_st_super(event: TouchEvent) -> Optional[dict[str, float]]:
    """entry=收盘, stop=1m ST(trigger_level), tp=TP_RR×R。"""
    entry = round(event.m1_close, 2)
    stop = round(event.trigger_level, 2)
    is_long = event.side.upper() == "LONG"
    risk = round(entry - stop, 4) if is_long else round(stop - entry, 4)
    if risk <= 0:
        return None
    sign = 1 if is_long else -1
    tp = round(entry + sign * ST_SUPER_TP_RR * risk, 2)
    reward = round(abs(tp - entry), 4)
    rr = round(reward / risk, 2)
    return {
        "entry_price": entry,
        "stop_price": stop,
        "tp_half_price": tp,
        "tp_price": tp,
        "pullback_extreme": stop,
        "prior_swing": tp,
        "risk_est": risk,
        "reward_half_est": reward,
        "rr_half_est": rr,
    }
