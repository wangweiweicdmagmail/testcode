"""
超级信号（st_super）：5m ST 定方向 + 1m ST 翻回同向入场，止损 = 1m ST 值。

节奏（与四宫格 M1 细线一致）：
  - 1m ST **每根 M1 收盘在内部计算**（状态机连续）
  - **对外展示 / 超级信号检测** 仅在 M5 桶收盘时刷新（每 5 分钟）

参数（与四宫格前端一致，可通过环境变量覆盖）：
  ST_SUPER_PERIOD=10  ST_SUPER_MULT_1M=3.0  ST_SUPER_MULT_5M=3.5
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

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


# SuperTrend 状态机已合并至 indicators.supertrend.STState（设计原则 #2）。


@dataclass
class StSuperSymbolState:
    """st_super 翻转检测的轻量状态：仅保留跨 bar 的 st5_dir / prev1_dir。

    M1/M5 ST 值由指标策略算好后写入共享 registry / bar_dict，本类不再自维护 STState
    （设计原则 #2：消除重复指标计算）。
    """
    st5_dir: int = 0
    prev1_dir: Optional[int] = None

    @classmethod
    def create(cls) -> StSuperSymbolState:
        return cls()


def bucket5m(bar_time: int) -> int:
    """ET fake-UTC 时间戳 → M5 桶起始秒。"""
    return int(bar_time) - (int(bar_time) % 300)


def _bar_et_minutes(bar_time: int) -> int:
    return (bar_time % 86400) // 60


def _tradable_minutes(bar_time: int) -> bool:
    em = _bar_et_minutes(bar_time)
    return RTH_ENTRY_START <= em < RTH_ENTRY_END


def _m1_st_from_bar(m1_bar: dict[str, Any]) -> tuple[float, int]:
    """从 bar_dict 读 M1 ST（指标策略已算好写入）。无值返回 (0.0, 0)。"""
    raw_dir = m1_bar.get("st_dir")
    raw_val = m1_bar.get("st_value")
    if raw_dir is not None and raw_val is not None:
        try:
            st_dir = int(raw_dir)
            st_val = float(raw_val)
            if st_val > 0 and st_dir in (1, -1):
                return st_val, st_dir
        except (TypeError, ValueError):
            pass
    return 0.0, 0


def _m5_dir_from_bar(m5_bar: dict[str, Any]) -> int:
    """从 bar_dict 读 M5 ST 方向。无值返回 0。"""
    raw_dir = m5_bar.get("st_dir")
    if raw_dir is not None:
        try:
            d = int(raw_dir)
            if d in (1, -1):
                return d
        except (TypeError, ValueError):
            pass
    return 0


def _m1_bucket_end_bars(m1_bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """每桶取最后一根 M1（1m ST 对外仅在此刻刷新）。"""
    m1 = sorted(m1_bars, key=lambda b: int(b.get("time") or 0))
    if not m1:
        return []
    ends: dict[int, dict[str, Any]] = {}
    for i, b in enumerate(m1):
        bt = bucket5m(int(b["time"]))
        nxt = m1[i + 1] if i + 1 < len(m1) else None
        if nxt is None or bucket5m(int(nxt["time"])) != bt:
            ends[bt] = b
    return [ends[k] for k in sorted(ends)]


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
    for b in _m1_bucket_end_bars(m1):
        st_val, st_dir = _m1_st_from_bar(b)
        if st_val > 0 and st_dir in (1, -1):
            state.prev1_dir = st_dir


def replay_st_super_touches(
    symbol: str,
    m5_bars: list[dict[str, Any]],
    m1_bars: list[dict[str, Any]],
) -> tuple[StSuperSymbolState, list[TouchEvent]]:
    """回放当日 RTH；超级信号仅在 M5 桶收盘节奏检测。"""
    state = StSuperSymbolState.create()
    warmup_st_super_state(state, m5_bars, [], rth_only=True)
    rth_lo, rth_hi = 9 * 3600 + 30 * 60, 16 * 3600
    m1 = sorted(m1_bars, key=lambda b: int(b.get("time") or 0))
    m1 = [b for b in m1 if rth_lo <= int(b["time"]) % 86400 < rth_hi]
    events: list[TouchEvent] = []
    for b in m1:
        snap = dict(b)
        st_val, st_dir = _m1_st_from_bar(b)
        if st_val > 0 and st_dir in (1, -1):
            snap["st_value"] = st_val
            snap["st_dir"] = st_dir
        ev = detect_st_super_flip(symbol, snap, state)
        if ev:
            events.append(ev)
    return state, events


def detect_st_super_flip(
    symbol: str,
    m1_bar: dict[str, Any],
    state: StSuperSymbolState,
) -> Optional[TouchEvent]:
    """M5 桶收盘节奏：若 1m ST 相对上一档已翻转且与 5m ST 同向 → 超级信号。

    调用方应在 M5 收盘时传入该桶最后一根 M1 的 OHLC + 已冻结的 st_dir/st_value。
    """
    try:
        bar_time = int(m1_bar.get("time") or 0)
        c = float(m1_bar["close"])
        h = float(m1_bar["high"])
        lo = float(m1_bar["low"])
    except (KeyError, TypeError, ValueError):
        return None
    if not bar_time:
        return None

    st_val, st_dir = _m1_st_from_bar(m1_bar)
    if st_val <= 0 or st_dir not in (1, -1):
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
    d = _m5_dir_from_bar(m5_bar)
    if d in (1, -1):
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
