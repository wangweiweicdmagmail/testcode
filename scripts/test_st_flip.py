#!/usr/bin/env python3
"""SuperTrend 转向回归：多头时须跌破上一根 ST 线才翻空。"""
from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass


@dataclass
class _WilderATR:
    period: int
    _trs: list[float] | None = None
    value: float = 0.0

    def __post_init__(self) -> None:
        self._trs = []

    @property
    def initialized(self) -> bool:
        return len(self._trs) >= self.period

    def update_raw(self, h: float, lo: float, c: float, prev_close: float) -> float | None:
        if not self._trs and prev_close == 0.0:
            tr = h - lo
        else:
            tr = max(h - lo, abs(h - prev_close), abs(lo - prev_close))
        self._trs.append(tr)
        if len(self._trs) < self.period:
            return None
        if len(self._trs) == self.period:
            self.value = sum(self._trs[-self.period :]) / self.period
        else:
            self.value = (self.value * (self.period - 1) + tr) / self.period
        return self.value


class STState:
    """与 strategy._STState 转向逻辑一致（修复后：用上一根 band 判断）。"""

    def __init__(self, period: int = 10, mult: float = 3.0):
        self.period = period
        self.mult = mult
        self._atr = _WilderATR(period)
        self._prev_close = 0.0
        self._prev_upper_b = 0.0
        self._prev_lower_b = 0.0
        self._prev_dir = -1
        self._initialized = False

    def update(self, o: float, h: float, lo: float, c: float) -> tuple[float, int]:
        atr = self._atr.update_raw(h, lo, c, self._prev_close)
        if atr is None:
            self._prev_close = c
            return 0.0, 1
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


class STStateBuggy:
    """修复前：用当根收紧后的 band 判断（错误）。"""

    def __init__(self, period: int = 10, mult: float = 3.0):
        self.period = period
        self.mult = mult
        self._atr = _WilderATR(period)
        self._prev_close = 0.0
        self._prev_upper_b = 0.0
        self._prev_lower_b = 0.0
        self._prev_dir = -1
        self._initialized = False

    def update(self, o: float, h: float, lo: float, c: float) -> tuple[float, int]:
        atr = self._atr.update_raw(h, lo, c, self._prev_close)
        if atr is None:
            self._prev_close = c
            return 0.0, 1
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr
        if not self._initialized:
            upper_b, lower_b = basic_upper, basic_lower
            self._initialized = True
        else:
            pu, pl, pc = self._prev_upper_b, self._prev_lower_b, self._prev_close
            upper_b = basic_upper if (basic_upper < pu or pc > pu) else pu
            lower_b = basic_lower if (basic_lower > pl or pc < pl) else pl
        if self._prev_dir == -1 and c > upper_b:
            st_dir = 1
        elif self._prev_dir == 1 and c < lower_b:
            st_dir = -1
        else:
            st_dir = self._prev_dir
        st_val = lower_b if st_dir == 1 else upper_b
        self._prev_close, self._prev_upper_b, self._prev_lower_b, self._prev_dir = c, upper_b, lower_b, st_dir
        return round(st_val, 4), st_dir


def test_flip_bear_only_below_prev_st() -> None:
    st = STState(10, 3.0)
    for _ in range(12):
        st.update(100, 101, 99, 100)
    _, d1 = st.update(100, 102, 99, 101)
    assert d1 == 1
    prev_st = st._prev_lower_b
    _, d2 = st.update(101, 102, 100.5, prev_st + 0.5)
    assert d2 == 1
    _, d3 = st.update(101, 102, prev_st - 1, prev_st - 0.01)
    assert d3 == -1


def test_amzn_redis_1015_fixed_stays_bull() -> None:
    raw = subprocess.check_output(["redis-cli", "LRANGE", "bars:5m:AMZN", "0", "-1"], text=True)
    bars = [json.loads(x) for x in raw.strip().split("\n") if x.strip()]
    flip = next((b for b in bars if b["time"] == 1782209700), None)
    if not flip:
        return
    idx = bars.index(flip)
    if idx < 1:
        return
    prev = bars[idx - 1]
    fixed = STState(10, 3.0)
    for b in bars[:idx]:
        fixed.update(b["open"], b["high"], b["low"], b["close"])
    st_val, st_dir = fixed.update(flip["open"], flip["high"], flip["low"], flip["close"])
    assert prev["st_dir"] == 1, "前置条件：10:10 应为多头"
    assert flip["close"] > prev["st_value"], (
        f"收盘 {flip['close']} 未跌破前 ST {prev['st_value']}"
    )
    assert st_dir == 1, (
        f"修复后 10:15 不得翻空: close={flip['close']} prev_ST={prev['st_value']} "
        f"got dir={st_dir} ST={st_val} (redis错误记录 dir={flip['st_dir']})"
    )


def test_double_update_same_m5_bar_corrupts_direction() -> None:
    """同一根 M5 OHLC 若 update 两次，第二次会用当根收紧 band 误翻空（引擎不得双喂）。"""
    st = STState(10, 3.0)
    for _ in range(12):
        st.update(100, 101, 99, 100)
    st.update(100, 102, 99, 101)
    prev_lo = st._prev_lower_b
    o, h, lo, c = 101, 102, 100.5, prev_lo + 0.5
    _, d1 = st.update(o, h, lo, c)
    assert d1 == 1
    _, d2 = st.update(o, h, lo, c)
    assert d2 == -1, "双喂演示：第二次 update 会误翻空，故 on_historical flush 后须 return"


if __name__ == "__main__":
    test_flip_bear_only_below_prev_st()
    test_amzn_redis_1015_fixed_stays_bull()
    test_double_update_same_m5_bar_corrupts_direction()
    print("OK: SuperTrend flip tests passed")
