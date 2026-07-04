#!/usr/bin/env python3
"""Session VWAP 回归：零成交量不累计；poll 回放不得双喂。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from signals.indicators import session_vwap


class _SessionVWAPState:
    """与 strategy._SessionVWAPState 同逻辑（测试用副本）。"""

    def __init__(self) -> None:
        self._date = None
        self._pv = 0.0
        self._vol = 0.0

    def update(self, session_date: str, high: float, low: float, close: float, volume: int):
        if self._date != session_date:
            self._date = session_date
            self._pv = 0.0
            self._vol = 0.0
        if volume <= 0:
            if self._vol > 0:
                return round(self._pv / self._vol, 4)
            return None
        tp = (high + low + close) / 3.0
        self._pv += tp * float(volume)
        self._vol += float(volume)
        return round(self._pv / self._vol, 4)


def test_zero_volume_not_weighted() -> None:
    st = _SessionVWAPState()
    v1 = st.update("2026-06-16", 100, 100, 100, 1000)
    assert v1 == 100.0
    v2 = st.update("2026-06-16", 200, 200, 200, 0)
    assert v2 == 100.0, "volume=0 不应改变 VWAP"


def test_session_vwap_skips_zero_vol_bars() -> None:
    bars = [
        {"time": 1, "high": 10, "low": 10, "close": 10, "volume": 100},
        {"time": 2, "high": 20, "low": 20, "close": 20, "volume": 0},
    ]
    assert session_vwap(bars) == 10.0


def test_double_feed_inflates_vwap() -> None:
    """模拟 poll 双喂：同一 session 棒重复 update 会偏离。"""
    st = _SessionVWAPState()
    st.update("2026-06-16", 238, 242, 240, 50000)
    once = st.update("2026-06-16", 239, 241, 240.5, 30000)
    st2 = _SessionVWAPState()
    st2.update("2026-06-16", 238, 242, 240, 50000)
    st2.update("2026-06-16", 239, 241, 240.5, 30000)
    st2.update("2026-06-16", 239, 241, 240.5, 30000)  # duplicate poll
    assert st2.update("2026-06-16", 239, 241, 240.5, 30000) != once


if __name__ == "__main__":
    test_zero_volume_not_weighted()
    test_session_vwap_skips_zero_vol_bars()
    test_double_feed_inflates_vwap()
    print("OK: vwap tests")
