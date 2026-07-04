#!/usr/bin/env python3
"""超级信号 st_super 单元测试（检测 + 定价）。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from signals.st_super import (
    StSuperSymbolState,
    detect_st_super_flip,
    pricing_from_st_super,
    update_st5_from_m5_bar,
)
from signals.touch_detector import TouchEvent


def _bar(t, o, h, l, c):
    return {"time": t, "open": o, "high": h, "low": l, "close": c}


def test_st_super_flip_long():
    st = StSuperSymbolState.create()
    # warmup 1m ST
    for i in range(15):
        detect_st_super_flip("AAPL", _bar(1000 + i * 60, 100, 101, 99, 100 + i * 0.1), st)
    # warmup 5m bull
    for i in range(15):
        update_st5_from_m5_bar(_bar(2000 + i * 300, 110, 112, 109, 111), st)
    assert st.st5_dir == 1
    st.prev1_dir = -1
    # 09:45 ET（与 RiskGate 入场窗口一致）
    ev = detect_st_super_flip(
        "AAPL",
        _bar(585 * 60, 112, 113, 111, 112.5),
        st,
    )
    assert ev is not None
    assert ev.signal_type == "st_super"
    assert ev.side == "LONG"
    assert ev.trigger_level > 0


def test_pricing():
    ev = TouchEvent(
        symbol="AAPL", signal_type="st_super", side="LONG",
        trigger_level=100.0, touch_time=1, m1_bar_time=1,
        m5_context_bar_time=None, session_date="2026-07-02",
        m1_high=102, m1_low=99, m1_close=101.0,
        reclaim=True, rule_confidence=0.75, rule_thesis="t",
    )
    px = pricing_from_st_super(ev)
    assert px is not None
    assert px["entry_price"] == 101.0
    assert px["stop_price"] == 100.0
    assert px["rr_half_est"] == 2.0


if __name__ == "__main__":
    test_st_super_flip_long()
    test_pricing()
    print("OK: st_super tests passed")
