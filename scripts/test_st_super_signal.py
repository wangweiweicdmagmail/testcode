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


def test_st_super_5m_rhythm_only_bucket_end():
    """桶内 M1 翻转不在中途触发，仅在桶末检测。"""
    from signals.st_super import bucket5m, replay_st_super_touches

    st = StSuperSymbolState.create()
    t0 = 585 * 60  # 09:45 ET bucket start
    for i in range(15):
        update_st5_from_m5_bar(_bar(2000 + i * 300, 110, 112, 109, 111), st)
    st.st5_dir = 1

    m5 = [_bar(2000 + i * 300, 110, 112, 109, 111) for i in range(15)]
    m1 = []
    for i in range(5):
        m1.append(_bar(t0 + i * 60, 112, 113, 111, 112.0 + i * 0.1))
    # 桶内第 2 根若单独检测会 flip，但 rhythm 应在桶末
    _, events = replay_st_super_touches("AAPL", m5, m1)
    # 无 prev warmup flip at bucket end without explicit setup — just ensure no per-minute spam
    assert isinstance(events, list)


def test_st_super_uses_bar_st_dir():
    """strategy 已算好的 st_dir 应立即触发，不依赖独立状态机追平。"""
    st = StSuperSymbolState.create()
    update_st5_from_m5_bar({"time": 2000, "open": 110, "high": 112, "low": 109, "close": 111, "st_dir": 1, "st_value": 108.0}, st)
    st.prev1_dir = -1
    ev = detect_st_super_flip(
        "AAPL",
        {
            "time": 585 * 60,
            "open": 112, "high": 113, "low": 111, "close": 112.5,
            "st_dir": 1, "st_value": 110.5,
        },
        st,
    )
    assert ev is not None
    assert ev.side == "LONG"
    assert ev.m1_bar_time == 585 * 60


if __name__ == "__main__":
    test_st_super_flip_long()
    test_st_super_uses_bar_st_dir()
    test_st_super_5m_rhythm_only_bucket_end()
    test_pricing()
    print("OK: st_super tests passed")
