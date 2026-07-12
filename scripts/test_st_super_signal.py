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
    # warmup 5m 多头（M5 bar 带 st_dir=1，从 bar_dict 读——指标策略已算好写入）
    for i in range(15):
        update_st5_from_m5_bar(
            {"time": 2000 + i * 300, "open": 110, "high": 112, "low": 109, "close": 111, "st_dir": 1},
            st,
        )
    assert st.st5_dir == 1
    st.prev1_dir = -1
    # 09:45 ET（与 RiskGate 入场窗口一致），M1 翻多（bar 带 st_dir/st_value）
    ev = detect_st_super_flip(
        "AAPL",
        {"time": 585 * 60, "open": 112, "high": 113, "low": 111, "close": 112.5,
         "st_dir": 1, "st_value": 110.0},
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


def test_st_super_m1_rhythm_catches_intra_bucket_flip():
    """M1 节奏：桶内 1m ST 翻转也能被检测（不再只看 M5 桶末）。"""
    from signals.st_super import replay_st_super_touches

    # m5 在 RTH 内（09:30+）且附 st_dir=1 → replay warmup 后内部 state.st5_dir=1（多头）
    m5 = [_bar(34200 + i * 300, 110, 112, 109, 111) for i in range(15)]
    for b in m5:
        b["st_dir"] = 1

    t0 = 585 * 60  # 09:45 ET
    m1 = []
    for i in range(10):
        m1.append(_bar(t0 + i * 60, 112, 113, 111, 112.0 + i * 0.1))
    # 前几根 1m 空头，第 5 根翻多（桶内翻转），附 st_dir/st_value 模拟 strategy 写入
    for i, b in enumerate(m1):
        if i < 5:
            b["st_dir"], b["st_value"] = -1, 110.0
        else:
            b["st_dir"], b["st_value"] = 1, 111.0

    _, events = replay_st_super_touches("AAPL", m5, m1)
    flips = [e for e in events if e.side == "LONG"]
    assert len(flips) >= 1, f"M1 节奏应捕捉桶内翻转，got {len(events)} events"


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


def test_detect_updates_prev1_dir_each_call():
    """每根 M1 调用都更新 prev1_dir：prev=-1 → st_dir=1（与5m多头同向）触发 LONG。"""
    st = StSuperSymbolState.create()
    st.st5_dir = 1
    st.prev1_dir = -1  # 预设上一根空头
    b = {"time": 585 * 60, "open": 100, "high": 101, "low": 99, "close": 100,
         "st_dir": 1, "st_value": 98.0}
    ev = detect_st_super_flip("AAPL", b, st)
    assert ev is not None and ev.side == "LONG"
    assert st.prev1_dir == 1


def test_signal_detector_reads_registry():
    """signal_detector 从共享 registry 读 M5 st_dir（取代 _on_m5_bar 自维护）触发 st_super。

    验证 Phase 2 的数据源切换：M5 st_dir 由 M5 策略写入 registry，signal_detector
    _on_m1_bar 时读出同步到 state.st5_dir，再走翻转检测。
    """
    from indicators import IndicatorRegistry

    sym = "AAPL"
    reg = IndicatorRegistry()
    reg.set(sym, "m5", "st_dir", 1)   # M5 多头（M5 策略职责：_publish_m5_snapshot_to_registry）

    st = StSuperSymbolState.create()
    # 模拟 signal_detector._on_m1_bar：从 registry 读 M5 st_dir 同步到 state
    snap_m5 = reg.get_all(sym, "m5")
    if snap_m5.get("st_dir") in (1, -1):
        st.st5_dir = int(snap_m5["st_dir"])
    assert st.st5_dir == 1
    st.prev1_dir = -1
    ev = detect_st_super_flip(sym, {"time": 585 * 60, "open": 112, "high": 113, "low": 111,
                                     "close": 112.5, "st_dir": 1, "st_value": 110.0}, st)
    assert ev is not None and ev.side == "LONG"

    # registry 缺 M5 → st5_dir=0 → 同向过滤失败，detect 不触发
    st2 = StSuperSymbolState.create()
    st2.prev1_dir = -1
    ev2 = detect_st_super_flip(sym, {"time": 585 * 60, "open": 112, "high": 113, "low": 111,
                                      "close": 112.5, "st_dir": 1, "st_value": 110.0}, st2)
    assert ev2 is None


if __name__ == "__main__":
    test_st_super_flip_long()
    test_st_super_uses_bar_st_dir()
    test_st_super_m1_rhythm_catches_intra_bucket_flip()
    test_detect_updates_prev1_dir_each_call()
    test_signal_detector_reads_registry()
    test_pricing()
    print("OK: st_super tests passed")
