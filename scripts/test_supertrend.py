"""indicators 共享层单元测试（设计原则 #2 / #3 验证）。

验证合并后的 SuperTrend 状态机、各增量状态机与 IndicatorRegistry 行为正确。
运行：python scripts/test_supertrend.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# 确保能 import 项目根的 indicators 包
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from indicators.registry import IndicatorRegistry
from indicators.states import DEMAState, EMAState, MomentumATRState, SessionVWAPState
from indicators.supertrend import STState


def _bar(c: float, spread: float = 1.0):
    """close → (o, h, l, c)，h/l 围绕 close 各 spread/2。"""
    return c, c + spread / 2, c - spread / 2, c


def test_st_prewarm_returns_zero():
    """ATR 未初始化前，update 返回 (0,0,0,0)，方向 0=无方向（不输出假信号）。"""
    st = STState(10, 3.5)
    for _ in range(9):
        r = st.update(*_bar(100))
        assert r == (0.0, 0, 0.0, 0.0), f"预热期应返回占位值，got {r}"
    print("  ✓ 预热期返回 (0,0,0,0)，方向 0")


def test_st_initializes_after_warmup():
    st = STState(10, 3.5)
    for _ in range(15):
        st.update(*_bar(100))
    assert st.initialized, "喂 15 根后应 initialized"
    print("  ✓ 喂 15 根后 initialized=True")


def test_st_returns_four_values():
    """合并后 update 返回 4 值 (st_val, st_dir, upper, lower)。"""
    st = STState(10, 3.5)
    for _ in range(15):
        st.update(*_bar(100))
    r = st.update(*_bar(100))
    assert len(r) == 4, f"应返回 4 值，got {r}"
    st_val, st_dir, upper, lower = r
    assert upper > lower, f"upper 应大于 lower: {r}"
    print(f"  ✓ 返回 4 值：st={st_val} dir={st_dir} upper={upper} lower={lower}")


def test_st_flip_bull_and_bear():
    """预热后默认多头；剧烈下跌翻空，剧烈上涨翻回多头。

    注意：预热完成的第一根因 flip_upper 初值=0 会默认翻多（dir=1），
    这是历史 strategy._STState 的既有行为，合并后原样保留（不做对比/不改逻辑）。
    """
    st = STState(10, 3.5)
    for _ in range(20):
        st.update(*_bar(100))          # 预热 → 默认多头
    assert st._prev_dir == 1, f"预热后默认多头，got {st._prev_dir}"
    r = st.update(*_bar(50))           # 剧烈下跌，跌破 lower band → 翻空
    assert r[1] == -1, f"跌破应翻空 dir=-1，got {r}"
    r = st.update(*_bar(200))          # 剧烈上涨，突破 upper band → 翻多
    assert r[1] == 1, f"突破应翻多 dir=1，got {r}"
    print("  ✓ 多→空→多 翻转正确（预热后默认多头，保留历史行为）")


def test_registry_crud():
    reg = IndicatorRegistry()
    assert reg.get("NVDA", "m1", "st_dir") is None
    reg.set("NVDA", "m1", "st_dir", 1)
    assert reg.get("NVDA", "m1", "st_dir") == 1
    reg.set_many("NVDA", "m5", {"st_dir": -1, "st_value": 100.5})
    all_m5 = reg.get_all("NVDA", "m5")
    assert all_m5 == {"st_dir": -1, "st_value": 100.5}
    assert reg.get_all("AAPL", "m1") == {}
    assert "m1" in reg.timeframes("NVDA")
    # get_all 返回浅拷贝，外部修改不影响内部
    all_m5["st_dir"] = 999
    assert reg.get("NVDA", "m5", "st_dir") == -1
    print("  ✓ registry set/get/get_all/timeframes + 浅拷贝隔离 正确")


def test_states_basic():
    ema = EMAState(5)
    assert ema.update(100) is None       # 预热期 None
    for c in [100, 101, 102, 103, 104]:
        ema.update(c)
    assert ema.initialized and ema.update(105) is not None

    dema = DEMAState(5)
    for c in [100, 101, 102, 103, 104]:
        dema.update(c)
    v = dema.update(105)
    assert v is not None and dema.slope in (1, -1, 0), f"DEMA {v} slope {dema.slope}"

    vwap = SessionVWAPState()
    assert vwap.update("2026-07-11", 101, 99, 100, 1000) == 100.0  # tp=100

    mom = MomentumATRState()
    assert mom.update(100, 101, 0.5) is None      # 仅 1 根
    assert mom.update(102, 103, 0.5) is not None  # 2 根 + ATR 有效
    print("  ✓ EMA/DEMA/SessionVWAP/Momentum 基本行为正确")


def main():
    tests = [
        test_st_prewarm_returns_zero,
        test_st_initializes_after_warmup,
        test_st_returns_four_values,
        test_st_flip_bull_and_bear,
        test_registry_crud,
        test_states_basic,
    ]
    for t in tests:
        print(f"[RUN] {t.__name__}")
        t()
    print(f"\n✅ 全部 {len(tests)} 个测试通过")


if __name__ == "__main__":
    main()
