"""SuperTrend 增量状态机（项目唯一实现，设计原则 #2）。

取代历史上三份重复实现：
  - strategy._STState
  - signals.st_super.STState
  - signals.m5_st_audit.STReplay（及其手写 _WilderATR —— 原则 #2 的明确违背项）

ATR 使用鹦鹉螺内置 AverageTrueRange(WILDER)；转向规则与 TradingView
ta.supertrend 对齐——收盘价 vs 上一根 K 线的 band（非当根收紧后的 band）。

update(o, h, lo, c) 返回 (st_val, st_dir, upper_b, lower_b)：
  - 预热期（ATR 未初始化）返回 (0.0, 0, 0.0, 0.0)，方向 0 = 无方向（不输出假信号）
  - 方向约定：1 = 多头（ST 在下轨），-1 = 空头（ST 在上轨）
"""
from __future__ import annotations

from nautilus_trader.indicators import AverageTrueRange
from nautilus_trader.indicators.averages import MovingAverageType


class STState:
    """SuperTrend 在线状态机（与 TradingView ta.supertrend 转向规则对齐）。"""

    def __init__(self, period: int = 10, mult: float = 3.5):
        self.period = period
        self.mult = mult
        # 鹦鹉螺内置 ATR，Wilder 平滑
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close: float = 0.0
        self._prev_upper_b: float = 0.0
        self._prev_lower_b: float = 0.0
        self._prev_st_val: float = 0.0
        # TradingView 初始 direction=1 → ST=upperBand（空头）；约定 -1=空头，与 TV 一致
        self._prev_dir: int = -1
        self._initialized: bool = False

    @property
    def initialized(self) -> bool:
        return self._initialized

    def update(self, o: float, h: float, lo: float, c: float
               ) -> tuple[float, int, float, float]:
        """输入一根 K 线，返回 (st_val, st_dir, upper_b, lower_b)。"""
        self._atr.update_raw(h, lo, c)

        if not self._atr.initialized:
            # ATR 预热未完成：暂返回占位值，方向 0=无方向（不输出假信号）
            self._prev_close = c
            return 0.0, 0, 0.0, 0.0

        atr = self._atr.value
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr

        # 保存上一根 ST 线（转向判断必须用这根，不能用当根收紧后的 band）
        flip_upper = self._prev_upper_b
        flip_lower = self._prev_lower_b

        if not self._initialized:
            # 第一次 ATR 有效：初始化 band
            upper_b = basic_upper
            lower_b = basic_lower
            self._initialized = True
        else:
            prev_upper = self._prev_upper_b
            prev_lower = self._prev_lower_b
            prev_close = self._prev_close
            # 与 TradingView 一致的 band 收紧逻辑
            upper_b = basic_upper if (
                basic_upper < prev_upper or prev_close > prev_upper
            ) else prev_upper
            lower_b = basic_lower if (
                basic_lower > prev_lower or prev_close < prev_lower
            ) else prev_lower

        # 转向：收盘价 vs 上一根 K 线的 band
        # 多头时 ST 在下轨 → 翻空须 close < 上一根 lower band
        # 空头时 ST 在上轨 → 翻多须 close > 上一根 upper band
        if self._prev_dir == -1 and c > flip_upper:
            st_dir = 1
        elif self._prev_dir == 1 and c < flip_lower:
            st_dir = -1
        else:
            st_dir = self._prev_dir

        st_val = lower_b if st_dir == 1 else upper_b

        self._prev_close = c
        self._prev_upper_b = upper_b
        self._prev_lower_b = lower_b
        self._prev_st_val = st_val
        self._prev_dir = st_dir
        return round(st_val, 4), st_dir, round(upper_b, 4), round(lower_b, 4)
