"""增量指标状态机（设计原则 #2：标准指标优先）。

EMA / DEMA 使用鹦鹉螺内置 ExponentialMovingAverage / DoubleExponentialMovingAverage；
SessionVWAP / MomentumATR 为业务特有复合（引擎与开源库均无），自写合理。
"""
from __future__ import annotations

from typing import Optional

from nautilus_trader.indicators import (
    DoubleExponentialMovingAverage,
    ExponentialMovingAverage,
)


class EMAState:
    """单标的 EMA 在线状态机，使用鹦鹉螺内置 EMA 指标。"""

    def __init__(self, period: int = 21):
        self.period = period
        self._ema = ExponentialMovingAverage(period)

    @property
    def initialized(self) -> bool:
        return self._ema.initialized

    def warmup(self, closes: list[float]) -> None:
        """批量喂入历史 close，完成初始化。"""
        for c in closes:
            self.update(c)

    def update(self, close: float) -> Optional[float]:
        """喂入一根 K 线 close，返回当前 EMA 值（不足 period 时返回 None）。"""
        self._ema.update_raw(close)
        if not self._ema.initialized:
            return None
        return round(self._ema.value, 4)


class DEMAState:
    """DEMA 在线状态机，使用鹦鹉螺内置 DoubleExponentialMovingAverage。

    update(close) 返回当前 DEMA 值（预热期 None）；slope 属性返回相对上一根的
    方向（1 上 / -1 下 / 0 平），供信号引擎判断趋势。
    """

    def __init__(self, period: int = 20):
        self._dema = DoubleExponentialMovingAverage(period)
        self._prev: Optional[float] = None
        self.slope: int = 0

    @property
    def initialized(self) -> bool:
        return self._dema.initialized

    def update(self, close: float) -> Optional[float]:
        self._dema.update_raw(close)
        if not self._dema.initialized:
            return None
        cur = self._dema.value
        if self._prev is not None:
            self.slope = 1 if cur > self._prev else (-1 if cur < self._prev else 0)
        self._prev = cur
        return round(cur, 4)


class SessionVWAPState:
    """Session VWAP = Σ(typical_price × volume) / Σ(volume)，新交易日 reset。"""

    def __init__(self) -> None:
        self._date: Optional[str] = None
        self._pv = 0.0
        self._vol = 0.0

    def reset(self) -> None:
        self._date = None
        self._pv = 0.0
        self._vol = 0.0

    def update(
        self, session_date: str, high: float, low: float, close: float, volume: int,
    ) -> Optional[float]:
        if self._date != session_date:
            self._date = session_date
            self._pv = 0.0
            self._vol = 0.0
        if volume <= 0:
            # IBKR 偶发 volume=0：不纳入累计，避免 v=1 虚假权重拉歪 VWAP
            if self._vol > 0:
                return round(self._pv / self._vol, 4)
            return None
        tp = (high + low + close) / 3.0
        self._pv += tp * float(volume)
        self._vol += float(volume)
        return round(self._pv / self._vol, 4)


class MomentumATRState:
    """M5 归一化动量状态机（业务特有复合指标）。

    每根 M5 bar 收盘时计算：
      mom_atr = (M5_close_now - M5_open_1bar_ago) / M1_ATR_14
    即把 2 根 M5 bar 组合成一根 10 分钟 K 线，取其实体大小，再用 M1 ATR(14)
    归一化。代表过去 10 分钟内相对 ATR 的价格移动幅度。
    """

    def __init__(self):
        # 滑动 M5 bar open 队列，保留最近 2 根（index 0=最旧，1=最新）
        self._opens: list[float] = []

    def update(self, o: float, c: float, m1_atr: Optional[float]) -> Optional[float]:
        """喂入 M5 bar 的 (open, close) 及当前 M1_ATR。

        返回 mom_atr（M1_ATR 有效且累积 ≥2 根 M5 bar 后才输出）。
        """
        self._opens.append(o)
        if len(self._opens) > 2:
            self._opens.pop(0)
        # 预热检查：M1 ATR 有值 + 至少累积 2 根 M5 bar（可取 open_1bar_ago）
        if m1_atr is None or m1_atr == 0 or len(self._opens) < 2:
            return None
        # 10 分钟 K 线实体 = 当前 close − 上一根 M5 bar 的 open
        mom = (c - self._opens[0]) / m1_atr
        return round(mom, 4)
