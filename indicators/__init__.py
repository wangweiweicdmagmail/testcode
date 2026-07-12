"""共享指标层。

落实设计原则 #2（指标标准化：引擎内置 > 开源库 > 自写）与 #3（共享指标表）：

- registry.IndicatorRegistry：跨周期策略共享的指标值表（拉模式，进程内对象）
- supertrend.STState：项目唯一 SuperTrend 实现（合并历史 3 份重复）
- states：EMA / DEMA / SessionVWAP / MomentumATR 增量状态机
"""
from indicators.registry import IndicatorRegistry
from indicators.states import (
    DEMAState,
    EMAState,
    MomentumATRState,
    SessionVWAPState,
)
from indicators.supertrend import STState

__all__ = [
    "IndicatorRegistry",
    "STState",
    "EMAState",
    "DEMAState",
    "SessionVWAPState",
    "MomentumATRState",
]
