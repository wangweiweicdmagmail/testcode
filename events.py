import time
from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4

class BarCollectedEvent(Event):
    """
    当策略计算完指标后发布的事件，供 ExitManager 等模块监听并执行风险检查。
    """
    def __init__(self, symbol: str, bar_dict: dict):
        # Cython Event 子类需手动初始化私有属性
        self._id = UUID4()
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        
        self.symbol = symbol
        self.bar = bar_dict  # 包含 OHLC, ST_value, st_dir 等

class STTrailSettingsEvent(Event):
    """
    设置变更事件，用于同步 UI 的开关状态到引擎内部策略中。
    """
    def __init__(self, symbol: str, active: bool):
        self._id = UUID4()
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        
        self.symbol = symbol
        self.active = active
