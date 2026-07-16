import time
from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4


class BarCollectedEvent(Event):
    """
    当策略计算完指标后发布的事件，供 ExitManager 等模块监听并执行风险检查。
    """
    def __init__(self, symbol: str, bar_dict: dict):
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        self._id = UUID4()

        self.symbol = symbol
        self.bar = bar_dict  # 包含 OHLC, ST_value, st_dir 等


class BarCollectedM5Event(Event):
    """
    M5 K 线收盘时由 strategy.py 发布，供 ExitManager 执行 M5 ST 跟踪止盈。
    携带 M5 bar 的 st_value / st_dir（参数 10, 3.0）。
    """
    def __init__(self, symbol: str, bar_dict: dict):
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        self._id = UUID4()

        self.symbol = symbol
        self.bar = bar_dict  # 包含 M5 OHLC, st_value, st_dir, instrument_id 等


class BarsHistoryFlushedEvent(Event):
    """strategy 历史 K 线写入 Redis 后发布，供 SignalDetector 回放触线标记。"""
    def __init__(self, symbol: str, session_date: str = ""):
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        self._id = UUID4()

        self.symbol = symbol
        self.session_date = session_date


class AgentExecuteNowEvent(Event):
    """审批通过后立即触发 Agent 执行（不等下一根 M1）。"""
    TOPIC = "agent.execute.now"

    def __init__(self, symbol: str, proposal_id: str = "") -> None:
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        self._id = UUID4()

        self.symbol = str(symbol).upper()
        self.proposal_id = str(proposal_id or "")


class EntryExecuteNowEvent(Event):
    """控制台进场请求立即触发（不等下一根 M1，市价单低延迟进场）。"""
    TOPIC = "entry.execute.now"

    def __init__(self, symbol: str) -> None:
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        self._id = UUID4()

        self.symbol = str(symbol).upper()


# 订单终态集合（不再活跃），全局共用，避免各模块重复定义
TERMINAL_STATUS: frozenset[str] = frozenset({
    "FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"
})
