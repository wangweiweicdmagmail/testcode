"""组合风控与执行参数（与具体 Alpha 无关）。"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PortfolioRiskConfig:
    risk_pct: float = 0.002
    max_position_pct: float = 0.10
    max_portfolio_positions: int = 3
    rth_open_blackout_min: int = 15
    pre_eod_blackout_min: int = 30
    cooldown_bars_after_stop: int = 3
    max_trades_per_sym_per_day: int = 3
    min_qty: int = 1
    fixed_qty: int = 0  # >0 时每次固定股数（测试）；0=以损定量
    # 相关性/集中度约束：同一组（如高 beta 科技股）最多同时持有 N 个，0=不限。
    # correlation_groups: 标的 → 组名映射，例如 {"NVDA":"semis","AMD":"semis"}
    correlation_groups: dict[str, str] = field(default_factory=dict)
    max_per_correlation_group: int = 0


@dataclass(frozen=True)
class ExecutionConfig:
    atr_mult: float = 1.5
    tp_rr: float = 2.0
    max_units: int = 2
    fa_group: str = ""
    fa_method: str = "NetLiq"
    # 对账接管到无止损持仓时，补挂的兜底止损距离（占入场价比例）
    emergency_stop_pct: float = 0.02
