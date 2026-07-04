"""
RiskGate 单元测试 — 验证审核修复（#1/#4/#5/#7）。

无需 nautilus_trader / 真实 Redis：对 risk_gate 仅依赖的少量外部符号打桩，
并用内存版 FakeRedis 验证持久化语义。

运行：python scripts/test_risk_gate.py
"""
import sys
import types
from enum import Enum
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


# ── 打桩：nautilus_trader.model.enums.OrderSide ──────────────────────────
class OrderSide(Enum):
    BUY = 1
    SELL = 2


_nt = types.ModuleType("nautilus_trader")
_nt_model = types.ModuleType("nautilus_trader.model")
_nt_enums = types.ModuleType("nautilus_trader.model.enums")
_nt_enums.OrderSide = OrderSide
sys.modules["nautilus_trader"] = _nt
sys.modules["nautilus_trader.model"] = _nt_model
sys.modules["nautilus_trader.model.enums"] = _nt_enums


# ── 打桩：execution.models（Unit / UnitState）────────────────────────────
class UnitState(Enum):
    PENDING_ENTRY = "pending_entry"
    ACTIVE = "active"
    BREAKEVEN = "breakeven"
    CLOSED = "closed"


class Unit:
    def __init__(self, side, state):
        self.side = side
        self.state = state


_exec = types.ModuleType("execution")
_exec_models = types.ModuleType("execution.models")
_exec_models.Unit = Unit
_exec_models.UnitState = UnitState
sys.modules["execution"] = _exec
sys.modules["execution.models"] = _exec_models


# ── 打桩：signals.base（IntentAction / TradeIntent）──────────────────────
class IntentAction(Enum):
    ENTER = "enter"
    ADD = "add"
    EXIT = "exit"


class TradeIntent:
    def __init__(self, *, profile, symbol, side, ref_price, atr_ref, bar_time,
                 stop_px=None, action=IntentAction.ENTER, meta=None):
        self.profile = profile
        self.symbol = symbol
        self.side = side
        self.ref_price = ref_price
        self.atr_ref = atr_ref
        self.bar_time = bar_time
        self.stop_px = stop_px
        self.action = action
        self.meta = meta or {}


_sig = types.ModuleType("signals")
_sig_base = types.ModuleType("signals.base")
_sig_base.IntentAction = IntentAction
_sig_base.TradeIntent = TradeIntent
sys.modules["signals"] = _sig
sys.modules["signals.base"] = _sig_base


# ── 内存版 FakeRedis（仅实现用到的命令）──────────────────────────────────
class FakeRedis:
    def __init__(self):
        self.kv = {}

    def get(self, k):
        v = self.kv.get(k)
        return None if v is None else str(v).encode()

    def set(self, k, v, ex=None, nx=False):
        if nx and k in self.kv:
            return None
        self.kv[k] = v
        return True

    def incr(self, k):
        self.kv[k] = int(self.kv.get(k, 0)) + 1
        return self.kv[k]

    def expire(self, k, s):
        return True

    def exists(self, k):
        return 1 if k in self.kv else 0

    def delete(self, k):
        self.kv.pop(k, None)


# ── 导入被测对象（桩已就位）─────────────────────────────────────────────
from portfolio.config import PortfolioRiskConfig
from portfolio.risk_gate import RiskGate

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        print(f"  ❌ {name}")


def make_gate(cfg, redis=None, equity=100_000.0, units=None):
    return RiskGate(
        config=cfg,
        redis=redis,
        equity_fn=lambda: equity,
        units_fn=lambda: (units or {}),
        log_fn=lambda m: None,
    )


# ── #4 sizing：结构止损 vs ATR 口径 ──────────────────────────────────────
def test_sizing():
    print("[#4] sizing 按实际止损距离")
    cfg = PortfolioRiskConfig(risk_pct=0.002, max_position_pct=1.0)  # 放开仓位上限
    g = make_gate(cfg, equity=100_000.0)
    # 预算 = 100000*0.002 = 200。止损距离 = |500-497| = 3 → qty = 200//3 = 66
    check("size_by_stop: 200预算/3距离=66股", g.size_by_stop(500.0, 497.0) == 66)
    # ATR 口径：atr=1, mult=1.5 → rps=1.5 → 200//1.5=133
    check("size_by_risk: 200预算/1.5=133股", g.size_by_risk(1.0, 500.0, 1.5) == 133)
    # 关键：止损更宽时 size_by_stop 给出更小仓位（风险对齐）
    check("更宽止损→更小仓位", g.size_by_stop(500.0, 490.0) < g.size_by_stop(500.0, 497.0))
    # 仓位上限生效
    cfg2 = PortfolioRiskConfig(risk_pct=0.5, max_position_pct=0.10)
    g2 = make_gate(cfg2, equity=100_000.0)
    cap = int(100_000 * 0.10 / 500)  # 20
    check("max_position_pct 上限封顶=20", g2.size_by_stop(500.0, 499.0) == cap)


# ── #5 冷却/日内次数持久化 ───────────────────────────────────────────────
def test_persistence():
    print("[#5] 冷却 & 日内次数持久化到 Redis")
    r = FakeRedis()
    cfg = PortfolioRiskConfig(cooldown_bars_after_stop=3, max_trades_per_sym_per_day=3)
    g = make_gate(cfg, redis=r)
    bt = 1_700_000_000  # 任意 ET fake-UTC
    g.set_cooldown("NVDA", bt)
    check("set_cooldown 写入 Redis", r.exists("risk:cooldown:NVDA") == 1)
    # 新实例（模拟重启，内存清零）应能从 Redis 读回冷却
    g2 = make_gate(cfg, redis=r)
    check("重启后仍在冷却(读回 Redis)", g2.in_cooldown("NVDA", bt + 300) is True)
    check("冷却到期后放行", g2.in_cooldown("NVDA", bt + 3 * 300 + 1) is False)

    g.record_daily_trade("AAPL", bt)
    g.record_daily_trade("AAPL", bt)
    g3 = make_gate(cfg, redis=r)  # 重启
    check("重启后日内次数读回=2", g3.daily_trade_count("AAPL", bt) == 2)


# ── #7 相关性/集中度上限 ─────────────────────────────────────────────────
def test_correlation():
    print("[#7] 相关组集中度上限")
    cfg = PortfolioRiskConfig(
        risk_pct=0.002, max_position_pct=1.0,
        max_portfolio_positions=10,
        correlation_groups={"NVDA": "semis", "AMD": "semis", "QQQ": "index"},
        max_per_correlation_group=1,
    )
    # 已持有 NVDA（semis 组）
    units = {"NVDA": [Unit(OrderSide.BUY, UnitState.ACTIVE)]}
    g = make_gate(cfg, redis=None, units=units)
    intent_amd = TradeIntent(profile="pullback", symbol="AMD", side=OrderSide.BUY,
                             ref_price=160.0, atr_ref=1.0, bar_time=1_700_000_000,
                             stop_px=158.0)
    v = g.check_enter(intent_amd, atr_mult=1.5, max_units=2, live=True)
    check("同组(semis)第2个被拒", (not v.allowed) and v.reason == "correlation_group_limit")
    # 不同组放行
    intent_qqq = TradeIntent(profile="pullback", symbol="QQQ", side=OrderSide.BUY,
                             ref_price=500.0, atr_ref=1.0, bar_time=1_700_000_000,
                             stop_px=497.0)
    v2 = g.check_enter(intent_qqq, atr_mult=1.5, max_units=2, live=True)
    check("不同组(index)放行", v2.allowed)


# ── #1 熔断键用 ET 日期 ──────────────────────────────────────────────────
def test_halt_et():
    print("[#1] is_halted 读 ET 日期键")
    from portfolio.sessions import et_session_date
    r = FakeRedis()
    cfg = PortfolioRiskConfig()
    g = make_gate(cfg, redis=r)
    check("无熔断时 is_halted=False", g.is_halted() is False)
    r.set(f"risk:halt:{et_session_date()}", "1")
    check("置 ET 日期键后 is_halted=True", g.is_halted() is True)


if __name__ == "__main__":
    test_sizing()
    test_persistence()
    test_correlation()
    test_halt_et()
    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    sys.exit(1 if FAIL else 0)
