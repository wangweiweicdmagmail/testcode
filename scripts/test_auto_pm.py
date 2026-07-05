"""
AutoPositionManager 单元测试 — 验证审核修复（#2/#8/#9/#10/#11/#12）。

用 FakeHost/FakeCache 打桩 NautilusTrader，无需引擎。覆盖：
  - reconcile A: broker 已平但本地有单元 → 撤残单并重置（防裸仓反向）
  - reconcile B: broker 有仓本地无单元 → 接管 + 无止损时补兜底止损
  - #9 入场部分成交后被终止 → 平残仓
  - #10 小仓全量止盈成交 → 撤残留止损，单元了结

运行：python scripts/test_auto_pm.py
"""
import sys
import types
from enum import Enum
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


# ── 打桩 nautilus_trader ─────────────────────────────────────────────────
class OrderSide(Enum):
    BUY = 1
    SELL = 2


class TimeInForce(Enum):
    DAY = 1
    GTC = 2


class ClientOrderId:
    def __init__(self, value):
        self.value = value

    def __eq__(self, o):
        return isinstance(o, ClientOrderId) and o.value == self.value

    def __hash__(self):
        return hash(self.value)


class InstrumentId:
    def __init__(self, sym):
        self.sym = sym

    def __eq__(self, o):
        return isinstance(o, InstrumentId) and o.sym == self.sym

    def __hash__(self):
        return hash(self.sym)


def _mod(name):
    m = types.ModuleType(name)
    sys.modules[name] = m
    return m


_mod("nautilus_trader")
_mod("nautilus_trader.model")
_e = _mod("nautilus_trader.model.enums"); _e.OrderSide = OrderSide; _e.TimeInForce = TimeInForce
_c = _mod("nautilus_trader.model.currencies"); _c.USD = "USD"
_i = _mod("nautilus_trader.model.identifiers"); _i.ClientOrderId = ClientOrderId; _i.InstrumentId = InstrumentId
_mod("nautilus_trader.trading"); _ts = _mod("nautilus_trader.trading.strategy"); _ts.Strategy = object
# events 桩（只需 TERMINAL_STATUS）
_ev = _mod("events"); _ev.TERMINAL_STATUS = frozenset({"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"})

from execution.auto_pm import AutoPositionManager  # noqa: E402
from execution.models import Unit, UnitState  # noqa: E402
from portfolio.config import ExecutionConfig  # noqa: E402

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1; print(f"  ✅ {name}")
    else:
        FAIL += 1; print(f"  ❌ {name}")


# ── Fake NautilusTrader 运行时 ───────────────────────────────────────────
class _Named:
    def __init__(self, name): self.name = name


class FakeOrder:
    def __init__(self, coid, side, qty, otype, price=None, trigger=None, sid="AUTO"):
        self.client_order_id = ClientOrderId(coid)
        self.order_side = side
        self.quantity = qty
        self.order_type = _Named(otype)
        self.price = price
        self.trigger_price = trigger
        self.status = _Named("INITIALIZED")
        self.is_open = False
        self.instrument_id = None
        self.strategy_id = sid
        self.filled_qty = 0
        self.avg_px = price or 0.0


class FakeInstrument:
    def make_qty(self, x): return x
    def make_price(self, x): return x


class FakePosition:
    def __init__(self, iid, is_long, avg_px, qty):
        self.instrument_id = iid
        self.is_long = is_long
        self.avg_px_open = avg_px
        self.quantity = qty
        self.realized_pnl = None
    def unrealized_pnl(self, px): return None


class FakeCache:
    def __init__(self): self._orders = {}; self._positions = []
    def add(self, o): self._orders[o.client_order_id.value] = o
    def instrument(self, iid): return FakeInstrument()
    def order(self, coid): return self._orders.get(coid.value)
    def orders(self): return list(self._orders.values())
    def orders_open(self): return [o for o in self._orders.values() if o.is_open]
    def positions_open(self): return list(self._positions)


class FakePortfolio:
    def __init__(self): self.net = {}
    def net_position(self, iid): return self.net.get(iid.sym, 0.0)


class FakeLog:
    def info(self, *a): pass
    def warning(self, *a): pass
    def error(self, *a): pass


class FakeOrderFactory:
    def __init__(self, host): self.host = host; self.n = 0
    def _coid(self):
        self.n += 1; return f"O{self.n}"
    def market(self, *, instrument_id, order_side, quantity, **kw):
        o = FakeOrder(self._coid(), order_side, quantity, "MARKET"); o.instrument_id = instrument_id; return o
    def stop_market(self, *, instrument_id, order_side, quantity, trigger_price, **kw):
        o = FakeOrder(self._coid(), order_side, quantity, "STOP_MARKET", trigger=trigger_price)
        o.instrument_id = instrument_id; return o
    def limit(self, *, instrument_id, order_side, quantity, price, **kw):
        o = FakeOrder(self._coid(), order_side, quantity, "LIMIT", price=price)
        o.instrument_id = instrument_id; return o


class FakeHost:
    def __init__(self):
        self.id = "AUTO"
        self.log = FakeLog()
        self.cache = FakeCache()
        self.portfolio = FakePortfolio()
        self.order_factory = FakeOrderFactory(self)
        self.submitted = []
        self.canceled = []
    def submit_order(self, o):
        o.is_open = True; o.status = _Named("ACCEPTED")
        self.cache.add(o); self.submitted.append(o)
    def cancel_order(self, o):
        o.is_open = False; o.status = _Named("CANCELED"); self.canceled.append(o)
    def modify_order(self, *a, **k): pass


def make_pm(host):
    iid_map = {"NVDA": InstrumentId("NVDA")}
    return AutoPositionManager(
        host=host, exec_cfg=ExecutionConfig(emergency_stop_pct=0.02),
        redis=None, iid_map=iid_map,
        publish=lambda *a: None, on_stop_cooldown=lambda *a: None,
        equity_fn=lambda: 100_000.0,
    )


# ── #12-A: broker 已平但本地有单元 → 撤残单 + 重置 ────────────────────────
def test_reconcile_stale_flat():
    print("[#12-A] broker 已平本地仍持仓 → 撤残单重置")
    h = FakeHost(); pm = make_pm(h)
    # 残留一张挂着的 TP（模拟宕机期止损成交、TP 仍挂）
    tp = FakeOrder("OLD_TP", OrderSide.SELL, 50, "LIMIT", price=110.0)
    tp.instrument_id = InstrumentId("NVDA"); h.submit_order(tp)
    pm._units["NVDA"] = [Unit(sym="NVDA", seq=0, side=OrderSide.BUY,
                              state=UnitState.ACTIVE, qty=100, entry_px=100.0,
                              stop_coid="OLD_TP")]
    h.portfolio.net["NVDA"] = 0.0
    res = pm.reconcile("NVDA")
    check("返回 engine_stale_flat", res == "engine_stale_flat")
    check("残留 TP 已撤", tp.is_open is False)
    check("本地单元清空", all(u.state == UnitState.CLOSED for u in pm._units["NVDA"]))


# ── #12-B: broker 有仓本地无单元（有 PM 止损）→ 接管，不重复挂止损 ────────
def test_reconcile_adopt_with_stop():
    print("[#12-B1] broker 有仓本地无单元(有止损) → 接管不重复挂")
    h = FakeHost(); pm = make_pm(h)
    stop = FakeOrder("ST1", OrderSide.SELL, 100, "STOP_MARKET", trigger=97.0)
    stop.instrument_id = InstrumentId("NVDA"); h.submit_order(stop)
    h.cache._positions = [FakePosition(InstrumentId("NVDA"), True, 100.0, 100)]
    h.portfolio.net["NVDA"] = 100.0
    n_before = len(h.submitted)
    res = pm.reconcile("NVDA")
    check("返回 engine_adopted_position", res == "engine_adopted_position")
    check("接管出 1 个单元", len([u for u in pm._units["NVDA"] if u.state != UnitState.CLOSED]) == 1)
    check("未重复挂止损", len(h.submitted) == n_before)


# ── #12-B2: broker 有仓本地无单元且无止损 → 补兜底止损 ────────────────────
def test_reconcile_adopt_naked():
    print("[#12-B2] broker 有仓且无止损 → 补兜底止损")
    h = FakeHost(); pm = make_pm(h)
    # 仅有一张已成交的入场单（PM 拥有），无止损
    entry = FakeOrder("EN1", OrderSide.BUY, 100, "MARKET"); entry.instrument_id = InstrumentId("NVDA")
    entry.status = _Named("FILLED"); h.cache.add(entry)
    h.cache._positions = [FakePosition(InstrumentId("NVDA"), True, 100.0, 100)]
    h.portfolio.net["NVDA"] = 100.0
    res = pm.reconcile("NVDA")
    check("返回 engine_adopted_position", res == "engine_adopted_position")
    new_stops = [o for o in h.submitted if o.order_type.name == "STOP_MARKET"]
    check("补挂了 1 张兜底止损", len(new_stops) == 1)
    check("兜底止损价 = 100×(1-0.02)=98", new_stops and float(new_stops[0].trigger_price) == 98.0)


# ── #9: 入场部分成交后被终止 → 平残仓 ────────────────────────────────────
def test_partial_entry_terminal():
    print("[#9] 入场部分成交后被取消 → 平残仓")
    h = FakeHost(); pm = make_pm(h)
    pm.set_last_bar("NVDA", 100.0, 0)
    entry = FakeOrder("EN9", OrderSide.BUY, 100, "MARKET"); entry.instrument_id = InstrumentId("NVDA")
    h.cache.add(entry)
    u = Unit(sym="NVDA", seq=0, side=OrderSide.BUY, state=UnitState.PENDING_ENTRY,
             qty=100, entry_coid="EN9", entry_filled=40)
    pm._units["NVDA"] = [u]; pm._coid_index["EN9"] = ("NVDA", "entry")
    h.portfolio.net["NVDA"] = 40.0  # 已成交 40 股
    ev = types.SimpleNamespace(client_order_id=ClientOrderId("EN9"))
    pm.on_order_terminal(ev, "CANCELED")
    closes = [o for o in h.submitted if o.order_side == OrderSide.SELL]
    check("平残仓单已提交(SELL 40)", len(closes) == 1 and int(closes[0].quantity) == 40)
    check("单元 PENDING_CLOSE（等成交确认）", u.state == UnitState.PENDING_CLOSE)
    close_coid = closes[0].client_order_id.value
    pm.on_order_filled(types.SimpleNamespace(
        client_order_id=ClientOrderId(close_coid), last_qty=40, last_px=100.0))
    check("平仓成交后单元关闭", u.state == UnitState.CLOSED)


# ── #10: 小仓全量止盈成交 → 撤残留止损，单元了结 ──────────────────────────
def test_full_tp_cancels_stop():
    print("[#10] 全量止盈成交 → 撤残留止损")
    h = FakeHost(); pm = make_pm(h)
    stop = FakeOrder("ST10", OrderSide.SELL, 1, "STOP_MARKET", trigger=98.0)
    stop.instrument_id = InstrumentId("NVDA"); h.submit_order(stop)
    tp = FakeOrder("TP10", OrderSide.SELL, 1, "LIMIT", price=104.0)
    tp.instrument_id = InstrumentId("NVDA"); h.submit_order(tp)
    u = Unit(sym="NVDA", seq=0, side=OrderSide.BUY, state=UnitState.ACTIVE,
             qty=1, entry_px=100.0, stop_coid="ST10", tp_coid="TP10", tp_filled=1)
    pm._units["NVDA"] = [u]
    pm._on_tp_complete("NVDA", u)
    check("残留止损已撤", stop.is_open is False)
    check("单元已关闭", u.state == UnitState.CLOSED)


if __name__ == "__main__":
    test_reconcile_stale_flat()
    test_reconcile_adopt_with_stop()
    test_reconcile_adopt_naked()
    test_partial_entry_terminal()
    test_full_tp_cancels_stop()
    print(f"\n结果: {PASS} 通过 / {FAIL} 失败")
    sys.exit(1 if FAIL else 0)
