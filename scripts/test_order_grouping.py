"""定势订单分组单元测试（照 scripts/test_trend_state.js 范式）。

不连 IBKR / 不真下单：用 mock IBOrder + 内存 redis 验证纯逻辑——
coid 生成唯一性、position_key 解析、跨子账户聚合、手动单(clientId=0)区分、permId 补录。

运行：python scripts/test_order_grouping.py
"""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

from portfolio.fa_account import (
    query_all_open_orders_grouped,
    _position_key_from_coid,
)
from execution.auto_pm import AutoPositionManager  # noqa: F401  (仅触发 import 拿方法)


class _FakeRedis:
    """dict-backed redis 子集（get/set/setex）。"""

    def __init__(self) -> None:
        self.d: dict[str, str] = {}

    def get(self, k: str) -> Any:
        return self.d.get(k)

    def set(self, k: str, v: str) -> None:
        self.d[k] = v

    def setex(self, k: str, ttl: int, v: str) -> None:
        self.d[k] = v


def _iborder(
    *, order_ref="", perm_id=0, client_id=0, account="", fa_group="",
    symbol="", action="BUY", qty=0.0, order_type="MKT", lmt_price=None,
    aux_price=None, status="Submitted", order_id=0,
) -> SimpleNamespace:
    contract = SimpleNamespace(symbol=symbol)
    state = SimpleNamespace(status=status)
    return SimpleNamespace(
        orderRef=order_ref, permId=perm_id, clientId=client_id, account=account,
        faGroup=fa_group, contract=contract, action=action, totalQuantity=qty,
        orderType=order_type, lmtPrice=lmt_price, auxPrice=aux_price,
        order_state=state, orderId=order_id,
    )


class _FakeIBClient:
    def __init__(self, orders: list) -> None:
        self._orders = orders

    async def query_all_open_orders(self, timeout: int = 20) -> list:
        return list(self._orders)


_passed = 0


def _ok(name: str, cond: bool) -> None:
    if not cond:
        raise AssertionError(f"FAIL: {name}")
    print(f"  ✓ {name}")
    global _passed
    _passed += 1


def test_position_key_parse() -> None:
    _ok("coid 前缀解析: AAPL3-E-1 → AAPL3", _position_key_from_coid("AAPL3-E-1") == "AAPL3")
    _ok("coid 前缀解析: PROPABC-S-7 → PROPABC", _position_key_from_coid("PROPABC-S-7") == "PROPABC")
    _ok("CLOSE 单解析: CLOSEAAPL-C-2 → CLOSEAAPL", _position_key_from_coid("CLOSEAAPL-C-2") == "CLOSEAAPL")
    _ok("非引擎格式 coid → 空", _position_key_from_coid("O-20260716-1") == "")
    _ok("空 coid → 空", _position_key_from_coid("") == "")


def test_gen_coid_unique() -> None:
    # 直接验 _gen_coid 的格式 + 唯一性（构造一个最小 manager，仅用计数器字段）
    mgr = AutoPositionManager.__new__(AutoPositionManager)
    mgr._coid_counter = 0
    coids = [mgr._gen_coid("AAPL3", "E").value for _ in range(5)]
    _ok("coid 格式: {poskey}-{leg}-{n}", all(c.startswith("AAPL3-E-") for c in coids))
    _ok("coid 唯一（5 个互不相同）", len(set(coids)) == 5)

    # position_key sanitize：proposal_id 含连字符/小写 → 去掉并大写
    pk = mgr._position_key_for("prop-abc_123", "AAPL", 3)
    _ok("position_key sanitize: prop-abc_123 → PROPABC123", pk == "PROPABC123")
    pk2 = mgr._position_key_for("", "AAPL", 3)
    _ok("position_key fallback: 空 proposal → SYM+seq+date", pk2.startswith("AAPL3"))


def test_grouping_cross_account() -> None:
    """同一仓位 entry/stop/tp 散落在 2 个子账户 → 聚合成一组，净量正确。"""
    redis = _FakeRedis()
    # 预置 order:meta（模拟 auto_pm 提交时写入）
    redis.set("order:meta:AAPL3-E-1", json.dumps({"position_key": "AAPL3", "proposal_id": "P1", "symbol": "AAPL"}))

    orders = [
        # entry 多头 100 股，在 U1 子账户
        _iborder(order_ref="AAPL3-E-1", perm_id=1001, client_id=3, account="U1",
                 symbol="AAPL", action="BUY", qty=100, order_type="MKT", status="Filled"),
        # stop 在 U2 子账户
        _iborder(order_ref="AAPL3-S-2", perm_id=1002, client_id=3, account="U2",
                 symbol="AAPL", action="SELL", qty=100, order_type="STP", aux_price=180.0,
                 status="Submitted"),
        # tp 在 U1
        _iborder(order_ref="AAPL3-T-3", perm_id=1003, client_id=3, account="U1",
                 symbol="AAPL", action="SELL", qty=50, order_type="LMT", lmt_price=200.0,
                 status="Submitted"),
        # TWS 手动单
        _iborder(order_ref="", perm_id=2001, client_id=0, account="U1",
                 symbol="MSFT", action="BUY", qty=10, order_type="LMT", lmt_price=400.0,
                 status="Submitted"),
    ]
    out = asyncio.run(query_all_open_orders_grouped(_FakeIBClient(orders), redis))

    _ok("引擎单聚成 1 组", len(out["engine"]) == 1)
    grp = out["engine"][0]
    _ok("position_key = AAPL3", grp["position_key"] == "AAPL3")
    _ok("proposal_id 透传 = P1", grp["proposal_id"] == "P1")
    _ok("3 条腿聚合", len(grp["legs"]) == 3)
    _ok("跨 2 个子账户", sorted(grp["accounts"]) == ["U1", "U2"])
    _ok("净量 = entry 100（多）", grp["net_qty"] == 100)
    _ok("腿排序 E→S→T", [lg["leg"] for lg in grp["legs"]] == ["E", "S", "T"])
    _ok("手动单进 manual 桶 1 条", len(out["manual"]) == 1)
    _ok("other_api 桶为空", out["other_api"] == [])

    # permId 补录（entry 腿 coid 已有 meta → 合并 perm_id；stop/tp 无 meta → 新建）
    _ok("order:meta:AAPL3-E-1 补 permId=1001",
        json.loads(redis.get("order:meta:AAPL3-E-1"))["perm_id"] == 1001)
    _ok("order:meta:AAPL3-S-2 新建含 permId=1002",
        json.loads(redis.get("order:meta:AAPL3-S-2"))["perm_id"] == 1002)
    _ok("反向 order:permid:1003 → AAPL3-T-3",
        redis.get("order:permid:1003") == "AAPL3-T-3")


def test_other_api_bucket() -> None:
    """非零 clientId 但 coid 非 engine 格式 → other_api 桶。"""
    redis = _FakeRedis()
    orders = [
        _iborder(order_ref="O-20260716-1", perm_id=9001, client_id=7, account="U1",
                 symbol="TSLA", action="BUY", qty=5, order_type="MKT"),
    ]
    out = asyncio.run(query_all_open_orders_grouped(_FakeIBClient(orders), redis))
    _ok("其他 API client 单进 other_api", len(out["other_api"]) == 1)
    _ok("engine 桶为空", out["engine"] == [])


def main() -> None:
    print("定势订单分组单元测试")
    test_position_key_parse()
    test_gen_coid_unique()
    test_grouping_cross_account()
    test_other_api_bucket()
    print(f"\n✅ 全部通过（{_passed} 项断言）")


if __name__ == "__main__":
    main()
