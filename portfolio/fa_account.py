"""FA Group 操作封装层（设计原则 #1：ibapi 直连仅集中在 portfolio/fa_*.py）。

把 FA Group 余额查询 + FA 校验从"业务层建临时 ibapi 连接"(原 order_actor._query_fa_group_once
每 3 分钟建断 aux client / fa_validate.py 启动建断)改为**复用 NautilusTrader 引擎已建立的
InteractiveBrokersClient 连接**——通过运行时 monkey-patch:

  1. `_patch_client_for_fa`:给 InteractiveBrokersClient 挂两个一次性 async 查询方法
     (query_fa_group_summary / request_fa_groups_xml),复用 self._eclient + self._requests
     请求/响应框架(仿 account.py:get_positions 模式),reqId 用 90000+ 段隔离引擎 10000+。
  2. `_patch_wrapper_for_fa`:patch wrapper 的 accountSummary / accountSummaryEnd / receiveFA
     回调(原 accountSummaryEnd / receiveFA 是空壳,future 永挂),保留原逻辑 + 按 reqId 路由 FA 响应。

不修改 site-packages 源文件(patch 在本项目运行时执行,同 main.py:_FAIBOrderTags)。
业务层(order_actor)经 main.py 注入引擎 client,调本模块对外 API,不再 import ibapi。
"""
from __future__ import annotations

import json
import time
import xml.etree.ElementTree as ET

from ibapi.common import FaDataTypeEnum
from nautilus_trader.adapters.interactive_brokers.client import wrapper as _wrapper_mod
from nautilus_trader.adapters.interactive_brokers.client.client import InteractiveBrokersClient

# FA 专用 reqId 段，避开引擎 _request_id_seq(10000+, client.py:165)
_FA_REQID_BASE = 90000
FA_GROUPS_TYPE = int(FaDataTypeEnum.GROUPS)  # == 1（修复 fa_validate.py:9 写 2 的 bug）

_PATCHED_CLIENT = False
_PATCHED_WRAPPER = False


# ── 1. 给 InteractiveBrokersClient 挂一次性 FA 查询方法 ────────────────
def _patch_client_for_fa() -> None:
    """幂等：给 InteractiveBrokersClient 挂一次性查询方法。"""
    global _PATCHED_CLIENT
    if _PATCHED_CLIENT:
        return
    _PATCHED_CLIENT = True
    print("[FA-PATCH] ✓ _patch_client_for_fa: 挂 InteractiveBrokersClient.query_fa_group_summary / "
          "request_fa_groups_xml / query_all_positions / query_all_open_orders（FA 余额/校验 + "
          "全账户持仓 + 全账户活跃单分组；只读查询，不改下单路径）", flush=True)

    async def query_fa_group_summary(
        self: InteractiveBrokersClient, fa_group: str, timeout: int = 20,
    ) -> dict[str, dict[str, float]]:
        """一次性 reqAccountSummary(group=fa_group)。

        返回 {account_id: {"NetLiquidation": float, "AvailableFunds": float}}（USD）。
        """
        await self.wait_until_ready(timeout=30)
        req_id = _FA_REQID_BASE + (abs(hash(fa_group)) % 1000)
        name = f"faGroupSummary-{fa_group}"
        self._requests.remove(name=name)   # 清理同名残留
        request = self._requests.add(
            req_id=req_id,
            name=name,
            handle=lambda: self._eclient.reqAccountSummary(
                req_id, fa_group, "NetLiquidation,AvailableFunds"),
            cancel=lambda: self._eclient.cancelAccountSummary(req_id),
        )
        if request is None:
            return {}
        try:
            request.handle()
            results = await self._await_request(request, timeout, default_value=[])
        finally:
            try:
                self._eclient.cancelAccountSummary(req_id)
            except Exception:
                pass
            self._requests.remove(req_id=req_id)

        sub_accounts: dict[str, dict[str, float]] = {}
        for row in results or []:
            # row = (account, tag, value, currency)，由 patch 后的 wrapper.accountSummary append
            try:
                account, tag, value, currency = row
            except (TypeError, ValueError):
                continue
            if currency != "USD" or tag not in ("NetLiquidation", "AvailableFunds"):
                continue
            try:
                sub_accounts.setdefault(str(account), {})[tag] = float(value)
            except ValueError:
                pass
        return sub_accounts

    async def request_fa_groups_xml(self: InteractiveBrokersClient, timeout: int = 20) -> str:
        """一次性 requestFA(GROUPS)，返回 XML 字符串（空串=失败/超时）。"""
        await self.wait_until_ready(timeout=30)
        req_id = _FA_REQID_BASE + 500
        name = "faGroupsXML"
        self._requests.remove(name=name)
        request = self._requests.add(
            req_id=req_id,
            name=name,
            handle=lambda: self._eclient.requestFA(FA_GROUPS_TYPE),
        )
        if request is None:
            return ""
        try:
            request.handle()
            results = await self._await_request(request, timeout, default_value=[])
        finally:
            self._requests.remove(req_id=req_id)
        if results:
            return str(results[0]) if results[0] else ""
        return ""

    async def query_all_positions(self: InteractiveBrokersClient, timeout: int = 20) -> list:
        """一次性 reqPositions()，返回 [(account, symbol, qty, avg_cost), ...] 全账户持仓快照。

        账户无关：覆盖 FA Group 所有子账户，绕过 Nautilus Position 缓存对子账户的盲区
        （FA 分配后主账号 net=0，cache.positions_open() 会丢，但 wrapper.position 收得到全部）。
        每条 (account, contract.symbol, position, avgCost) 由 patch 后的 wrapper.position 喂给本 request.result。
        """
        await self.wait_until_ready(timeout=30)
        req_id = _FA_REQID_BASE + 600
        name = "AllPositions"
        self._requests.remove(name=name)   # 清理同名残留
        request = self._requests.add(
            req_id=req_id,
            name=name,
            handle=lambda: self._eclient.reqPositions(),
            cancel=lambda: self._eclient.cancelPositions(),
        )
        if request is None:
            return []
        try:
            request.handle()
            results = await self._await_request(request, timeout, default_value=[])
        finally:
            try:
                self._eclient.cancelPositions()
            except Exception:
                pass
            self._requests.remove(req_id=req_id)
        return list(results or [])

    async def query_all_open_orders(self: InteractiveBrokersClient, timeout: int = 20) -> list:
        """一次性 reqAllOpenOrders()，返回全部活跃单原样列表（IBOrder），不过滤。

        覆盖：TWS GUI 手动单(clientId=0)、所有 API client、全 FA 子账户。
        每条 IBOrder 含 orderRef(=coid，已剥 :orderId)/clientId/permId/account/faGroup/
        action/orderType/totalQuantity/lmtPrice/auxPrice/orderId/order_state.status。

        与适配器 get_open_orders(account_id) 区别：后者按 account 过滤且吃掉子账户单；
        本方法返回原始全集，供跨子账户按 position_key 聚合 + 区分量化/手动单。
        复用同一 "OpenOrders" named request（process_open_order 按 name 路由，硬编码不可改名）。
        """
        await self.wait_until_ready(timeout=30)
        name = "OpenOrders"
        if not (request := self._requests.get(name=name)):
            handle = (self._eclient.reqAllOpenOrders
                      if self._fetch_all_open_orders else self._eclient.reqOpenOrders)
            request = self._requests.add(
                req_id=self._next_req_id(),
                name=name,
                handle=handle,
            )
            if not request:
                return []
            request.handle()
        all_orders = await self._await_request(request, timeout, default_value=[])
        return list(all_orders or [])

    InteractiveBrokersClient.query_fa_group_summary = query_fa_group_summary  # type: ignore[attr-defined]
    InteractiveBrokersClient.request_fa_groups_xml = request_fa_groups_xml  # type: ignore[attr-defined]
    InteractiveBrokersClient.query_all_positions = query_all_positions  # type: ignore[attr-defined]
    InteractiveBrokersClient.query_all_open_orders = query_all_open_orders  # type: ignore[attr-defined]


# ── 2. patch wrapper 回调：保留原逻辑 + 按 reqId 路由 FA 响应 ──────────
def _patch_wrapper_for_fa() -> None:
    """幂等：patch accountSummary / accountSummaryEnd / receiveFA。

    原 accountSummaryEnd / receiveFA 是空壳(不路由 client,future 永挂)。patch 后:
    - accountSummary:先调原 process_account_summary(保留 ExecClient 的 accountSummary-{account}
      订阅)，再按 reqId 喂 faGroupSummary-* request.result。
    - accountSummaryEnd / receiveFA:按 reqId/name 匹配 FA request → _end_request(触发 future)。
    所有对 client 的操作经 submit_to_msg_handler_queue(线程安全)。
    """
    global _PATCHED_WRAPPER
    if _PATCHED_WRAPPER:
        return
    _PATCHED_WRAPPER = True
    print("[FA-PATCH] ✓ _patch_wrapper_for_fa: patch wrapper.accountSummary/accountSummaryEnd/receiveFA/position/positionEnd（FA 余额/校验 + 全账户持仓 tap；不碰 openOrder/orderStatus/error 下单回调）", flush=True)

    WrapperCls = _wrapper_mod.InteractiveBrokersEWrapper
    _orig_account_summary = WrapperCls.accountSummary
    _orig_account_summary_end = WrapperCls.accountSummaryEnd
    _orig_receive_fa = WrapperCls.receiveFA

    def accountSummary(self, reqId, account, tag, value, currency):  # noqa: N802
        _orig_account_summary(self, reqId, account, tag, value, currency)
        client = self._client

        async def _collect():
            req = client._requests.get(req_id=reqId)
            if req and str(req.name).startswith("faGroupSummary-"):
                req.result.append((account, tag, value, currency))

        client.submit_to_msg_handler_queue(_collect)

    def accountSummaryEnd(self, reqId):  # noqa: N802
        _orig_account_summary_end(self, reqId)
        client = self._client

        async def _end():
            req = client._requests.get(req_id=reqId)
            if req and str(req.name).startswith("faGroupSummary-"):
                client._end_request(reqId)

        client.submit_to_msg_handler_queue(_end)

    def receiveFA(self, faData, cxml):  # noqa: N802
        _orig_receive_fa(self, faData, cxml)
        client = self._client

        async def _collect():
            if int(faData) == FA_GROUPS_TYPE:
                req = client._requests.get(name="faGroupsXML")
                if req:
                    req.result.append(cxml or "")
                    client._end_request(req.req_id)

        client.submit_to_msg_handler_queue(_collect)

    # ── position / positionEnd：tap 全账户持仓喂给 AllPositions 一次性查询 ──
    _orig_position = WrapperCls.position
    _orig_position_end = WrapperCls.positionEnd

    def position(self, account, contract, position, avgCost):  # noqa: N802
        _orig_position(self, account, contract, position, avgCost)
        client = self._client

        async def _collect():
            req = client._requests.get(name="AllPositions")
            if req:
                sym = getattr(contract, "symbol", "") or ""
                try:
                    qty = float(position)
                    cost = float(avgCost)
                except (TypeError, ValueError):
                    return
                req.result.append((account, sym, qty, cost))

        client.submit_to_msg_handler_queue(_collect)

    def positionEnd(self):  # noqa: N802
        _orig_position_end(self)
        client = self._client

        async def _end():
            req = client._requests.get(name="AllPositions")
            if req:
                client._end_request(req.req_id)

        client.submit_to_msg_handler_queue(_end)

    WrapperCls.position = position  # type: ignore[assignment]
    WrapperCls.positionEnd = positionEnd  # type: ignore[assignment]
    WrapperCls.accountSummary = accountSummary  # type: ignore[assignment]
    WrapperCls.accountSummaryEnd = accountSummaryEnd  # type: ignore[assignment]
    WrapperCls.receiveFA = receiveFA  # type: ignore[assignment]


# ── 3. 对外 API（业务层 order_actor 调用，传入引擎 client）─────────────
async def query_fa_group_balance(ib_client: InteractiveBrokersClient, fa_group: str) -> list[dict]:
    """查 FA Group 余额 → 聚合成 [{currency, total, free, locked}]（复刻 order_actor 格式）。

    异常向上抛(由 order_actor 的查询循环捕获 log)。空 list = 无数据。
    """
    sub_accounts = await ib_client.query_fa_group_summary(fa_group)
    if not sub_accounts:
        return []
    total_usd = sum(t.get("NetLiquidation", 0.0) for t in sub_accounts.values())
    free_usd = sum(t.get("AvailableFunds", 0.0) for t in sub_accounts.values())
    return [{
        "currency": "USD",
        "total": round(total_usd, 2),
        "free": round(free_usd, 2),
        "locked": round(total_usd - free_usd, 2),
    }]


async def validate_fa_group_via_engine(
    ib_client: InteractiveBrokersClient, fa_group: str,
) -> tuple[bool, str]:
    """requestFA(GROUPS) → 解析 XML 确认 fa_group 存在(复刻 fa_validate，修复 GROUPS=1)。"""
    if not fa_group or not fa_group.strip():
        return False, "fa_group 为空"
    try:
        xml_str = await ib_client.request_fa_groups_xml()
    except Exception as e:
        return False, f"requestFA 异常: {e}"
    if not xml_str.strip():
        return False, "requestFA(GROUPS) 无返回（检查 TWS API 与 FA 权限）"
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        return False, f"FA XML 解析失败: {e}"
    names: set[str] = set()
    for grp in root.iter("Group"):
        el = grp.find("name")
        if el is not None and el.text:
            names.add(el.text.strip())
    if fa_group in names:
        return True, f"FA Group '{fa_group}' 已确认"
    preview = ", ".join(sorted(names)[:8])
    suffix = "..." if len(names) > 8 else ""
    return False, f"FA Group '{fa_group}' 不存在；可用: {preview}{suffix}"


async def query_all_positions_aggregated(ib_client: InteractiveBrokersClient) -> list[dict]:
    """reqPositions → 按 symbol 跨 FA 子账户聚合，返回对齐 /api/positions 的列表。

    返回 [{symbol, side, quantity, avg_px_open, accounts}]，quantity 为 0 的剔除。
    avg_px_open 按 |qty| 加权平均；quantity 带符号（正=多，负=空）。
    异常向上抛（由 order_actor 捕获返回 []）。
    """
    rows = await ib_client.query_all_positions()
    by_sym: dict[str, dict] = {}
    for row in rows or []:
        try:
            account, sym, qty, avg_cost = row
        except (TypeError, ValueError):
            continue
        if not sym:
            continue
        try:
            qty = float(qty)
            avg_cost = float(avg_cost)
        except (TypeError, ValueError):
            continue
        sym = str(sym).upper()
        slot = by_sym.setdefault(sym, {"qty": 0.0, "cost_w": 0.0, "by_acct": {}})
        abs_q = abs(qty)
        slot["qty"] += qty
        slot["cost_w"] += avg_cost * abs_q
        if account:
            # reqPositions 每 (账户,合约) 一行，avg_cost 即该账户的每股均成本
            slot["by_acct"][str(account)] = {"quantity": qty, "avg_px_open": avg_cost}
    out: list[dict] = []
    for sym, s in by_sym.items():
        qty = s["qty"]
        if qty == 0:
            continue
        abs_q = abs(qty)
        avg_px = round(s["cost_w"] / abs_q, 4) if abs_q else 0.0
        out.append({
            "symbol": sym,
            "side": "LONG" if qty > 0 else "SHORT",
            "quantity": qty,
            "avg_px_open": avg_px,
            "accounts": [
                {"account": a, "quantity": d["quantity"], "avg_px_open": d["avg_px_open"]}
                for a, d in sorted(s["by_acct"].items())
            ],
        })
    return out


def _order_meta(redis_client, coid: str) -> dict | None:
    """读 order:meta:{coid}（auto_pm 提交时写入 position_key/leg/sym/proposal_id）。"""
    if not redis_client or not coid:
        return None
    try:
        raw = redis_client.get(f"order:meta:{coid}")
        return json.loads(raw) if raw else None
    except Exception:
        return None


def _enrich_order_meta(redis_client, coid: str, perm_id, client_id, account) -> None:
    """把 IBKR 回传的 permId/clientId/account 补进 order:meta:{coid}（merge），
    并建反向 order:permid:{permId}→coid。permId 为账户级永久 ID，跨重启稳定。"""
    if not redis_client or not coid:
        return
    try:
        key = f"order:meta:{coid}"
        raw = redis_client.get(key)
        meta = json.loads(raw) if raw else {}
        meta["perm_id"] = int(perm_id) if perm_id else meta.get("perm_id")
        meta["client_id"] = int(client_id) if client_id is not None else meta.get("client_id")
        if account:
            meta.setdefault("accounts", set())
            if isinstance(meta["accounts"], list):
                if account not in meta["accounts"]:
                    meta["accounts"].append(account)
            else:
                meta["accounts"] = [account]
        meta["updated_at"] = int(time.time())
        redis_client.set(key, json.dumps(meta))
        if perm_id:
            redis_client.set(f"order:permid:{int(perm_id)}", coid)
    except Exception:
        pass


def _ib_order_to_dict(o) -> dict:
    """IBOrder → 可序列化字典（取定势分组/查询所需字段，容错缺失）。"""
    contract = getattr(o, "contract", None)
    state = getattr(o, "order_state", None)
    def _f(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return None
    return {
        "coid": getattr(o, "orderRef", "") or "",
        "perm_id": getattr(o, "permId", 0) or 0,
        "client_id": getattr(o, "clientId", 0) or 0,
        "account": getattr(o, "account", "") or "",
        "fa_group": getattr(o, "faGroup", "") or "",
        "symbol": getattr(contract, "symbol", "") or "",
        "action": getattr(o, "action", "") or "",
        "qty": _f(getattr(o, "totalQuantity", None)),
        "order_type": getattr(o, "orderType", "") or "",
        "lmt_price": _f(getattr(o, "lmtPrice", None)),
        "aux_price": _f(getattr(o, "auxPrice", None)),
        "status": getattr(state, "status", "") or "" if state else "",
        "order_id": getattr(o, "orderId", 0) or 0,
    }


def _position_key_from_coid(coid: str) -> str:
    """从 coid={poskey}-{leg}-{counter} 解析 poskey（poskey 仅大写字母数字，无连字符）。"""
    if not coid or "-" not in coid:
        return ""
    parts = coid.split("-")
    # 期望 3 段（poskey/leg/counter）；leg ∈ {E,S,T,C}；否则视为非引擎格式
    if len(parts) >= 3 and parts[-2] in ("E", "S", "T", "C"):
        return parts[0]
    return ""


async def query_all_open_orders_grouped(ib_client, redis_client=None) -> dict:
    """reqAllOpenOrders → 按 position_key 跨子账户聚合，区分量化/手动/其他 API 单。

    返回 {
      "engine": [ {position_key, proposal_id, symbol, side, net_qty, legs:[...], accounts:[...] } ],
      "manual": [ {...单条 TWS 手动单 clientId==0...} ],
      "other_api": [ {...非本引擎 client_id 的 API 单...} ],
      "fetched_at": int(time),
    }
    legs 每条含 leg/coid/account/qty/order_type/price/status/perm_id。
    引擎单按 position_key 聚合（entry+stop+tp+close 跨子账户归一仓位）。

    副作用：把 permId/clientId/account 补进 order:meta:{coid} + 反向 order:permid
    （permId 为账户级永久 ID，跨重启稳定句柄；查询时补录，不动下单热路径）。

    异常向上抛（由 order_actor 捕获返回空结构）。
    """
    orders = await ib_client.query_all_open_orders()
    engine_groups: dict[str, dict] = {}
    manual: list[dict] = []
    other_api: list[dict] = []

    for o in orders or []:
        d = _ib_order_to_dict(o)
        coid = d["coid"]
        client_id = d["client_id"]
        # 引擎单：补录 permId（跨重启稳定句柄）
        if coid and client_id != 0:
            _enrich_order_meta(redis_client, coid, d["perm_id"], client_id, d["account"])

        if client_id == 0:
            # TWS GUI 手动单（orderRef 通常空 / 无 position_key）
            manual.append(d)
            continue

        # 解析 position_key：优先 order:meta（含 proposal_id），退化到 coid 前缀解析
        meta = _order_meta(redis_client, coid) or {}
        poskey = meta.get("position_key") or _position_key_from_coid(coid)
        if not poskey:
            # 非零 clientId 但非本引擎格式 → 其他 API client 的单
            other_api.append(d)
            continue

        leg_letter = (coid.split("-")[-2] if "-" in coid and coid.split("-")[-2] in
                      ("E", "S", "T", "C") else "")
        leg = {
            "leg": leg_letter,
            "coid": coid,
            "account": d["account"],
            "qty": d["qty"],
            "order_type": d["order_type"],
            "price": d["lmt_price"] if d["order_type"] in ("LMT", "STP LMT") else d["aux_price"],
            "status": d["status"],
            "perm_id": d["perm_id"],
        }
        grp = engine_groups.setdefault(poskey, {
            "position_key": poskey,
            "proposal_id": meta.get("proposal_id", ""),
            "symbol": d["symbol"] or meta.get("symbol", ""),
            "side": "BUY" if d["action"] == "BUY" else "SELL",
            "legs": [],
            "accounts": set(),
        })
        grp["legs"].append(leg)
        if d["account"]:
            grp["accounts"].add(d["account"])
        # side 以 entry(BUY 建多)为准；若首条是 stop/tp(SELL)，后续 entry 会覆盖
        if leg_letter == "E":
            grp["side"] = "BUY" if d["action"] == "BUY" else "SELL"

    # 净量按 action 符号累加（BUY +qty / SELL -qty），账户集合转 list
    engine_out = []
    for poskey, grp in engine_groups.items():
        net = 0.0
        for lg in grp["legs"]:
            q = lg["qty"] or 0.0
            # entry 腿方向决定仓位的净持仓；stop/tp/close 是平仓向，不计入净持仓
            if lg["leg"] == "E":
                net += q if grp["side"] == "BUY" else -q
        engine_out.append({
            "position_key": grp["position_key"],
            "proposal_id": grp["proposal_id"],
            "symbol": grp["symbol"],
            "side": grp["side"],
            "net_qty": round(net, 4),
            "accounts": sorted(grp["accounts"]),
            "legs": sorted(grp["legs"], key=lambda x: {"E": 0, "S": 1, "T": 2, "C": 3}.get(x["leg"], 9)),
        })

    return {
        "engine": engine_out,
        "manual": manual,
        "other_api": other_api,
        "fetched_at": int(time.time()),
    }
