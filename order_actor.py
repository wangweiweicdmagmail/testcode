"""
OrderGatewayActor — 使用 NautilusTrader 标准 MessageBus 消息架构的订单网关代理

架构说明：
  外部进程（order_sender.py）
    │  HTTP POST /order
    ▼
  HTTP Server（后台守护线程）
    │  asyncio.run_coroutine_threadsafe()
    ▼
  _async_bridge()（在引擎 asyncio 事件循环中执行）
    │  self.msgbus.publish(ExternalOrderCommand.TOPIC, event)
    ▼
  MessageBus 路由 → on_external_order_command(event)
    │  （标准 NautilusTrader 消息模式）
    ▼
  submit_order() → RiskEngine → ExecEngine → IBKR

关键点：
  - ExternalOrderCommand 继承自 nautilus_trader.core.message.Event
  - 使用 msgbus.subscribe / msgbus.publish 标准 API
  - HTTP Server 只负责接收外部 HTTP 并跨线程安全地触发消息发布
  - 策略 (BarLoggerStrategy) 与本 Actor 完全解耦
"""
import asyncio
import json
import time
import queue
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import redis as _redis_lib   # pip install redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

from nautilus_trader.adapters.interactive_brokers.common import IBOrderTags
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import OrderListId
from nautilus_trader.model.orders.list import OrderList
from nautilus_trader.trading.strategy import Strategy
from decimal import Decimal
from events import STTrailSettingsEvent


# ---------------------------------------------------------------------------
# 1. 自定义消息类型（继承 nautilustrader Event）
# ---------------------------------------------------------------------------
class ExternalOrderCommand(Event):
    """
    表示来自引擎外部的下单指令的 Event 消息

    发布到 MessageBus Topic: "commands.order.external"
    """
    TOPIC = "commands.order.external"

    def __init__(
        self,
        instrument_id: str,
        side: str,            # "BUY" | "SELL"
        qty: int,
        order_type: str = "MARKET",  # "MARKET" | "LIMIT" | "BRACKET"
        price: float | None = None,
        stop_loss: float | None = None,    # BRACKET: 止损触发价
        sl_steps: list | None = None,     # BRACKET: 依次修改的止损价列表 [602, 603]
        sl_step_secs: int = 60,           # BRACKET: 两次修改的间隔秒数
    ) -> None:
        # Cython Event 子类：直接设置私有属性，不调 super().__init__()
        self._id = UUID4()
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        # 业务字段
        self.instrument_id = instrument_id
        self.side = side.upper()
        self.qty = int(qty)   # 强制转整数，拒绝浮点股数
        self.order_type = order_type.upper()
        self.price = price
        self.stop_loss = stop_loss
        self.sl_steps = sl_steps or []
        self.sl_step_secs = sl_step_secs

    def __repr__(self) -> str:
        return (
            f"ExternalOrderCommand("
            f"{self.order_type} {self.side} {self.qty}x {self.instrument_id}"
            f"{f' @ {self.price}' if self.price else ''})"
        )



# ---------------------------------------------------------------------------
# 2. Actor 配置
# ---------------------------------------------------------------------------
class OrderGatewayConfig(StrategyConfig, frozen=True):
    """
    OrderGatewayActor 配置

    参数
    ----------
    http_host : str
        HTTP 监听地址，默认 localhost
    http_port : int
        HTTP 监听端口，默认 8888
    fa_group : str
        FA Group 名称（留空则不使用 FA 分配，直接在当前账号下单）
    fa_method : str
        FA 分配方法：EqualQuantity | AvailableEquity | NetLiq | PctChange
    """
    http_host: str = "localhost"
    http_port: int = 8888
    fa_group: str = ""         # 留空则不使用 FA
    fa_method: str = "EqualQuantity"


# ---------------------------------------------------------------------------
# 3. Actor 实现
# ---------------------------------------------------------------------------
class OrderGatewayActor(Strategy):
    """
    订单网关代理 Actor

    使用 NautilusTrader 标准 MessageBus pub/sub 机制作为内部消息总线，
    以 HTTP Server 作为外部通信入口。
    """

    def __init__(self, config: OrderGatewayConfig) -> None:
        super().__init__(config)
        self._loop: asyncio.AbstractEventLoop | None = None
        self._http_server: HTTPServer | None = None
        self._sl_tasks: list[asyncio.Task] = []  # P5: 保存 task 引用，避免被 GC 或引擎停止时静默取消
        self._redis: "_redis_lib.Redis | None" = None  # Redis 确认客户端

        # 引擎 IB client 引用（由 main.py 在 node.build() 后注入）
        self._ib_client = None
        # 账户余额定期轮询（充当兆底：订单事件已覆盖大部分情屦1）
        self._account_poll_timer: threading.Timer | None = None
        self._account_poll_running: bool = False
        self._account_poll_interval: int = 120  # 定期轮询间隔（秒）
        # 防止并发 reqAccountSummary（IBKR code=322 限制同时只能有1个请求）
        self._account_sync_lock = threading.Lock()
        # accountSummary 实时缓存：{account_id: {currency: {tag: float}}}
        # 由引擎已有的 accountSummary-{account_id} 事件实时写入
        self._account_summary_cache: dict = {}
        # 待提交的止损单：{entry_client_order_id: (sl_order, sl_tasks_params)}
        # 入场单成交后在 on_order_filled 里提交
        self._pending_sl: dict = {}

    # ------------------------------------------------------------------
    # 引擎 IB client 注入（main.py 在 node.build() 后调用）
    # ------------------------------------------------------------------

    def set_ib_client(self, ib_client) -> None:
        """接收引擎的 InteractiveBrokersClient 引用，复用已有 TWS 连接"""
        self._ib_client = ib_client
        # node.build() 阶段 self.log 尚未初始化，用 print 避免异常
        print("[Gateway] IB client 已注入 OrderGatewayActor，FA Group 余额将使用引擎连接")


    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """启动：注册 MessageBus 订阅 → 启动 HTTP Server"""
        self.log.info("[Gateway] 正在启动 OrderGatewayActor...")
        try:
            # 获取引擎的 asyncio 事件循环（跨线程通信用）
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                self._loop = asyncio.get_event_loop()
            
            self.log.info(f"[Gateway] 成功获取事件循环: {self._loop}")

            # Redis 客户端（用于向前端推送订单状态通知）
            if _REDIS_AVAILABLE:
                try:
                    self._redis = _redis_lib.Redis(
                        host="localhost", port=6379,
                        decode_responses=True, socket_connect_timeout=2
                    )
                    self._redis.ping()
                    self.log.info("[Gateway] Redis 已连接，将推送 order:update 事件")
                except Exception as e:
                    self._redis = None
                    self.log.warning(f"[Gateway] Redis 连接失败，order:update 推送已禁用: {e}")
            else:
                self.log.warning("[Gateway] redis 包未安装，order:update 推送已禁用")

            # ★ 标准 MessageBus 订阅：注册 ExternalOrderCommand 的处理函数
            self.msgbus.subscribe(
                topic=ExternalOrderCommand.TOPIC,
                handler=self.on_external_order_command,
            )
            self.log.info(
                f"[Gateway] 已订阅 MessageBus Topic: {ExternalOrderCommand.TOPIC!r}"
            )

            # 启动 HTTP 网关线程
            self._start_http_server()

            self.log.info(
                f"[Gateway] OrderGatewayActor 就绪 | "
                f"HTTP: http://{self.config.http_host}:{self.config.http_port}/order"
            )
        except Exception as e:
            self.log.error(f"[Gateway] OrderGatewayActor 启动失败: {e}")
            import traceback
            self.log.error(traceback.format_exc())

        # 延迟 30s 注册 accountSummary 事件（等待 IB client 注入 + TWS 完成初始化）
        _t = threading.Timer(30.0, self._register_account_summary_events)
        _t.daemon = True
        _t.start()

        # 启动定期写入 Redis（待事件注册后 60s 开始，之后每 120s）
        self._account_poll_running = True
        _poll_t = threading.Timer(60.0, self._start_account_poll)
        _poll_t.daemon = True
        _poll_t.start()

    def on_stop(self) -> None:
        """停止：取消订阅 + 关闭 HTTP Server + 取消止损修改 tasks"""
        self.msgbus.unsubscribe(
            topic=ExternalOrderCommand.TOPIC,
            handler=self.on_external_order_command,
        )

        # P5: 取消未完成的止损修改计划
        for t in self._sl_tasks:
            if not t.done():
                t.cancel()
                self.log.warning(f"[Gateway] 止损修改 task 已取消: {t}")
        self._sl_tasks.clear()

        # 停止账户余额定期轮询
        self._account_poll_running = False
        if self._account_poll_timer and self._account_poll_timer.is_alive():
            self._account_poll_timer.cancel()

        if self._redis:
            self._redis.close()

        if self._http_server:
            threading.Thread(
                target=self._http_server.shutdown, daemon=True
            ).start()

        self.log.info("[Gateway] OrderGatewayActor 已停止")

    # ------------------------------------------------------------------
    # 账户余额同步（FA Group 余额 → Redis）
    # ------------------------------------------------------------------

    def _start_account_poll(self) -> None:
        """账户余额定期轮询（兜底补偿，订单事件已覆盖大部分情况）"""
        if not self._account_poll_running:
            return
        try:
            self._sync_account_to_redis()
        except Exception as e:
            self.log.warning(f"[Account] 定期轮询异常: {e}")
        finally:
            if self._account_poll_running:
                self._account_poll_timer = threading.Timer(
                    self._account_poll_interval, self._start_account_poll
                )
                self._account_poll_timer.daemon = True
                self._account_poll_timer.start()

    def _register_account_summary_events(self) -> None:
        """
        查询 FA Group 账户余额：

        直接 patch self._ib_client.process_account_summary 实例方法，
        按 req_id 精确过滤只收 reqAccountSummary(fa_group) 这次请求的响应，
        完全隔离引擎 subscribe_account_summary("All") 的持续推送干扰。
        """
        if self._ib_client is None:
            self.log.warning("[Account] IB client 未注入，延迟 10s 再试")
            t = threading.Timer(10.0, self._register_account_summary_events)
            t.daemon = True
            t.start()
            return

        fa_group = self.config.fa_group
        if not fa_group:
            self.log.error("[Account] fa_group 未配置，无法查询账户余额")
            return

        group_name = fa_group  # 直接用 FA Group 名称（dt_test）

        try:
            from ibapi.account_summary_tags import AccountSummaryTags

            req_id = self._ib_client._next_req_id()

            # 独立缓存：只存本次查询返回的账户数据（按 req_id 过滤）
            query_cache: dict = {}
            target_req_id = req_id  # 闭包捕获

            # 保存原始实例方法
            original_process = self._ib_client.process_account_summary

            async def _patched_process(*, req_id, account_id, tag, value, currency):
                # 只收本次 req_id 对应的数据
                if req_id == target_req_id and currency and tag:
                    if account_id not in query_cache:
                        query_cache[account_id] = {}
                    if currency not in query_cache[account_id]:
                        query_cache[account_id][currency] = {}
                    try:
                        query_cache[account_id][currency][tag] = float(value)
                    except (ValueError, TypeError):
                        pass
                # 始终调用原始方法，不破坏引擎内部逻辑
                await original_process(req_id=req_id, account_id=account_id,
                                       tag=tag, value=value, currency=currency)

            # 安装 patch
            self._ib_client.process_account_summary = _patched_process

            # 发起查询
            self._ib_client._eclient.reqAccountSummary(req_id, group_name, AccountSummaryTags.AllTags)
            self.log.info(f"[Account] 已触发 reqAccountSummary({group_name}) req_id={req_id}，等待 20s 收集数据")

            def _finish():
                # 卸载 patch，恢复原始方法
                self._ib_client.process_account_summary = original_process
                try:
                    self._ib_client._eclient.cancelAccountSummary(req_id)
                    self.log.info(f"[Account] cancelAccountSummary req_id={req_id}，共 {len(query_cache)} 个账户响应")
                except Exception as ce:
                    self.log.warning(f"[Account] cancelAccountSummary 异常: {ce}")
                # 将本次查询结果存入缓存，再写入 Redis
                self._account_summary_cache = query_cache
                self._sync_account_to_redis()

            t = threading.Timer(20.0, _finish)
            t.daemon = True
            t.start()

        except Exception as e:
            self.log.warning(f"[Account] reqAccountSummary 触发失败: {e}，延迟 10s 后用 fallback")
            t = threading.Timer(10.0, self._sync_account_to_redis)
            t.daemon = True
            t.start()

    def _sync_account_to_redis(self) -> None:
        """
        将 _account_summary_cache 里累加的子账户数据汇总后写入 Redis。

        \u6570据来源：引擎已有的 subscribe_account_summary("All") 实时推送（不会再发 reqAccountSummary）。
        """
        if not self._account_sync_lock.acquire(blocking=False):
            self.log.debug("[Account] 账户余额同步已在进行中，跳过本次")
            return
        try:
            cache = self._account_summary_cache
            if cache:
                # 按货币累加所有子账户的 NetLiquidation / FullAvailableFunds
                totals: dict = {}   # {currency: {tag: float}}
                for aid, currencies in cache.items():
                    for currency, tags in currencies.items():
                        if currency not in totals:
                            totals[currency] = {}
                        for tag, val in tags.items():
                            totals[currency][tag] = totals[currency].get(tag, 0.0) + val

                balances = []
                for currency, tags in totals.items():
                    net_liq  = tags.get("NetLiquidation",    0.0)
                    avail    = tags.get("FullAvailableFunds", 0.0)
                    init_mar = tags.get("FullInitMarginReq",  0.0)
                    maint_mar= tags.get("FullMaintMarginReq", 0.0)
                    if net_liq > 0 or avail > 0:
                        balances.append({
                            "currency": currency,
                            "total":  round(net_liq,  2),
                            "free":   round(avail,    2),
                            "locked": round(init_mar + maint_mar, 2),
                        })

                if balances:
                    fa_group = self.config.fa_group or "FA"
                    self.log.info(
                        f"[Account] ✓ FA Group={fa_group} 余额查询成功（{len(cache)} 个子账户汇总）"
                    )
                    self._write_account_to_redis(fa_group, balances)
                    return
                else:
                    self.log.warning("[Account] 缓存数据有效为零，fallback")
            else:
                self.log.warning("[Account] _account_summary_cache 为空，待 TWS 推送数据...fallback")

            # Fallback：cache.account_for_venue
            from nautilus_trader.model.identifiers import Venue
            account = self.cache.account_for_venue(Venue("IB"))
            if account is None:
                self.log.warning("[Account] cache.account_for_venue('IB') 返回 None")
                return
            balances = []
            for currency, bal in account.balances().items():
                balances.append({
                    "currency": str(currency),
                    "total":    round(float(bal.total.as_double()),  2),
                    "free":     round(float(bal.free.as_double()),   2),
                    "locked":   round(float(bal.locked.as_double()), 2),
                })
            if balances:
                account_id = str(account.id)
                self.log.info(f"[Account] Fallback 余额  account={account_id}")
                self._write_account_to_redis(account_id, balances)
        except Exception as e:
            self.log.warning(f"[Account] _sync_account_to_redis 失败: {e}")
        finally:
            self._account_sync_lock.release()

    def _write_account_to_redis(self, account_id: str, balances: list) -> None:
        """将账户余额写入 Redis account:funds 并 PUBLISH account:update"""
        if not self._redis:
            return
        payload = {
            "account_id": account_id,
            "balances": balances,
            "ts": int(time.time()),
        }
        try:
            self._redis.set("account:funds", json.dumps(payload))
            self._redis.publish("account:update", json.dumps(payload))
            usd = next((b for b in balances if b["currency"] == "USD"), None)
            if usd:
                self.log.info(
                    f"[Account] 余额已同步 Redis  "
                    f"total={usd['total']:,.2f}  free={usd['free']:,.2f}  USD"
                )
        except Exception as e:
            self.log.warning(f"[Account] Redis 写入失败: {e}")

    def _trigger_account_sync(self, delay: float = 3.0) -> None:
        """
        订单事件后触发一次账户余额刷新。

        不读旧缓存，而是重新发起 reqAccountSummary(fa_group) 请求，
        等 10s 收集 IBKR 最新数据后写入 Redis。
        避免：成交/撤单后余额未更新的问题。
        """
        def _do_refresh():
            if self._ib_client is None:
                return
            fa_group = self.config.fa_group
            if not fa_group:
                return
            try:
                from ibapi.account_summary_tags import AccountSummaryTags
                req_id = self._ib_client._next_req_id()
                query_cache: dict = {}
                target_req_id = req_id
                original_process = self._ib_client.process_account_summary

                async def _patched(*, req_id, account_id, tag, value, currency):
                    if req_id == target_req_id and currency and tag:
                        if account_id not in query_cache:
                            query_cache[account_id] = {}
                        if currency not in query_cache[account_id]:
                            query_cache[account_id][currency] = {}
                        try:
                            query_cache[account_id][currency][tag] = float(value)
                        except (ValueError, TypeError):
                            pass
                    await original_process(req_id=req_id, account_id=account_id,
                                           tag=tag, value=value, currency=currency)

                self._ib_client.process_account_summary = _patched
                self._ib_client._eclient.reqAccountSummary(req_id, fa_group, AccountSummaryTags.AllTags)
                self.log.debug(f"[Account] 订单事件后刷新账户余额 req_id={req_id}，等待 10s")

                def _finish():
                    self._ib_client.process_account_summary = original_process
                    try:
                        self._ib_client._eclient.cancelAccountSummary(req_id)
                    except Exception:
                        pass
                    if query_cache:
                        self._account_summary_cache = query_cache
                    self._sync_account_to_redis()

                t2 = threading.Timer(10.0, _finish)
                t2.daemon = True
                t2.start()
            except Exception as e:
                self.log.warning(f"[Account] _trigger_account_sync 失败: {e}")
                # fallback：直接读旧缓存
                self._sync_account_to_redis()

        t = threading.Timer(delay, _do_refresh)
        t.daemon = True
        t.start()


    # ------------------------------------------------------------------
    # 账户 & 仓位查询（供 HTTP GET /account 和 /positions 调用）
    # ------------------------------------------------------------------

    def get_account_info(self) -> dict:
        """
        返回 IBKR 账户净资产和可用资金。
        通过 run_coroutine_threadsafe 在引擎事件循环中访问 cache，避免线程竞争。
        """
        if self._loop is None:
            return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}
        future = asyncio.run_coroutine_threadsafe(
            self._async_get_account_info(), self._loop
        )
        try:
            return future.result(timeout=3.0)
        except Exception as e:
            self.log.warning(f"[Gateway] get_account_info 超时或失败: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}

    async def _async_get_account_info(self) -> dict:
        """在引擎事件循环中安全访问 cache，获取账户余额"""
        try:
            from nautilus_trader.model.identifiers import Venue
            from nautilus_trader.model.currencies import USD
            venue = Venue("INTERACTIVE_BROKERS")
            account = self.cache.account_for_venue(venue)
            if account is None:
                return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}
            total = account.balance_total(USD)
            free  = account.balance_free(USD)
            return {
                "total_equity":   float(total.as_double()) if total else 0.0,
                "available_cash": float(free.as_double())  if free  else 0.0,
                "currency": "USD",
            }
        except Exception as e:
            self.log.warning(f"[Gateway] _async_get_account_info 失败: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}

    def get_positions(self) -> list:
        """
        返回当前所有开放仓位。
        通过 run_coroutine_threadsafe 在引擎事件循环中访问 cache，避免线程竞争。
        """
        if self._loop is None:
            return []
        future = asyncio.run_coroutine_threadsafe(
            self._async_get_positions(), self._loop
        )
        try:
            return future.result(timeout=3.0)
        except Exception as e:
            self.log.warning(f"[Gateway] get_positions 超时或失败: {e}")
            return []

    async def _async_get_positions(self) -> list:
        """在引擎事件循环中安全访问 cache，获取开放仓位"""
        result = []
        try:
            for pos in self.cache.positions_open():
                sym = pos.instrument_id.symbol.value
                last_price = None
                instrument = self.cache.instrument(pos.instrument_id)
                # 尝试从 bar 缓存拿最新收盘价
                bars = self.cache.bars(pos.instrument_id)
                if bars and instrument:
                    # bars 可能是对象或列表，用 list() 确保支持索引
                    bar_list = list(bars)
                    if bar_list:
                        last_price = instrument.make_price(bar_list[-1].close)

                upnl = None
                if last_price is not None:
                    try:
                        money = pos.unrealized_pnl(last_price)
                        upnl = float(money.as_double()) if money else None
                    except Exception:
                        pass

                result.append({
                    "symbol":         sym,
                    "instrument_id":  str(pos.instrument_id),
                    "side":           "LONG" if pos.is_long else "SHORT",
                    "quantity":       float(pos.quantity),
                    "avg_px_open":    float(pos.avg_px_open),
                    "unrealized_pnl": upnl,
                    "realized_pnl":   float(pos.realized_pnl.as_double()) if pos.realized_pnl else 0.0,
                    "last_price":     float(last_price) if last_price else None,
                })
        except Exception as e:
            self.log.warning(f"[Gateway] _async_get_positions 失败: {e}")
        return result

    def get_active_orders(self) -> dict:
        """
        返回当前活跃的入场价和止损单信息，供前端恢复价格线显示。
        格式：{symbol: {entry: {...}, stop_loss: {...}}}
        """
        if self._loop is None:
            return {}
        future = asyncio.run_coroutine_threadsafe(
            self._async_get_active_orders(), self._loop
        )
        try:
            return future.result(timeout=3.0)
        except Exception as e:
            self.log.warning(f"[Gateway] get_active_orders 失败: {e}")
            return {}

    async def _async_get_active_orders(self) -> dict:
        """查询 cache 中 ACCEPTED 状态的订单和已打开的仓位，组合成前端可用的价格线数据"""
        result = {}
        try:
            # 1. 已打开的仓位 → 入场价
            for pos in self.cache.positions_open():
                sym = pos.instrument_id.symbol.value
                result.setdefault(sym, {})
                result[sym]["entry"] = {
                    "price": float(pos.avg_px_open),
                    "side": "LONG" if pos.is_long else "SHORT",
                    "quantity": float(pos.quantity),
                }
            # 2. ACCEPTED 状态的止损单（STOP_MARKET）
            for order in self.cache.orders_open():
                order_type = str(order.order_type).replace("OrderType.", "")
                if order_type != "STOP_MARKET":
                    continue
                sym = order.instrument_id.symbol.value
                result.setdefault(sym, {})
                tp = getattr(order, "trigger_price", None)
                result[sym]["stop_loss"] = {
                    "price": float(tp) if tp else None,
                    "side": str(order.side).replace("OrderSide.", ""),
                    "quantity": float(order.quantity),
                    "client_order_id": str(order.client_order_id),
                }
        except Exception as e:
            self.log.warning(f"[Gateway] _async_get_active_orders 失败: {e}")
        return result


    # ------------------------------------------------------------------
    # ★ 标准 MessageBus 消息处理器
    # ------------------------------------------------------------------

    def on_external_order_command(self, event: ExternalOrderCommand) -> None:
        """
        处理来自 MessageBus 的外部下单指令
        此函数由 msgbus.publish() 在引擎事件循环中同步触发
        """
        self.log.info(
            f"[Order] ⬇ 收到下单指令  "
            f"类型={event.order_type}  "
            f"{'买入' if event.side == 'BUY' else '卖出'}  "
            f"qty={event.qty}  "
            f"symbol={event.instrument_id}  "
            f"{'price=' + str(event.price) + '  ' if event.price else ''}"
            f"{'stop_loss=' + str(event.stop_loss) + '  ' if event.stop_loss else ''}"
            f"{'sl_steps=' + str(event.sl_steps) if event.sl_steps else ''}"
        )

        # 解析合约
        instrument_id = InstrumentId.from_str(event.instrument_id)
        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            self.log.error(
                f"[Gateway] 合约 {instrument_id} 未加载，"
                f"请将其加入 load_ids 后重启"
            )
            return

        order_side = OrderSide.BUY if event.side == "BUY" else OrderSide.SELL
        quantity = instrument.make_qty(Decimal(event.qty))

        if event.order_type == "MARKET":
            order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                time_in_force=TimeInForce.DAY,  # M2: IBKR 市价单不接受 GTC
                tags=self._fa_tags(),
            )
            self.log.info(
                f"[Order] → submit MARKET  "
                f"{'买入' if order_side == OrderSide.BUY else '卖出'}  "
                f"qty={quantity}  {instrument_id}  "
                f"ClientOrderId={order.client_order_id}"
            )
            self.submit_order(order)
            self._log_submitted(event, order.client_order_id.value)


        elif event.order_type == "LIMIT":
            if event.price is None:
                self.log.error("[Gateway] LIMIT 单必须提供 price")
                return
            order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                price=instrument.make_price(Decimal(str(event.price))),
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(),
            )
            self.log.info(
                f"[Order] → submit LIMIT  "
                f"{'买入' if order_side == OrderSide.BUY else '卖出'}  "
                f"qty={quantity}  price={event.price}  {instrument_id}  "
                f"ClientOrderId={order.client_order_id}"
            )
            self.submit_order(order)
            self._log_submitted(event, order.client_order_id.value)

        elif event.order_type == "BRACKET":
            # 正确的 BRACKET 实现：
            # 1. 只提交入场单（市价）
            # 2. 入场单成交后（on_order_filled）再提交止损单
            # OCA 方案不可用：入场单成交会触发 OCA 取消止损单
            if event.stop_loss is None:
                self.log.error("[Gateway] BRACKET 单必须提供 stop_loss")
                return

            # 入场单（市价）：FA 分配 tags
            entry_order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                time_in_force=TimeInForce.DAY,
                tags=self._fa_tags(),
            )

            # 止损单参数：暂不提交，存入 _pending_sl 等入场单成交后提交
            sl_side = OrderSide.SELL if order_side == OrderSide.BUY else OrderSide.BUY
            sl_price = instrument.make_price(Decimal(str(event.stop_loss)))
            sl_order = self.order_factory.stop_market(
                instrument_id=instrument_id,
                order_side=sl_side,
                quantity=quantity,
                trigger_price=sl_price,
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(),
            )

            # 存入待提交字典，键为入场单的 client_order_id
            self._pending_sl[entry_order.client_order_id.value] = (
                sl_order,
                list(event.sl_steps),
                event.sl_step_secs,
                instrument,
            )

            # 只提交入场单
            self.submit_order(entry_order)

            self.log.info(
                f"[Gateway] BRACKET 入场单已提交 | "
                f"Entry={entry_order.client_order_id} {order_side.name} qty={quantity} {instrument_id} | "
                f"止损待入场单成交后提交 @ {event.stop_loss}"
            )

        else:
            self.log.error(f"[Gateway] 不支持的订单类型: {event.order_type}")
            return


    def _log_submitted(self, event: "ExternalOrderCommand", client_order_id: str) -> None:
        """统一打印单笔订单提交成功日志"""
        self.log.info(
            f"[Gateway] ✅ 订单已提交 → "
            f"{event.order_type} {'买入' if event.side == 'BUY' else '卖出'} "
            f"{event.qty}股 {event.instrument_id} "
            f"{'| FA Group=' + self.config.fa_group if self.config.fa_group else ''} | "
            f"ClientOrderId={client_order_id}"
        )

    # ------------------------------------------------------------------
    # Redis 订单状态推送辅助方法
    # ------------------------------------------------------------------

    def _pub_order(self, status: str, event, extra: dict | None = None) -> None:
        """
        向 Redis 发布 order:update 消息，前端通过 WebSocket 接收并触发语音/提示。

        消息格式：
        {
            "status": "FILLED" | "REJECTED" | "ACCEPTED" | ...,
            "client_order_id": "...",
            "venue_order_id": "...",
            "reason": "..."       (REJECTED/DENIED 才有),
            "last_px": "...",    (FILLED 才有),
            "last_qty": "...",   (FILLED 才有),
            "side": "BUY"|"SELL",
            "symbol": "QQQ",    (从 instrument_id 提取)
            "ts": 1234567890
        }
        """
        if not self._redis:
            return
        try:
            msg = {
                "status": status,
                "client_order_id": str(event.client_order_id),
                "ts": int(time.time()),
            }
            # 安全取字段（各 event 类型字段不同）
            for field in ("venue_order_id", "reason", "last_px", "last_qty",
                          "filled_qty", "leaves_qty", "commission"):
                val = getattr(event, field, None)
                if val is not None:
                    msg[field] = str(val)
            # 尝试从 order cache 拿 side、symbol、order_type、quantity、trigger_price
            try:
                order = self.cache.order(event.client_order_id)
                if order:
                    msg["side"] = str(order.side).replace("OrderSide.", "")
                    msg["symbol"] = str(order.instrument_id).split(".")[0]
                    msg["order_type"] = str(order.order_type).replace("OrderType.", "")
                    msg["quantity"] = str(order.quantity)
                    # 止损单有 trigger_price
                    tp = getattr(order, "trigger_price", None)
                    if tp is not None:
                        msg["trigger_price"] = str(tp)
            except Exception:
                pass
            if extra:
                msg.update(extra)
            self._redis.publish("order:update", json.dumps(msg, ensure_ascii=False))
        except Exception as e:
            self.log.warning(f"[Gateway] Redis publish order:update 失败: {e}")

    # ------------------------------------------------------------------
    # 订单生命周期回调（NautilusTrader 标准 on_order_* 接口）
    # 覆盖范围：denied → rejected → accepted → (triggered) → filled/canceled/expired
    # ------------------------------------------------------------------

    def on_order_denied(self, event) -> None:
        """订单被 NautilusTrader 风控引擎拒绝（未到达交易所）"""
        self.log.error(
            f"[Order] ❌ DENIED  "
            f"ClientOrderId={event.client_order_id}  "
            f"原因: {event.reason}"
        )
        self._pub_order("DENIED", event)

    def on_order_rejected(self, event) -> None:
        """订单被交易所拒绝（已到达 IBKR，IBKR 拒绝）"""
        self.log.error(
            f"[Order] ❌ REJECTED  "
            f"ClientOrderId={event.client_order_id}  "
            f"原因: {event.reason}"
        )
        self._pub_order("REJECTED", event)

    def on_order_accepted(self, event) -> None:
        """订单被交易所接受（已进入撮合队列，等待成交）"""
        self.log.info(
            f"[Order] ✅ ACCEPTED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )
        self._pub_order("ACCEPTED", event)

    def on_order_pending_update(self, event) -> None:
        """改单请求已发出，等待交易所响应"""
        self.log.info(
            f"[Order] ⏳ PENDING_UPDATE  "
            f"ClientOrderId={event.client_order_id}"
        )

    def on_order_updated(self, event) -> None:
        """改单成功（止损价移动等）"""
        self.log.info(
            f"[Order] ✅ UPDATED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )

    def on_order_triggered(self, event) -> None:
        """止损单触发（stop price 已触碰，转为市价单执行）"""
        self.log.warning(
            f"[Order] 🔔 TRIGGERED（止损单触发）"
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )
        self._pub_order("TRIGGERED", event)

    def on_order_filled(self, event) -> None:
        """订单完全成交"""
        self.log.info(
            f"[Order] ✅ FILLED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}  "
            f"成交价={event.last_px}  "
            f"成交量={event.last_qty}  "
            f"{'买入' if str(event.order_side) == 'BUY' else '卖出'}  "
            f"佣金={event.commission}"
        )
        self._pub_order("FILLED", event)
        # 成交后延迟 3s 同步账户余额（等 IBKR 更新保证金）
        self._trigger_account_sync(delay=3.0)

        # BRACKET：入场单成交后自动提交止损单
        coid = event.client_order_id.value
        if coid in self._pending_sl:
            sl_order, sl_steps, sl_step_secs, instrument = self._pending_sl.pop(coid)
            self.log.info(
                f"[Gateway] BRACKET 入场单 {coid} 已成交，"
                f"提交止损单 {sl_order.client_order_id} @ {sl_order.trigger_price}"
            )
            self.submit_order(sl_order)
            # 止损价定时修改任务
            for i, new_sl in enumerate(sl_steps):
                delay = sl_step_secs * (i + 1)
                task = asyncio.ensure_future(
                    self._schedule_sl_modify(
                        sl_order_id=sl_order.client_order_id.value,
                        instrument=instrument,
                        new_trigger_price=Decimal(str(new_sl)),
                        delay_secs=delay,
                        step_index=i + 1,
                    )
                )
                self._sl_tasks.append(task)

    def on_order_partially_filled(self, event) -> None:
        """订单部分成交"""
        self.log.info(
            f"[Order] 🔶 PARTIALLY_FILLED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}  "
            f"成交价={event.last_px}  "
            f"本次={event.last_qty}  "
            f"累计={event.filled_qty}  "
            f"剩余={event.leaves_qty}"
        )
        self._pub_order("PARTIALLY_FILLED", event)
        # 部分成交后也更新余额
        self._trigger_account_sync(delay=3.0)

    def on_order_canceled(self, event) -> None:
        """订单已取消"""
        self.log.info(
            f"[Order] ⛔ CANCELED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )
        self._pub_order("CANCELED", event)
        # 撤单后更新余额（释放资金）
        self._trigger_account_sync(delay=2.0)

    def on_order_expired(self, event) -> None:
        """订单已过期（DAY 单收市未成交）"""
        self.log.warning(
            f"[Order] ⌛ EXPIRED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )
        self._pub_order("EXPIRED", event)

    async def _schedule_sl_modify(
        self,
        sl_order_id: str,
        instrument,
        new_trigger_price: Decimal,
        delay_secs: int,
        step_index: int,
    ) -> None:
        """定时修改止损单触发价（实现止损价定时移动）"""
        self.log.info(
            f"[Gateway] 止损修改 Step {step_index} —— "
            f"将在 {delay_secs}s 后把止损价改为 {new_trigger_price}"
        )
        await asyncio.sleep(delay_secs)

        order = self.cache.order(ClientOrderId(sl_order_id))
        if order is None:
            self.log.error(f"[Gateway] 止损单 {sl_order_id} 不在缓存中，跳过修改")
            return
        if not order.is_open:
            self.log.warning(
                f"[Gateway] 止损单 {sl_order_id} 已不活跃（可能已触发或被取消），跳过修改"
            )
            return

        price_obj = instrument.make_price(new_trigger_price)
        self.log.info(
            f"[Gateway] ✅ 止损价移动 Step {step_index}: "
            f"{sl_order_id} → trigger_price={price_obj}"
        )
        self.modify_order(
            order=order,
            quantity=order.quantity,
            trigger_price=price_obj,
        )


    # ------------------------------------------------------------------
    # HTTP Server + 跨线程桥接到 MessageBus
    # ------------------------------------------------------------------

    async def _async_bridge(self, data: dict) -> None:
        """
        在引擎 asyncio 事件循环中执行：
        构造 ExternalOrderCommand 并发布到 MessageBus
        """
        try:
            event = ExternalOrderCommand(
                instrument_id=data["instrument_id"],
                side=data["side"],
                qty=int(data["qty"]),
                order_type=data.get("order_type", "MARKET"),
                price=data.get("price"),
                stop_loss=data.get("stop_loss"),
                sl_steps=data.get("sl_steps", []),
                sl_step_secs=int(data.get("sl_step_secs", 60)),
            )
            # ★ 标准 MessageBus 发布
            self.msgbus.publish(
                topic=ExternalOrderCommand.TOPIC,
                msg=event,
            )
        except Exception as e:
            self.log.error(f"[Gateway] 消息发布失败: {e}")

    async def _async_bridge_settings(self, data: dict) -> None:
        """转发设置变更到 MessageBus"""
        try:
            event = STTrailSettingsEvent(
                symbol=data["symbol"],
                active=bool(data["active"])
            )
            self.msgbus.publish(topic="settings.st_trail", msg=event)
        except Exception as e:
            self.log.error(f"[Gateway] 设置发布失败: {e}")

    def _start_http_server(self) -> None:
        """在守护线程中启动 HTTP 网关"""
        loop = self._loop
        publish_fn = self._async_bridge  # 使用 self 的 coroutine
        actor = self                     # 闭包中显式引用 actor，避免 _Handler 内 self 覆盖

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                """GET /account —— 账户余额；GET /positions —— 当前仓位；GET /active-orders —— 活跃订单"""
                if self.path == "/account":
                    self._send(200, actor.get_account_info())
                elif self.path == "/positions":
                    self._send(200, actor.get_positions())
                elif self.path == "/active-orders":
                    self._send(200, actor.get_active_orders())
                else:
                    self._send(404, {"error": f"未知路径: {self.path}"})

            def do_POST(self) -> None:
                # P4: Token 认证（设置环境变量 ORDER_GATEWAY_SECRET 启用）
                import os as _os
                _secret = _os.environ.get("ORDER_GATEWAY_SECRET", "")
                if _secret and self.headers.get("X-Order-Token") != _secret:
                    self._send(403, {"error": "Unauthorized: invalid X-Order-Token"})
                    return

                if self.path == "/settings":
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n))
                        asyncio.run_coroutine_threadsafe(actor._async_bridge_settings(data), loop)
                        self._send(200, {"status": "ok"})
                    except Exception as e:
                        self._send(400, {"error": str(e)})
                    return

                if self.path != "/order":
                    self._send(404, {"error": "请使用 POST /order"})
                    return
                try:
                    n = int(self.headers.get("Content-Length", 0))
                    data = json.loads(self.rfile.read(n))
                except Exception as e:
                    self._send(400, {"error": f"JSON 解析失败: {e}"})
                    return

                for f in ("instrument_id", "side", "qty"):
                    if f not in data:
                        self._send(400, {"error": f"缺少字段: {f}"})
                        return

                # 跨线程安全：将协程调度到引擎事件循环
                asyncio.run_coroutine_threadsafe(publish_fn(data), loop)
                print(
                    f"[HTTP] ← POST /order  {data.get('side')} {data.get('qty')} "
                    f"{data.get('instrument_id')}  type={data.get('order_type','MARKET')}  "
                    f"stop_loss={data.get('stop_loss')}",
                    flush=True,
                )
                self._send(200, {"status": "accepted", "message": str(data)})

            def _send(self, code: int, body: dict) -> None:
                payload = json.dumps(body, ensure_ascii=False).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, fmt, *args) -> None:
                pass  # 静默 HTTP 访问日志

        self._http_server = HTTPServer(
            (self.config.http_host, self.config.http_port), _Handler
        )
        threading.Thread(
            target=self._http_server.serve_forever,
            daemon=True,
            name="OrderGatewayHTTP",
        ).start()
        fa_info = f" | FA Group={self.config.fa_group}" if self.config.fa_group else ""
        self.log.info(
            f"[Gateway] HTTP Server 已启动: "
            f"http://{self.config.http_host}:{self.config.http_port}/order{fa_info}"
        )

    def _fa_tags(self, extra_fields: dict | None = None) -> list[str] | None:
        """
        构造 IBOrderTags 标签字符串，将 FA 分配字段（faGroup/faMethod）与额外字段合并
        为单个 'IBOrderTags:{...}' 字符串（execution.py 只解析第一个 IBOrderTags tag）。

        Parameters
        ----------
        extra_fields : dict, optional
            额外的 IBOrderTags 字段，如 {'ocaGroup': 'BKT-xxx', 'ocaType': 2}
        """
        payload = {}

        # 先填入 FA 分配字段
        if self.config.fa_group:
            payload["faGroup"] = self.config.fa_group
            payload["faMethod"] = self.config.fa_method

        # 合并额外字段（如 ocaGroup/ocaType）
        if extra_fields:
            payload.update(extra_fields)

        if not payload:
            return None

        tag_str = f"IBOrderTags:{json.dumps(payload)}"
        return [tag_str]   # order_factory 的 tags 参数要求 list[str]




