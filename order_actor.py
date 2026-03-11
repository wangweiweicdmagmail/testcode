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
from events import STTrailSettingsEvent, EMATrailSettingsEvent, TERMINAL_STATUS

# 订单终态集合（全局复用，来自 events.py）
TERMINAL = TERMINAL_STATUS


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
        order_type: str = "MARKET",  # "MARKET" | "LIMIT" | "BRACKET" | "LIMIT_BRACKET"
        price: float | None = None,
        stop_loss: float | None = None,    # BRACKET/LIMIT_BRACKET: 止损触发价
        sl_steps: list | None = None,     # BRACKET: 依次修改的止损价列表 [602, 603]
        sl_step_secs: int = 60,           # BRACKET: 两次修改的间隔秒数
        # ── LIMIT_BRACKET 新增字段 ───────────────────────────────
        tp_price: float | None = None,     # 半仓止盈限价
        tp_qty: int | None = None,         # 止盈股数（通常为 floor(qty/2)）
        entry_price: float | None = None,  # 入场价（止盈触发后用于将止损移到保本）
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
        # LIMIT_BRACKET 字段
        self.tp_price = tp_price
        self.tp_qty = int(tp_qty) if tp_qty is not None else None
        self.entry_price = entry_price  # 备忘入场价，止盈触发后将剩余半仓止损移到此价

    def __repr__(self) -> str:
        return (
            f"ExternalOrderCommand("
            f"{self.order_type} {self.side} {self.qty}x {self.instrument_id}"
            f"{f' @ {self.price}' if self.price else ''}"
            f"{f' SL={self.stop_loss}' if self.stop_loss else ''}"
            f"{f' TP={self.tp_price}x{self.tp_qty}' if self.tp_price else ''})"
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
    fa_group_accounts : tuple[str, ...]
        dt_test FA Group 包含的真实子账户 ID（如 DU123456），
        用于精确读取该 Group 的专属余额，而非 FA 主账号总资产。
        ⚠️ 请替换为真实 DU 账号，账号可在 TWS → Allocation → FA Groups 查看。
        留空则汇总 IBKR 返回的该 Group 所有子账户数据。
    """
    http_host: str = "localhost"
    http_port: int = 8888
    fa_group: str = ""         # 留空则不使用 FA
    fa_method: str = "EqualQuantity"
    fa_group_accounts: tuple[str, ...] = ()  # dt_test Group 子账户列表，如 ("DU111111", "DU222222")


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
        self._sl_tasks: list[asyncio.Task] = []  # 保存 task 引用，避免被 GC 或引擎停止时静默取消
        self._redis: "_redis_lib.Redis | None" = None  # Redis 推送客户端
        # 待提交的止损单：{entry_client_order_id: (sl_order, sl_tasks_params)}
        # 入场单成交后在 on_order_filled 里提交
        self._pending_sl: dict = {}
        # FA Group 专属账户查询线程控制
        self._fa_accounts_running: bool = False
        # 心跳线程控制（在 __init__ 初始化，防止 on_stop 在 on_start 前被调用时 AttributeError）
        self._heartbeat_running: bool = False
        # 跟踪止损开关状态（sym -> bool）
        self._st_trail_active: dict[str, bool] = {}
        self._ema_trail_active: dict[str, bool] = {}
        # ── LIMIT_BRACKET 止盈/止盈联动字典 ──────────────────────
        # tp_coid -> (sl_coid, entry_price, instrument, sl_remaining_qty)
        # 止盈单成交时：修改 SL 单（数量减半 + 止损移至入场价）
        self._pending_tp: dict[str, tuple] = {}
        # sl_coid -> tp_coid（止损单成交时：反向取消止盈单）
        self._sl_to_tp:   dict[str, str]   = {}


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

            # ★ 路读 MessageBus 订阅：外部下单 + 跟踪止损开关 + K 线收盘
            self.msgbus.subscribe(
                topic=ExternalOrderCommand.TOPIC,
                handler=self.on_external_order_command,
            )
            self.msgbus.subscribe(
                topic="settings.st_trail",
                handler=self._on_st_trail_change,
            )
            self.msgbus.subscribe(
                topic="settings.ema_trail",
                handler=self._on_ema_trail_change,
            )
            self.msgbus.subscribe(
                topic="bar.collected",
                handler=self._on_bar_collected_trail,
            )
            self.log.info("[Gateway] 已订阅 ExternalOrderCommand / st_trail / ema_trail / bar.collected")

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

        # 启动心跳线程（每 5s 向 Redis 发布 engine:heartbeat）
        self._heartbeat_running = True
        _hb_t = threading.Thread(target=self._heartbeat_loop, daemon=True)
        _hb_t.start()

        # 启动 FA Group 专属账户查询线程（延迟 15s 等待 IB 连接稳定）
        self._fa_accounts_running = True
        threading.Thread(
            target=self._fa_group_account_loop,
            daemon=True,
            name="FAGroupAccountQuery",
        ).start()

    def on_stop(self) -> None:
        """停止：取消订阅 + 关闭 HTTP Server + 取消止损修改 tasks"""
        self.msgbus.unsubscribe(
            topic=ExternalOrderCommand.TOPIC,
            handler=self.on_external_order_command,
        )
        self.msgbus.unsubscribe(topic="settings.st_trail",  handler=self._on_st_trail_change)
        self.msgbus.unsubscribe(topic="settings.ema_trail", handler=self._on_ema_trail_change)
        self.msgbus.unsubscribe(topic="bar.collected",      handler=self._on_bar_collected_trail)

        # 取消未完成的止损修改计划
        for t in self._sl_tasks:
            if not t.done():
                t.cancel()
                self.log.warning(f"[Gateway] 止损修改 task 已取消: {t}")
        self._sl_tasks.clear()

        # 停止心跳 & FA Group 账户查询
        self._heartbeat_running = False
        self._fa_accounts_running = False

        if self._redis:
            self._redis.close()

        # 关闭 HTTP 网关（在守护线程中执行，避免阻塞引擎停止）
        if self._http_server:
            threading.Thread(
                target=self._http_server.shutdown, daemon=True
            ).start()

        self.log.info("[Gateway] OrderGatewayActor 已停止")

    # ------------------------------------------------------------------
    # ★ 跟踪止损逻辑（直接在 OrderGatewayActor 内实现，可访问自己的 cache.orders()）
    # ------------------------------------------------------------------

    def _on_st_trail_change(self, event) -> None:
        """ST 跟踪止损开关变更"""
        self._st_trail_active[event.symbol] = event.active
        status = "开启 ✓" if event.active else "关闭"
        self.log.info(
            f"[Trail] {event.symbol} ST 跟踪止损已{status}"
        )

    def _on_ema_trail_change(self, event) -> None:
        """EMA21 跟踪止损开关变更"""
        self._ema_trail_active[event.symbol] = event.active
        status = "开启 ✓" if event.active else "关闭"
        self.log.info(
            f"[Trail] {event.symbol} EMA21 跟踪止损已{status}"
        )

    def _on_bar_collected_trail(self, event) -> None:
        """
        M1 K 线收盘事件 - 跟踪止损逻辑入口。
        在 OrderGatewayActor 内执行，可直接访问自己提交的止损单。

        开关状态从 Redis settings:{sym} 读取（server.js 在用户切换开关时写入），
        引擎重启后自动恢复，无需重新点击界面开关。
        """
        sym = event.symbol

        # ── 从 Redis 读取开关状态（Source of Truth）──────────────────
        st_active = ema_active = False
        if self._redis:
            try:
                raw = self._redis.get(f"settings:{sym}")
                if raw:
                    settings = json.loads(raw)
                    st_active  = bool(settings.get("st_trail",  False))
                    ema_active = bool(settings.get("ema_trail", False))
            except Exception:
                # Redis 不可用时，降级到内存开关
                st_active  = self._st_trail_active.get(sym, False)
                ema_active = self._ema_trail_active.get(sym, False)
        else:
            # 无 Redis 时使用内存开关
            st_active  = self._st_trail_active.get(sym, False)
            ema_active = self._ema_trail_active.get(sym, False)

        if not st_active and not ema_active:
            return

        bar = event.bar
        instrument_id_str = bar.get("instrument_id", "")
        if not instrument_id_str:
            return

        try:
            from nautilus_trader.model.identifiers import InstrumentId as _InstrumentId
            instrument_id = _InstrumentId.from_str(instrument_id_str)
        except Exception as e:
            self.log.error(f"[Trail] {sym}: instrument_id 解析失败: {e}")
            return

        # 找开放仓位
        open_positions = [
            p for p in self.cache.positions_open()
            if p.instrument_id == instrument_id
        ]
        if not open_positions:
            return


        for pos in open_positions:
            # ―― 无条件计算 ST 候选値 ―――――――――――――――――――――
            st_val = float(bar.get("st_value", 0.0))
            st_dir = int(bar.get("st_dir", 0))
            st_candidate = None
            if st_val:
                if pos.is_long and st_dir == 1:
                    st_candidate = st_val
                elif not pos.is_long and st_dir == -1:
                    st_candidate = st_val

            # ―― 无条件计算 EMA21 M5 候选値 ――――――――――――――――
            ema_candidate = None
            if self._redis:
                try:
                    raw_ema = self._redis.lindex(f"bars:5m:{sym}", -1)
                    if raw_ema:
                        m5bar = json.loads(raw_ema)
                        ema21 = m5bar.get("ema21")
                        if ema21 is not None:
                            ema_candidate = float(ema21)
                except Exception:
                    pass

            # ―― 找到活跃的 STOP_MARKET 止损单 ―――――――――――――――
            stop_order = None
            for order in self.cache.orders():
                if order.instrument_id != instrument_id:
                    continue
                status_name = getattr(order.status,     "name", "")
                type_name   = getattr(order.order_type, "name", "")
                if status_name in TERMINAL:
                    continue
                if type_name == "STOP_MARKET":
                    stop_order = order
                    break

            if stop_order is None:
                self.log.debug(f"[Trail] {sym}: 无活跃止损单，跳过")
                continue

            current_sl = float(stop_order.trigger_price)

            # ―― 根据开关筛选候选（开关在此才起效）―――――――――
            candidate_sls = []
            if st_active and st_candidate is not None:
                candidate_sls.append(("ST", st_candidate))
            if ema_active and ema_candidate is not None:
                candidate_sls.append(("EMA", ema_candidate))

            if not candidate_sls:
                continue

            if pos.is_long:
                best_name, best_sl = max(candidate_sls, key=lambda x: x[1])
                new_sl = max(current_sl, best_sl)
            else:
                best_name, best_sl = min(candidate_sls, key=lambda x: x[1])
                new_sl = min(current_sl, best_sl)

            if abs(new_sl - current_sl) < 0.01:
                continue

            direction = "多" if pos.is_long else "空"
            candidates_str = [(n, f"{v:.2f}") for n, v in candidate_sls]
            self.log.info(
                f"[Trail] {sym}: [{best_name}] 止损价 {current_sl:.2f} → {new_sl:.2f}  "
                f"({direction}头)  "
                f"候选: {candidates_str}"
            )

            # 执行改单
            try:
                instrument = self.cache.instrument(instrument_id)
                if instrument is None:
                    self.log.error(f"[Trail] {sym}: 合约未加载")
                    continue
                new_trigger_price = instrument.make_price(new_sl)
                self.modify_order(
                    order=stop_order,
                    quantity=stop_order.quantity,
                    trigger_price=new_trigger_price,
                )
                self.log.info(
                    f"[Trail] {sym}: ✓ modify_order 已提交  "
                    f"触发价={new_trigger_price}  订单={stop_order.client_order_id}"
                )
                # 同步更新 Redis order:stop 中的 trigger_price
                if self._redis:
                    try:
                        stored = self._redis.get(f"order:stop:{sym}")
                        if stored:
                            data = json.loads(stored)
                            data["trigger_price"] = str(new_sl)
                            self._redis.set(f"order:stop:{sym}", json.dumps(data))
                    except Exception:
                        pass
            except Exception as e:
                self.log.error(f"[Trail] {sym}: modify_order 失败: {e}")

    # ------------------------------------------------------------------
    # 引擎心跳（每 5s 向 Redis 发布 engine:heartbeat 供前端显示状态）
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """后台线程：每 5 秒发布一次心跳消息到 Redis"""
        while self._heartbeat_running:
            try:
                if self._redis:
                    self._redis.publish("engine:heartbeat", json.dumps({
                        "ts": int(time.time()),
                        "status": "alive",
                    }))
            except Exception:
                pass
            # 分段 sleep，使停止时能快速响应
            for _ in range(50):  # 50 × 0.1s = 5s
                if not self._heartbeat_running:
                    return
                time.sleep(0.1)

    # ------------------------------------------------------------------
    # 账户余额同步（FA Group 余额 → Redis）
    # 使用引擎标准回调 on_account_state，IB 每次推送账户数据后自动触发
    # ------------------------------------------------------------------

    def on_account_state(self, event) -> None:
        """
        引擎标准回调：IB 推送 AccountState 后自动触发。
        
        注意：IBKR FA 账户下，cache 中的账户对象对应 FA 主账号（F10251881），
        其 balance 是所有 FA Group 的汇总，不是 dt_test 专属余额。
        因此这里仅记录日志，实际余额由 _fa_group_account_loop 通过独立连接获取。
        """
        try:
            from nautilus_trader.model.currencies import USD
            accounts = self.cache.accounts()
            if not accounts:
                return
            self.log.debug(
                f"[Account] on_account_state 触发（主账号汇总，仅供参考）: "
                f"{[str(a.id) for a in accounts]}"
            )
        except Exception as e:
            self.log.debug(f"[Account] on_account_state 处理异常: {e}")

    # ------------------------------------------------------------------
    # FA Group 专属账户查询（独立 ibapi 连接，绕过主账号汇总问题）
    # ------------------------------------------------------------------

    def _fa_group_account_loop(self) -> None:
        """
        后台线程：使用独立的 ibapi 轻量连接，每 3 分钟查询一次
        dt_test FA Group 的专属余额，避免与主账号（F10251881）总资产混淆。

        原理：
          NautilusTrader 主引擎配置 account_id='F10251881'，
          cache.accounts() 只返回主账号（balance = 所有 FA Group 之和）。
          通过 reqAccountSummary(group='dt_test') 可以直接拿到该 Group 专属余额。
        """
        import os as _os

        # 等待主引擎连接稳定
        time.sleep(15)
        self.log.info("[Account-FA] FA Group 账户查询线程启动")

        ibg_host = _os.environ.get("IBG_HOST", "127.0.0.1")
        ibg_port = int(_os.environ.get("IBG_PORT", "7496"))
        # 使用高编号 client_id 避免与主引擎冲突（DataClient=2, ExecClient=3, FAQuery=102）
        aux_client_id = int(_os.environ.get("IBG_CLIENT_ID", "2")) + 100
        fa_group = self.config.fa_group or "dt_test"

        # 查询间隔：3 分钟（与 IBKR 推送频率对齐，分段 sleep 支持快速停止）
        POLL_INTERVAL = 180
        SLEEP_UNIT   = 0.5

        while self._fa_accounts_running:
            try:
                self._query_fa_group_once(ibg_host, ibg_port, aux_client_id, fa_group)
            except Exception as e:
                self.log.warning(f"[Account-FA] 查询异常，将在下次重试: {e}")

            # 分段 sleep，支持快速响应停止信号
            for _ in range(int(POLL_INTERVAL / SLEEP_UNIT)):
                if not self._fa_accounts_running:
                    return
                time.sleep(SLEEP_UNIT)

        self.log.info("[Account-FA] FA Group 账户查询线程已停止")

    def _query_fa_group_once(self, host: str, port: int, client_id: int, fa_group: str) -> None:
        """
        建立临时 ibapi 连接，发起 reqAccountSummary(group=fa_group)，
        收集子账户余额后写入 Redis，然后断开连接。
        """
        try:
            from ibapi.client import EClient
            from ibapi.wrapper import EWrapper as _EWrapper
        except ImportError:
            self.log.warning("[Account-FA] ibapi 包未安装，无法查询 FA Group 余额")
            return

        # 内部 EWrapper：仅收集 accountSummary 回调
        class _SummaryWrapper(_EWrapper):
            def __init__(self):
                super().__init__()
                # {account_id: {"TotalCashValue": float, "AvailableFunds": float}}
                self.sub_accounts: dict = {}
                self._done = threading.Event()
                self._conn_error: str = ""

            def accountSummary(self, reqId, account, tag, value, currency):
                """每条子账户摘要数据回调"""
                if currency != "USD":
                    return
                if tag not in ("NetLiquidation", "AvailableFunds"):
                    return
                if account not in self.sub_accounts:
                    self.sub_accounts[account] = {}
                try:
                    self.sub_accounts[account][tag] = float(value)
                except ValueError:
                    pass

            def accountSummaryEnd(self, reqId):
                """所有数据推送完毕"""
                self._done.set()

            def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
                """连接或请求出错"""
                # errorCode=502/504=无法连接，504=未连接，忽略 -1（连接状态通知）
                if reqId == -1:
                    return  # 连接级别的通知，非错误
                self._conn_error = f"code={errorCode} {errorString}"
                self._done.set()

        wrapper = _SummaryWrapper()
        client = EClient(wrapper)
        try:
            client.connect(host, port, client_id)
        except Exception as e:
            self.log.warning(f"[Account-FA] ibapi 连接失败 ({host}:{port} clientId={client_id}): {e}")
            return

        # 启动消息循环线程
        msg_thread = threading.Thread(target=client.run, daemon=True)
        msg_thread.start()

        # 等待连接就绪（最多 5s）
        time.sleep(2)
        if not client.isConnected():
            self.log.warning(f"[Account-FA] ibapi 连接未就绪，跳过本次查询")
            try:
                client.disconnect()
            except Exception:
                pass
            return

        # 发起 FA Group accountSummary 请求
        REQ_ID = 9001  # 专用请求 ID，不与主引擎 reqId 冲突
        client.reqAccountSummary(REQ_ID, fa_group, "NetLiquidation,AvailableFunds")

        # 等待数据返回（最多 15s）
        wrapper._done.wait(timeout=15)

        if wrapper._conn_error:
            self.log.warning(f"[Account-FA] reqAccountSummary 出错: {wrapper._conn_error}")
        elif not wrapper.sub_accounts:
            self.log.warning(f"[Account-FA] FA Group '{fa_group}' 无子账户数据返回（检查 Group 名称是否正确）")
        else:
            # IBKR reqAccountSummary(group=fa_group) 本身只返回该 Group 的账户
            # 无需额外过滤，直接汇总所有返回账户的 USD 余额
            total_usd = 0.0
            free_usd  = 0.0
            sub_ids   = []
            for acct_id, tags in wrapper.sub_accounts.items():
                tv = tags.get("NetLiquidation", 0.0)  # 净资产（持仓市值 + 现金）
                fv = tags.get("AvailableFunds",  0.0)  # 剩余流动性
                total_usd += tv
                free_usd  += fv
                sub_ids.append(acct_id)
                self.log.info(
                    f"[Account-FA]   └─ {acct_id}  "
                    f"TotalCash={tv:,.2f}  AvailFunds={fv:,.2f}  USD"
                )

            locked_usd = total_usd - free_usd
            self.log.info(
                f"[Account-FA] FA Group '{fa_group}' 余额汇总: "
                f"total={total_usd:,.2f}  free={free_usd:,.2f}  "
                f"locked={locked_usd:,.2f}  USD | 子账户={sub_ids}"
            )

            balances = [{
                "currency": "USD",
                "total":  round(total_usd,  2),
                "free":   round(free_usd,   2),
                "locked": round(locked_usd, 2),
            }]
            self._write_account_to_redis(fa_group, balances)

        try:
            client.cancelAccountSummary(REQ_ID)
            client.disconnect()
        except Exception:
            pass


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
            from nautilus_trader.model.currencies import USD
            accounts = self.cache.accounts()
            if not accounts:
                return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}
            # 取 USD balance 最大的账户作为 FA 主账户
            best_account = None
            best_total = -1.0
            for acct in accounts:
                try:
                    t = acct.balance_total(USD)
                    v = float(t.as_double()) if t else 0.0
                    if v > best_total:
                        best_total = v
                        best_account = acct
                except Exception:
                    continue
            if best_account is None:
                return {"total_equity": 0.0, "available_cash": 0.0, "currency": "USD"}
            total = best_account.balance_total(USD)
            free  = best_account.balance_free(USD)
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
        """查询 cache 中活跃的止损单和已打开的仓位，组合成前端可用的价格线数据"""
        result = {}
        # 终态（不再活跃）
        TERMINAL_STATUS = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}
        try:
            # 1. 已打开的仓位 → 入场价
            open_positions = list(self.cache.positions_open())
            self.log.info(f"[SL诊断] positions_open 数量={len(open_positions)}")
            for pos in open_positions:
                sym = pos.instrument_id.symbol.value
                # ★ 二次校验：用 portfolio.net_position 确认净仓位不为 0
                # portfolio 比 cache 更实时，能更快感知 TWS 外部平仓
                try:
                    net_qty = self.portfolio.net_position(pos.instrument_id)
                    if net_qty == 0:
                        self.log.warning(
                            f"[SL诊断] ⚠️ {sym} cache 有仓位但 portfolio.net_position=0，"
                            f"可能已被外部平仓，跳过入场线显示"
                        )
                        continue
                except Exception:
                    pass  # 若 portfolio 查询失败，仍显示 cache 数据
                result.setdefault(sym, {})
                result[sym]["entry"] = {
                    "price": float(pos.avg_px_open),
                    "side": "LONG" if pos.is_long else "SHORT",
                    "quantity": float(pos.quantity),
                }
                self.log.info(f"[SL诊断] 开仓仓位 {sym}: side={result[sym]['entry']['side']} avg_px={pos.avg_px_open}")
            # 2. 使用 orders() 全量遍历，打印每个订单的实际 order_type，定位匹配失败原因
            all_orders = list(self.cache.orders())
            self.log.info(f"[SL诊断] cache.orders() 总数={len(all_orders)}")
            for order in all_orders:
                raw_type_str = str(order.order_type)
                type_name = getattr(order.order_type, "name", raw_type_str)
                # 用 .name 获取状态和方向（Cython 枚举 str() 返回整数序号）
                order_status = getattr(order.status, "name", str(order.status))
                order_side = getattr(order.side, "name", str(order.side))
                self.log.info(
                    f"[SL诊断]   订单 {order.client_order_id}: "
                    f"type.name={type_name!r}  status.name={order_status!r}  side.name={order_side!r}"
                )
                if type_name != "STOP_MARKET":
                    continue
                if order_status in TERMINAL_STATUS:
                    self.log.info(f"[SL诊断]   └→ STOP_MARKET 终态，跳过")
                    continue
                tp = getattr(order, "trigger_price", None)
                sym = order.instrument_id.symbol.value
                self.log.info(
                    f"[SL诊断] ✅ 活跃 STOP_MARKET {sym}: "
                    f"status={order_status} trigger_price={tp} "
                    f"client_order_id={order.client_order_id} qty={order.quantity}"
                )
                result.setdefault(sym, {})
                result[sym]["stop_loss"] = {
                    "price": float(tp) if tp else None,
                    "side": order_side,             # 已用 .name
                    "quantity": float(order.quantity),
                    "client_order_id": str(order.client_order_id),
                    "status": order_status,         # 已用 .name
                }
                self.log.info(f"[SL诊断]   └→ 写入 result[{sym}]['stop_loss'] price={float(tp) if tp else None}")
        except Exception as e:
            self.log.warning(f"[Gateway] _async_get_active_orders 失败: {e}")
        self.log.info(f"[SL诊断] active_orders 返回: {result}")
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

        elif event.order_type == "LIMIT_BRACKET":
            # 限价入场 + 全仓止损 + 半仓止盈
            # 入场单成交后才依次提交 SL/TP，避免挑价单与止损单互刻
            if event.price is None:
                self.log.error("[Gateway] LIMIT_BRACKET 单必须提供 price")
                return
            if event.stop_loss is None:
                self.log.error("[Gateway] LIMIT_BRACKET 单必须提供 stop_loss")
                return
            if event.tp_price is None or event.tp_qty is None:
                self.log.error("[Gateway] LIMIT_BRACKET 单必须提供 tp_price 和 tp_qty")
                return

            # 入场单（LIMIT，GTC）
            entry_order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                price=instrument.make_price(Decimal(str(event.price))),
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(),
            )

            # 止损单（全仓 STOP_MARKET）
            sl_side = OrderSide.SELL if order_side == OrderSide.BUY else OrderSide.BUY
            sl_order = self.order_factory.stop_market(
                instrument_id=instrument_id,
                order_side=sl_side,
                quantity=quantity,
                trigger_price=instrument.make_price(Decimal(str(event.stop_loss))),
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(),
            )

            # 止盈单（半仓 LIMIT）
            tp_side = sl_side  # 平仓方向和止损相同
            tp_qty  = instrument.make_qty(Decimal(event.tp_qty))
            tp_order = self.order_factory.limit(
                instrument_id=instrument_id,
                order_side=tp_side,
                quantity=tp_qty,
                price=instrument.make_price(Decimal(str(event.tp_price))),
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(),
            )

            entry_price = event.entry_price or event.price  # 备忘入场价，止盈触发后用于保本
            # 将 SL + TP + entry_price 存入待提交字典
            self._pending_sl[entry_order.client_order_id.value] = (
                sl_order,
                tp_order,
                entry_price,
                instrument,
            )

            # 只提交入场单
            self.submit_order(entry_order)
            self.log.info(
                f"[Gateway] LIMIT_BRACKET 入场单已提交 | "
                f"Entry={entry_order.client_order_id} LIMIT {order_side.name} qty={quantity} "
                f"price={event.price} {instrument_id} | "
                f"止损={event.stop_loss} | 止盈={event.tp_price}x{event.tp_qty} | "
                f"入场单成交后自动提交 SL/TP"
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
                    msg["side"] = order.side.name   # BUY / SELL（.name 而非 str()，避免取到整数值）
                    msg["symbol"] = str(order.instrument_id).split(".")[0]
                    msg["order_type"] = order.order_type.name  # MARKET / STOP_MARKET 等
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
            # 对重要状态打印完整消息供调试
            if status in ("ACCEPTED", "UPDATED", "CANCELED", "FILLED", "TRIGGERED"):
                self.log.info(f"[Gateway] 发布 order:update {status}: {msg}")
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
        # 尝试从 cache 取到该订单的详细信息加入日志
        try:
            order = self.cache.order(event.client_order_id)
            # 统一用 .name 获取枚举名，避免 Cython 枚举 str() 返回整数序号
            order_type = getattr(order.order_type, "name", "") if order else ""
            tp = getattr(order, "trigger_price", None) if order else None
            if order_type == "STOP_MARKET":
                self.log.info(
                    f"[SL] ✅ 止损单 ACCEPTED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}  "
                    f"trigger_price={tp}"
                )
                # ── 方案A：持久化止损单 ID 到 Redis ───────────────────────
                # 引擎重启后 IBKR 会重新推送 ACCEPTED 事件，届时 cache 中重新有该订单
                # 同时也确保手动拖动止损（_async_modify_stop）能在重启后找到正确订单
                if order:
                    sym = order.instrument_id.symbol.value
                    self._persist_stop_order(sym, str(event.client_order_id), tp)
            else:
                self.log.info(
                    f"[Order] ✅ ACCEPTED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}"
                )
        except Exception:
            self.log.info(
                f"[Order] ✅ ACCEPTED  "
                f"ClientOrderId={event.client_order_id}  "
                f"VenueOrderId={event.venue_order_id}"
            )
        self._pub_order("ACCEPTED", event)


    def _persist_stop_order(self, sym: str, client_order_id: str, trigger_price) -> None:
        """将止损单 ID 写入 Redis order:stop:{sym}，供引擎重启后恢复。"""
        if not self._redis:
            return
        try:
            data = {
                "client_order_id": client_order_id,
                "trigger_price": str(trigger_price) if trigger_price else None,
                "symbol": sym,
            }
            self._redis.set(f"order:stop:{sym}", json.dumps(data))
            self.log.info(f"[SL] 止损单 ID 已持久化到 Redis: order:stop:{sym} = {client_order_id}")
        except Exception as e:
            self.log.warning(f"[SL] 持久化止损单 ID 失败: {e}")

    def _clear_stop_order(self, sym: str, client_order_id: str) -> None:
        """止损单终结（成交/撤单/过期）时清除 Redis 记录。"""
        if not self._redis:
            return
        try:
            stored = self._redis.get(f"order:stop:{sym}")
            if stored:
                data = json.loads(stored)
                if data.get("client_order_id") == client_order_id:
                    self._redis.delete(f"order:stop:{sym}")
                    self.log.info(f"[SL] 止损单已终结，清除 Redis 记录: order:stop:{sym}")
        except Exception as e:
            self.log.warning(f"[SL] 清除止损单 Redis 记录失败: {e}")

    def on_order_pending_update(self, event) -> None:
        """改单请求已发出，等待交易所响应"""
        self.log.info(
            f"[Order] ⏳ PENDING_UPDATE  "
            f"ClientOrderId={event.client_order_id}"
        )

    def on_order_updated(self, event) -> None:
        """改单成功（止损价移动等）"""
        # 尝试从 cache 取 trigger_price
        try:
            order = self.cache.order(event.client_order_id)
            order_type = getattr(order.order_type, "name", "UNKNOWN") if order else "UNKNOWN"
            tp = getattr(order, "trigger_price", None) if order else None
            new_price = getattr(event, "price", None)
            new_tp = getattr(event, "trigger_price", None)
            if order_type == "STOP_MARKET":
                self.log.info(
                    f"[SL] ✅ 止损单 UPDATED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}  "
                    f"event.trigger_price={new_tp}  cache.trigger_price={tp}"
                )
            else:
                self.log.info(
                    f"[Order] ✅ UPDATED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}  "
                    f"price={new_price}"
                )
        except Exception:
            self.log.info(
                f"[Order] ✅ UPDATED  "
                f"ClientOrderId={event.client_order_id}  "
                f"VenueOrderId={event.venue_order_id}"
            )
        # IBKR 止损单通常在 ACCEPTED 之前先推一个 UPDATED 事件（含 trigger_price）
        # 发布 UPDATED 消息，前端可据此更新止损价线
        self._pub_order("UPDATED", event)

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

        # 止损单成交（被触发平仓）→ 清除 Redis 持久化记录
        # 修复：原代码遗漏此处，导致重启后可能误用已成交的旧止损单 ID
        try:
            order = self.cache.order(event.client_order_id)
            if order and getattr(order.order_type, "name", "") == "STOP_MARKET":
                sym = order.instrument_id.symbol.value
                self._clear_stop_order(sym, str(event.client_order_id))
                self.log.info(f"[SL] 止损单已成交触发，清除 Redis 记录: {sym}")
        except Exception as e:
            self.log.warning(f"[SL] on_order_filled 清除止损记录失败: {e}")

        # BRACKET 或 LIMIT_BRACKET：入场单成交后自动提交止损单
        coid = event.client_order_id.value
        if coid in self._pending_sl:
            pending = self._pending_sl.pop(coid)

            if len(pending) == 4 and isinstance(pending[1], object) and \
                    getattr(pending[1], 'order_type', None) is not None:
                # ── LIMIT_BRACKET：(sl_order, tp_order, entry_price, instrument) ──
                sl_order, tp_order, entry_price, instrument = pending
                self.log.info(
                    f"[SL/TP] LIMIT_BRACKET 入场单 {coid} 已成交，"
                    f"提交止损={sl_order.client_order_id} SL@{sl_order.trigger_price} "
                    f"止盈={tp_order.client_order_id} TP@{tp_order.price}x{tp_order.quantity}"
                )
                self.submit_order(sl_order)
                self.submit_order(tp_order)
                # 双向关联：止盈单 coid ↔ 止损单 coid
                sl_coid = sl_order.client_order_id.value
                tp_coid = tp_order.client_order_id.value
                self._pending_tp[tp_coid] = (sl_coid, entry_price, instrument, sl_order.quantity)
                self._sl_to_tp[sl_coid]   = tp_coid
                self.log.info(
                    f"[SL/TP] 双向关联已建立: SL={sl_coid} ↔ TP={tp_coid}  入场价={entry_price}"
                )
            else:
                # ── 标准 BRACKET：(sl_order, sl_steps, sl_step_secs, instrument) ──
                sl_order, sl_steps, sl_step_secs, instrument = pending
                self.log.info(
                    f"[SL] BRACKET 入场单 {coid} 已成交，"
                    f"正在提交止损单 {sl_order.client_order_id} "
                    f"@ trigger_price={sl_order.trigger_price}  "
                    f"qty={sl_order.quantity}  止损设置步={sl_steps}  "
                    f"设置间隔={sl_step_secs}s"
                )
                self.submit_order(sl_order)
                self.log.info(f"[SL] submit_order 已调用，止损单={sl_order.client_order_id}")
                # 止损价定时修改任务
                for i, new_sl in enumerate(sl_steps):
                    delay = sl_step_secs * (i + 1)
                    self.log.info(f"[SL] 计划第 {i+1} 步止损修改: {new_sl} 延迟 {delay}s")
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
        else:
            # 检查是否是 LIMIT_BRACKET 的止盈单成交
            coid_str = coid

            # ─── 情况A：止盈单成交 → 取消止盈单 ──────────────────────
            if coid_str in self._sl_to_tp:
                tp_coid_to_cancel = self._sl_to_tp.pop(coid_str)
                self._pending_tp.pop(tp_coid_to_cancel, None)  # 清除止盈记录
                tp_order_to_cancel = self.cache.order(ClientOrderId(tp_coid_to_cancel))
                if tp_order_to_cancel and tp_order_to_cancel.is_open:
                    try:
                        self.cancel_order(tp_order_to_cancel)
                        self.log.info(
                            f"[SL/TP] 止损已触发，自动取消止盈单 {tp_coid_to_cancel}"
                        )
                    except Exception as e:
                        self.log.error(f"[SL/TP] 取消止盈单失败: {e}")
                else:
                    self.log.info(
                        f"[SL/TP] 止损触发，止盈单 {tp_coid_to_cancel} 已不活跃（可能已成交或撤销）"
                    )

            # ─── 情况B：止盈单成交 → 修改 SL（减半 + 保本）────────────
            elif coid_str in self._pending_tp:
                sl_coid, entry_price, instrument, orig_sl_qty = self._pending_tp.pop(coid_str)
                self._sl_to_tp.pop(sl_coid, None)  # 清除止损单反向映射
                sl_order = self.cache.order(ClientOrderId(sl_coid))
                if sl_order and sl_order.is_open:
                    try:
                        # 数量减半（剩余半仓）
                        half_qty = instrument.make_qty(
                            Decimal(str(int(orig_sl_qty) // 2))
                        )
                        # 止损价移至入场价（保本）
                        breakeven_price = instrument.make_price(
                            Decimal(str(entry_price))
                        )
                        self.modify_order(
                            order=sl_order,
                            quantity=half_qty,
                            trigger_price=breakeven_price,
                        )
                        self.log.info(
                            f"[SL/TP] 半仓止盈已触发，止损单已修改: "
                            f"数量 {orig_sl_qty} → {half_qty}，"
                            f"止损价 → 保本价 {breakeven_price}"
                        )
                        # 同步更新 Redis order:stop 记录中的价格
                        sym = sl_order.instrument_id.symbol.value
                        if self._redis:
                            try:
                                stored = self._redis.get(f"order:stop:{sym}")
                                if stored:
                                    data = json.loads(stored)
                                    data["trigger_price"] = str(entry_price)
                                    self._redis.set(f"order:stop:{sym}", json.dumps(data))
                            except Exception:
                                pass
                    except Exception as e:
                        self.log.error(
                            f"[SL/TP] 半仓止盈后修改 SL 失败: {e}  sl_coid={sl_coid}"
                        )
                else:
                    self.log.warning(
                        f"[SL/TP] 止盈已触发，但止损单 {sl_coid} 已不活跃，跳过保本修改"
                    )
            else:
                self.log.info(f"[SL] 成交单 {coid} 不在 _pending_sl 中（非 BRACKET 入场单或已处理）")


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

    def on_order_canceled(self, event) -> None:
        """订单已取消"""
        # 尝试从 cache 读取 order_type，判断是否止损单取消
        try:
            order = self.cache.order(event.client_order_id)
            order_type = getattr(order.order_type, "name", "UNKNOWN") if order else "UNKNOWN"
            if order_type == "STOP_MARKET":
                self.log.warning(
                    f"[SL] ⛔ 止损单 CANCELED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}"
                )
                # 止损单终结，清除 Redis 持久化记录
                if order:
                    sym = order.instrument_id.symbol.value
                    self._clear_stop_order(sym, str(event.client_order_id))
            else:
                self.log.info(
                    f"[Order] ⛔ CANCELED  "
                    f"ClientOrderId={event.client_order_id}  "
                    f"VenueOrderId={event.venue_order_id}  "
                    f"order_type={order_type}"
                )
        except Exception:
            self.log.info(
                f"[Order] ⛔ CANCELED  "
                f"ClientOrderId={event.client_order_id}  "
                f"VenueOrderId={event.venue_order_id}"
            )
        self._pub_order("CANCELED", event)

    def on_order_expired(self, event) -> None:
        """订单已过期（DAY 单收市未成交）"""
        self.log.warning(
            f"[Order] ⌛ EXPIRED  "
            f"ClientOrderId={event.client_order_id}  "
            f"VenueOrderId={event.venue_order_id}"
        )
        # 过期也属于终结状态，清除 Redis 记录
        try:
            order = self.cache.order(event.client_order_id)
            if order and getattr(order.order_type, "name", "") == "STOP_MARKET":
                self._clear_stop_order(order.instrument_id.symbol.value, str(event.client_order_id))
        except Exception:
            pass
        self._pub_order("EXPIRED", event)

    # ------------------------------------------------------------------
    # 仓位生命周期回调（NautilusTrader 标准接口）
    # 无论是系统下单还是 TWS 手动操作，只要仓位变化就会触发
    # ------------------------------------------------------------------

    def on_position_opened(self, event) -> None:
        """仓位开仓（新建仓位时触发）"""
        try:
            pos = self.cache.position(event.position_id)
            if pos is None:
                return
            sym = pos.instrument_id.symbol.value
            self.log.info(
                f"[Position] ✅ OPENED  {sym}  "
                f"side={'LONG' if pos.is_long else 'SHORT'}  "
                f"qty={pos.quantity}  avg_px={pos.avg_px_open}"
            )
            if self._redis:
                self._redis.publish("order:update", json.dumps({
                    "status": "POSITION_OPENED",
                    "symbol": sym,
                    "side": "LONG" if pos.is_long else "SHORT",
                    "quantity": str(pos.quantity),
                    "avg_px_open": str(pos.avg_px_open),
                    "ts": int(time.time()),
                }, ensure_ascii=False))
        except Exception as e:
            self.log.warning(f"[Position] on_position_opened 处理异常: {e}")

    def on_position_changed(self, event) -> None:
        """仓位变化（加仓、部分平仓等）"""
        try:
            pos = self.cache.position(event.position_id)
            if pos is None:
                return
            sym = pos.instrument_id.symbol.value
            self.log.info(f"[Position] 🔄 CHANGED  {sym}  qty={pos.quantity}")
            if self._redis:
                self._redis.publish("order:update", json.dumps({
                    "status": "POSITION_CHANGED",
                    "symbol": sym,
                    "side": "LONG" if pos.is_long else "SHORT",
                    "quantity": str(pos.quantity),
                    "avg_px_open": str(pos.avg_px_open),
                    "ts": int(time.time()),
                }, ensure_ascii=False))
        except Exception as e:
            self.log.warning(f"[Position] on_position_changed 处理异常: {e}")

    def on_position_closed(self, event) -> None:
        """仓位已平（无论来源——系统止损/手动平仓/止盈都触发）"""
        try:
            sym = "UNKNOWN"
            try:
                sym = str(event.instrument_id).split(".")[0]
            except Exception:
                pass
            self.log.info(f"[Position] 🏁 CLOSED  {sym}  仓位已平仓（来源：引擎/TWS 均触发）")
            if self._redis:
                self._redis.publish("order:update", json.dumps({
                    "status": "POSITION_CLOSED",
                    "symbol": sym,
                    "ts": int(time.time()),
                }, ensure_ascii=False))
        except Exception as e:
            self.log.warning(f"[Position] on_position_closed 处理异常: {e}")

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
        """转发设置变更到 MessageBus（支持 st_trail 和 ema_trail）"""
        try:
            symbol = data["symbol"]
            active = bool(data["active"])
            # ST 跟踪止损开关
            if "st_trail" in data or data.get("type") == "st_trail":
                event = STTrailSettingsEvent(symbol=symbol, active=active)
                self.msgbus.publish(topic="settings.st_trail", msg=event)
                self.log.info(f"[Gateway] ST Trail 设置已发布: {symbol} active={active}")
            # EMA21 M5 跟踪止损开关
            elif "ema_trail" in data or data.get("type") == "ema_trail":
                event = EMATrailSettingsEvent(symbol=symbol, active=active)
                self.msgbus.publish(topic="settings.ema_trail", msg=event)
                self.log.info(f"[Gateway] EMA Trail 设置已发布: {symbol} active={active}")
            else:
                self.log.warning(f"[Gateway] 未知设置类型: {data}")
        except Exception as e:
            self.log.error(f"[Gateway] 设置发布失败: {e}")

    async def _async_close_position(self, symbol: str) -> dict:
        """
        在引擎事件循环中执行真正的平仓操作：
        1. 找到该标的的开放仓位
        2. 取消所有活跃止损单（STOP_MARKET）
        3. 提交反向市价单平仓
        """
        try:
            # 找到对应 instrument_id（支持 AAPL / AAPL.NASDAQ 两种格式）
            target_pos = None
            for pos in self.cache.positions_open():
                sym = pos.instrument_id.symbol.value
                if sym == symbol:
                    target_pos = pos
                    break

            if target_pos is None:
                self.log.warning(f"[Close] 未找到 {symbol} 的开放仓位")
                return {"error": f"未找到 {symbol} 的开放仓位"}

            instrument_id = target_pos.instrument_id
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                return {"error": f"合约 {instrument_id} 未加载"}

            quantity = target_pos.quantity
            close_side = OrderSide.SELL if target_pos.is_long else OrderSide.BUY
            side_str = "SELL" if target_pos.is_long else "BUY"

            self.log.info(
                f"[Close] 平仓 {symbol}  "
                f"side={side_str}  qty={quantity}  "
                f"avg_px={target_pos.avg_px_open}"
            )

            # 1. 取消所有该标的的活跃止损单
            TERMINAL_STATUS = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}
            canceled_count = 0
            for order in self.cache.orders():
                if order.instrument_id != instrument_id:
                    continue
                order_status = getattr(order.status, "name", str(order.status))
                type_name = getattr(order.order_type, "name", str(order.order_type))
                if order_status in TERMINAL_STATUS:
                    continue
                if type_name == "STOP_MARKET":
                    self.log.info(f"[Close] 取消止损单 {order.client_order_id}")
                    self.cancel_order(order)
                    canceled_count += 1

            # 2. 提交反向市价单
            close_order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=close_side,
                quantity=quantity,
                time_in_force=TimeInForce.DAY,
                tags=self._fa_tags(),
            )
            self.submit_order(close_order)

            self.log.info(
                f"[Close] ✅ 平仓单已提交  "
                f"ClientOrderId={close_order.client_order_id}  "
                f"side={side_str}  qty={quantity}  {instrument_id}  "
                f"已取消止损单数量={canceled_count}"
            )
            return {
                "status": "accepted",
                "symbol": symbol,
                "side": side_str,
                "qty": str(quantity),
                "client_order_id": str(close_order.client_order_id),
                "canceled_stop_orders": canceled_count,
            }
        except Exception as e:
            self.log.error(f"[Close] 平仓失败: {e}")
            return {"error": str(e)}

    async def _async_modify_stop(self, symbol: str, new_price: float) -> dict:
        """
        在引擎事件循环中修改该标的的活跃止损单触发价。
        先查 cache.orders()，找不到时从 Redis order:stop:{sym} 取 client_order_id 再查。
        引擎重启后 IBKR 会重将止损单推入 cache，因此 Redis fallback 很可靠。
        """
        try:
            TERMINAL_STATUS = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}
            target_order = None
            target_instrument = None

            # ─ 先从 cache 查找 ──────────────────────────────────
            for order in self.cache.orders():
                sym = order.instrument_id.symbol.value
                if sym != symbol:
                    continue
                order_status = getattr(order.status, "name", str(order.status))
                type_name = getattr(order.order_type, "name", str(order.order_type))
                if order_status in TERMINAL_STATUS:
                    continue
                if type_name == "STOP_MARKET":
                    target_order = order
                    target_instrument = self.cache.instrument(order.instrument_id)
                    break

            # ─ cache 找不到：尝试 Redis fallback ─────────────────
            # 引擎重启后 IBKR 不一定立即推充 cache，可能需要等待一个心跳
            if target_order is None and self._redis:
                try:
                    stored = self._redis.get(f"order:stop:{symbol}")
                    if stored:
                        data = json.loads(stored)
                        coid_str = data.get("client_order_id", "")
                        if coid_str:
                            from nautilus_trader.model.identifiers import ClientOrderId
                            coid = ClientOrderId(coid_str)
                            cached = self.cache.order(coid)
                            if cached:
                                status_name = getattr(cached.status, "name", str(cached.status))
                                if status_name not in TERMINAL_STATUS:
                                    target_order = cached
                                    target_instrument = self.cache.instrument(cached.instrument_id)
                                    self.log.info(
                                        f"[ModifySL] 通过 Redis fallback 找到订单: {coid_str}"
                                    )
                            else:
                                self.log.warning(
                                    f"[ModifySL] Redis 有记录 ({coid_str}) 但 cache 尝无此订单。"
                                    f"引擎刷新后再试。"
                                )
                except Exception as e:
                    self.log.warning(f"[ModifySL] Redis fallback 失败: {e}")

            if target_order is None:
                self.log.warning(f"[ModifySL] 未找到 {symbol} 的活跃止损单")
                return {"error": f"未找到 {symbol} 的活跃止损单"}

            if target_instrument is None:
                return {"error": f"合约未加载"}

            new_tp = target_instrument.make_price(Decimal(str(new_price)))
            self.log.info(
                f"[ModifySL] 修改止损单 {target_order.client_order_id}  "
                f"{symbol}  trigger_price: {target_order.trigger_price} → {new_tp}"
            )
            self.modify_order(
                order=target_order,
                quantity=target_order.quantity,
                trigger_price=new_tp,
            )
            return {
                "status": "accepted",
                "symbol": symbol,
                "new_price": float(new_tp),
                "client_order_id": str(target_order.client_order_id),
            }
        except Exception as e:
            self.log.error(f"[ModifySL] 修改失败: {e}")
            return {"error": str(e)}

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
                elif self.path == "/debug-orders":
                    # 调试：打印 cache 里所有订单和持仓状态
                    try:
                        orders_info = []
                        for o in actor.cache.orders():
                            orders_info.append({
                                "client_order_id": str(o.client_order_id),
                                "order_type": str(o.order_type).replace("OrderType.", ""),
                                "status": str(o.status).replace("OrderStatus.", ""),
                                "symbol": str(o.instrument_id).split(".")[0],
                                "side": str(o.side).replace("OrderSide.", ""),
                                "trigger_price": str(getattr(o, "trigger_price", None)),
                                "quantity": str(o.quantity),
                            })
                        positions_info = []
                        for p in actor.cache.positions_open():
                            positions_info.append({
                                "symbol": p.instrument_id.symbol.value,
                                "side": "LONG" if p.is_long else "SHORT",
                                "qty": float(p.quantity),
                                "avg_px": float(p.avg_px_open),
                            })
                        self._send(200, {"orders": orders_info, "positions": positions_info})
                    except Exception as e:
                        self._send(500, {"error": str(e)})
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

                if self.path.startswith("/close"):
                    # POST /close  body: {"symbol": "QQQ"}
                    # 或 POST /close/QQQ（从路径中取）
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n)) if n > 0 else {}
                        # 支持路径参数 /close/QQQ 和 body {"symbol": "QQQ"} 两种方式
                        path_parts = self.path.strip("/").split("/")
                        symbol = path_parts[1].upper() if len(path_parts) > 1 else data.get("symbol", "").upper()
                        if not symbol:
                            self._send(400, {"error": "缺少 symbol"})
                            return
                        future = asyncio.run_coroutine_threadsafe(
                            actor._async_close_position(symbol), loop
                        )
                        result = future.result(timeout=5.0)
                        code = 400 if "error" in result else 200
                        self._send(code, result)
                    except Exception as e:
                        self._send(500, {"error": str(e)})
                    return

                if self.path.startswith("/modify-stop"):
                    # POST /modify-stop  body: {"symbol": "QQQ", "price": 123.45}
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n)) if n > 0 else {}
                        symbol = data.get("symbol", "").upper()
                        price = data.get("price")
                        if not symbol or price is None:
                            self._send(400, {"error": "缺少 symbol 或 price"})
                            return
                        future = asyncio.run_coroutine_threadsafe(
                            actor._async_modify_stop(symbol, float(price)), loop
                        )
                        result = future.result(timeout=5.0)
                        code = 400 if "error" in result else 200
                        self._send(code, result)
                    except Exception as e:
                        self._send(500, {"error": str(e)})
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




