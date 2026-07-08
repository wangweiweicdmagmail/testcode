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
import os
import time
import queue
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import redis as _redis_lib   # pip install redis
    _REDIS_AVAILABLE = True
except ImportError:
    _REDIS_AVAILABLE = False

from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from decimal import Decimal
from events import (
    AgentExecuteNowEvent,
    STTrailSettingsEvent,
    EMATrailSettingsEvent,
    TERMINAL_STATUS,
)
from portfolio.sessions import et_session_date, et_minute_now
from portfolio.trading_env import live_orders_allowed
from portfolio.ib_orders import build_marketable_order
from portfolio.fa_validate import validate_fa_group

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
        self._redis: "_redis_lib.Redis | None" = None  # Redis 推送客户端
        # FA Group 专属账户查询线程控制
        self._fa_accounts_running: bool = False
        self._fa_validated: bool = False
        # 心跳线程控制（在 __init__ 初始化，防止 on_stop 在 on_start 前被调用时 AttributeError）
        self._heartbeat_running: bool = False
        # 跟踪止损开关状态（sym -> bool）
        self._st_trail_active: dict[str, bool] = {}
        self._ema_trail_active: dict[str, bool] = {}


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
            if live_orders_allowed() and not os.environ.get("ORDER_GATEWAY_SECRET", "").strip():
                self.log.error(
                    "[Gateway] ⛔ TRADING_ENV=live 但未设置 ORDER_GATEWAY_SECRET — "
                    "8888 下单网关对局域网开放且无 Token"
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

        self._print_engine_ready_banner()

    def _print_engine_ready_banner(self) -> None:
        if getattr(OrderGatewayActor, "_ready_banner_printed", False):
            return
        OrderGatewayActor._ready_banner_printed = True
        port = self.config.http_port
        inner = (
            f" SUCCESS: 引擎网关就绪 | HTTP :{port}/order | Redis 心跳已开启 | PID={os.getpid()} "
        )
        border = "*" * (len(inner) + 2)
        print(f"\n{border}", flush=True)
        print(f"*{inner}*", flush=True)
        print(f"{border}\n", flush=True)

    def on_stop(self) -> None:
        """停止：取消订阅 + 关闭 HTTP Server + 取消止损修改 tasks"""
        self.msgbus.unsubscribe(
            topic=ExternalOrderCommand.TOPIC,
            handler=self.on_external_order_command,
        )
        self.msgbus.unsubscribe(topic="settings.st_trail",  handler=self._on_st_trail_change)
        self.msgbus.unsubscribe(topic="settings.ema_trail", handler=self._on_ema_trail_change)
        self.msgbus.unsubscribe(topic="bar.collected",      handler=self._on_bar_collected_trail)

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
                    payload = json.dumps({
                        "ts": int(time.time()),
                        "status": "alive",
                    })
                    self._redis.publish("engine:heartbeat", payload)
                    # 供 MCP/CLI/前端 REST GET（pub/sub 无订阅者时不可见）
                    self._redis.setex("engine:heartbeat", 60, payload)
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

        if fa_group and not self._fa_validated:
            ok, msg = validate_fa_group(ibg_host, ibg_port, aux_client_id + 1, fa_group)
            if ok:
                self.log.info(f"[Account-FA] 启动校验 ✓ {msg}")
            else:
                self.log.error(f"[Account-FA] ⚠️ FA Group 校验失败: {msg}")
            self._fa_validated = True

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
                # ── 记录当日起始净值（SOD equity），用于日亏损计算 ──────────
                # 每个交易日第一次写入时设置，过期时间到次日 UTC 凌晨
                if usd['total'] > 0:
                    today = et_session_date()  # ET 日历日，避免 UTC+8 机器盘中翻篇
                    sod_key = f'risk:equity_sod:{today}'
                    if not self._redis.exists(sod_key):
                        self._redis.set(sod_key, str(usd['total']))
                        # 设置 30 小时过期（跨日自动清除）
                        self._redis.expire(sod_key, 30 * 3600)
                        # 护栏：若首次锚定 SOD 时已进入/越过 RTH（盘中冷启动），
                        # 该基准并非真实日开盘净值，日亏损熔断会从此刻起算 →
                        # 显式告警，提醒运营者熔断基准不可靠。
                        et_min = et_minute_now()
                        if et_min >= 9 * 60 + 35:  # 09:35 ET 之后才首记 → 视为晚启动
                            self.log.warning(
                                f"[Risk] ⚠️ SOD 起始净值在盘中({et_min // 60:02d}:{et_min % 60:02d} ET)"
                                f"才首次锚定={usd['total']:,.2f}：熔断基准非真实日开盘，"
                                f"日亏损将从此刻起算。如需准确请尽早启动引擎或手动校正 {sod_key}。"
                            )
                        else:
                            self.log.info(
                                f"[Risk] 今日起始净值已记录: {usd['total']:,.2f} USD  key={sod_key}"
                            )
        except Exception as e:
            self.log.warning(f"[Account] Redis 写入失败: {e}")
        self._maybe_trigger_risk_halt()

    DAILY_LOSS_LIMIT_PCT = -0.01

    def _maybe_trigger_risk_halt(self) -> None:
        """账户余额更新后检查日亏损；触及 -1% 则全平并熔断（每交易日一次）。"""
        if not self._redis or self._loop is None:
            return
        try:
            status = self.get_risk_status()
            if status.get("halted") or status.get("error"):
                return
            pct = status.get("daily_pnl_pct")
            limit = status.get("limit_pct", self.DAILY_LOSS_LIMIT_PCT)
            if pct is None or pct > limit:
                return
            self.log.warning(
                f"[Risk] 日亏损 {pct:.2%} ≤ 限制 {limit:.2%}，触发自动全平+熔断"
            )
            future = asyncio.run_coroutine_threadsafe(
                self._async_close_all(), self._loop
            )
            future.result(timeout=30.0)
        except Exception as e:
            self.log.error(f"[Risk] 自动熔断检查失败: {e}")

    def get_risk_status(self) -> dict:
        """返回今日风险状态供 HTTP GET /risk 调用"""
        if not self._redis:
            return {'error': 'Redis 未连接'}
        try:
            today = et_session_date()  # ET 日历日（统一口径）
            sod_key  = f'risk:equity_sod:{today}'
            halt_key = f'risk:halt:{today}'

            sod_raw  = self._redis.get(sod_key)
            acct_raw = self._redis.get('account:funds')
            halted   = bool(self._redis.get(halt_key))

            equity_sod = float(sod_raw) if sod_raw else None
            equity_now = None
            if acct_raw:
                acct = json.loads(acct_raw)
                usd = next(
                    (b for b in acct.get('balances', []) if b['currency'] == 'USD'), None
                )
                if usd:
                    equity_now = usd['total']

            limit_pct = -0.01  # 日内熔断阈值（-1%）
            daily_pnl = None
            daily_pnl_pct = None
            remaining_loss_pct = None
            if equity_sod and equity_now:
                daily_pnl     = round(equity_now - equity_sod, 2)
                daily_pnl_pct = round(daily_pnl / equity_sod, 6)
                # 离熔断还有多少个百分点（百分比单位）：当前收益率与熔断线的距离
                # 例：当前 -0.8% 时 = (-0.008 - (-0.01)) × 100 = 0.20（pp）
                remaining_loss_pct = round((daily_pnl_pct - limit_pct) * 100, 4)

            return {
                'equity_sod':    equity_sod,
                'equity_now':    equity_now,
                'daily_pnl':     daily_pnl,
                'daily_pnl_pct': daily_pnl_pct,
                'remaining_loss_pct': remaining_loss_pct,
                'halted':        halted,
                'limit_pct':     limit_pct,
                'ts':            int(time.time()),
            }
        except Exception as e:
            self.log.warning(f"[Risk] get_risk_status 失败: {e}")
            return {'error': str(e)}


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

    def _has_active_alpha_proposal(self, sym: str) -> bool:
        """pending 或 approved_wait/ready_to_execute 未过期建议存在。"""
        if not self._redis:
            return False
        import time as _time
        now = int(_time.time())
        for status in ("pending", "approved"):
            index = f"proposal:{status}:index"
            try:
                ids = self._redis.zrevrange(index, 0, 99)
            except Exception:
                continue
            for pid in ids:
                key = f"proposal:{status}:{pid}"
                raw = self._redis.hgetall(key)
                if not raw:
                    continue
                try:
                    p = {}
                    for k, v in raw.items():
                        try:
                            p[k] = json.loads(v)
                        except (json.JSONDecodeError, TypeError):
                            p[k] = v
                except Exception:
                    continue
                if str(p.get("symbol", "")).upper() != sym.upper():
                    continue
                if status == "pending":
                    return True
                if p.get("executed_at"):
                    continue
                phase = str(p.get("execution_phase") or "")
                if phase not in ("approved_wait", "ready_to_execute"):
                    continue
                exp = int(p.get("expires_at") or 0)
                if exp and now > exp:
                    continue
                return True
        return False

    def on_external_order_command(self, event: ExternalOrderCommand) -> None:
        """
        处理来自 MessageBus 的外部下单指令
        此函数由 msgbus.publish() 在引擎事件循环中同步触发
        """
        # ── 日亏损熔断检查：超限则拒绝所有开仓 ──────────────────────
        if self._redis:
            today = et_session_date()  # ET 日历日（统一口径）
            if self._redis.get(f'risk:halt:{today}'):
                sym = event.instrument_id.split('.')[0]
                self.log.error(
                    f"[Risk] 今日亏损已超过1%限制，拒绝开仓指令: {event.order_type} {sym}"
                )
                try:
                    self._redis.publish('order:update', json.dumps({
                        'status': 'DENIED',
                        'reason': '今日亏损已超过1%上限，禁止开仓',
                        'symbol': sym,
                        'ts': int(time.time()),
                    }))
                except Exception:
                    pass
                return

        sym = event.instrument_id.split('.')[0]

        # ── 禁止人工开仓（Agent/AutoPM 经 submit_order 路径，不经此外部 HTTP 网关）──
        self.log.error(f"[Order] 拒绝人工开仓: {sym} type={event.order_type}")
        if self._redis:
            try:
                self._redis.publish('order:update', json.dumps({
                    'status': 'DENIED',
                    'reason': '系统禁止手动开仓，请走 Alpha 审批流程',
                    'symbol': sym,
                    'ts': int(time.time()),
                }))
            except Exception:
                pass
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
                    # LIMIT 单有 price（挂单限价）
                    lp = getattr(order, "price", None)
                    if lp is not None:
                        msg["price"] = str(lp)
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

        # 任意成交后：若 broker 侧该标的已无持仓 → 清 Redis（避免报单即删造成 UI 假平仓）
        try:
            order = self.cache.order(event.client_order_id)
            if order is not None:
                self._clear_position_redis_if_flat(order.instrument_id)
        except Exception as e:
            self.log.warning(f"[Close] 成交后检查平仓 Redis 失败: {e}")

        # 止损单成交（被触发平仓）→ 清除 Redis 止损单记录 + 仓位记录 + 推送平仓事件
        # ★ 双保险：不依赖 on_position_closed（FA Group 场景下 NautilusTrader 可能不触发该回调）
        try:
            order = self.cache.order(event.client_order_id)
            if order and getattr(order.order_type, "name", "") == "STOP_MARKET":
                sym = order.instrument_id.symbol.value
                # 1. 清除止损单持久化记录
                self._clear_stop_order(sym, str(event.client_order_id))
                self.log.info(f"[SL] 止损单已成交触发，清除 Redis 止损记录: {sym}")
                # 2. 清除 Redis 仓位记录（防止前端刷新页面后重新显示已平仓的仓位线）
                if self._redis:
                    self._redis.delete(f"position:{sym}")
                    # 3. 推送 position:update closed=true（前端实时清除价格线和仓位面板）
                    self._redis.publish("position:update", json.dumps({
                        "symbol": sym,
                        "closed": True,
                    }))
                    self.log.info(f"[SL] 仓位已清除 Redis 并推送 position:update closed=True: {sym}")
        except Exception as e:
            self.log.warning(f"[SL] on_order_filled 清除止损/仓位记录失败: {e}")

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
                # LIMIT_BRACKET 新增字段
                tp_price=data.get("tp_price"),
                tp_qty=data.get("tp_qty"),
                entry_price=data.get("entry_price"),
            )
            # ★ 标准 MessageBus 发布
            self.msgbus.publish(
                topic=ExternalOrderCommand.TOPIC,
                msg=event,
            )
        except Exception as e:
            self.log.error(f"[Gateway] 消息发布失败: {e}")

    async def _async_bridge_execute_now(self, data: dict) -> None:
        """审批通过后立即触发 Agent 执行。"""
        try:
            symbol = str(data.get("symbol") or "").upper()
            if not symbol:
                self.log.warning("[Gateway] execute-proposal 缺少 symbol")
                return
            proposal_id = str(data.get("proposal_id") or "")
            event = AgentExecuteNowEvent(symbol=symbol, proposal_id=proposal_id)
            self.msgbus.publish(topic=AgentExecuteNowEvent.TOPIC, msg=event)
            self.log.info(
                f"[Gateway] Agent 立即执行已发布: {symbol}"
                + (f" proposal={proposal_id}" if proposal_id else "")
            )
        except Exception as e:
            self.log.error(f"[Gateway] Agent 立即执行发布失败: {e}")

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

    def _clear_position_redis_if_flat(self, instrument_id) -> None:
        """broker 侧该标的已无持仓时，清除 Redis 仓位/auto:units 并推送 closed。"""
        if not self._redis:
            return
        try:
            sym = instrument_id.symbol.value
            for pos in self.cache.positions_open():
                if pos.instrument_id.symbol.value == sym:
                    return
            sym_u = sym.upper()
            self._redis.delete(f"auto:units:{sym_u}")
            self._redis.delete(f"position:{sym_u}")
            self._redis.publish("position:update", json.dumps({
                "symbol": sym_u,
                "closed": True,
            }))
            self.log.info(f"[Close] 仓位已平，Redis 已清除: {sym_u}")
        except Exception as e:
            self.log.warning(f"[Close] _clear_position_redis_if_flat 失败: {e}")

    async def _async_close_all(self) -> dict:
        """
        全部平仓（风控触发）：遍历所有开放仓位，逐一平仓。
        同时设置 risk:halt:{today}=1，禁止当日后续开仓。
        """
        results = []
        try:
            positions = list(self.cache.positions_open())
            if not positions:
                self.log.warning("[Risk] _async_close_all: 无开放仓位")
            for pos in positions:
                sym = pos.instrument_id.symbol.value
                self.log.warning(f"[Risk] 自动平仓触发: {sym}")
                r = await self._async_close_position(sym)
                results.append(r)

            # 设置熔断标志（当日禁止开仓）
            if self._redis:
                today = et_session_date()  # ET 日历日（统一口径）
                self._redis.set(f'risk:halt:{today}', '1')
                self._redis.expire(f'risk:halt:{today}', 30 * 3600)
                self._redis.publish('risk:update', json.dumps({
                    'halted': True,
                    'reason': '日亏损超过1%，自动平仓并熔断',
                    'ts': int(time.time()),
                }))
                self.log.warning(
                    f"[Risk] ⛔ 熔断已激活：今日禁止开仓  risk:halt:{today}=1"
                )
        except Exception as e:
            self.log.error(f"[Risk] _async_close_all 失败: {e}")
            results.append({'error': str(e)})

        return {'closed': results, 'halted': True}

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

            # 1. 取消所有该标的的活跃止损单（STOP_MARKET）和止盈单（LIMIT）
            TERMINAL_STATUS = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}
            canceled_count = 0
            for order in self.cache.orders():
                if order.instrument_id != instrument_id:
                    continue
                order_status = getattr(order.status, "name", str(order.status))
                type_name = getattr(order.order_type, "name", str(order.order_type))
                if order_status in TERMINAL_STATUS:
                    continue
                if type_name in ("STOP_MARKET", "LIMIT"):
                    self.log.info(f"[Close] 取消 {type_name} 单 {order.client_order_id}")
                    self.cancel_order(order)
                    canceled_count += 1

            # 2. 提交反向平仓单（延迟行情用 marketable LIMIT）
            ref_px = float(target_pos.avg_px_open)
            if self._redis:
                try:
                    bar_raw = self._redis.get(f"bars:1m:{symbol.upper()}:last")
                    if bar_raw:
                        bar = json.loads(bar_raw)
                        ref_px = float(bar.get("close", ref_px) or ref_px)
                except Exception:
                    pass
            close_order, decision = build_marketable_order(
                self.order_factory,
                instrument=instrument,
                instrument_id=instrument_id,
                side=close_side,
                qty=int(quantity),
                ref_price=ref_px,
                tags=self._fa_tags(),
                log_fn=lambda m: self.log.info(f"[Close] {symbol}: {m}"),
            )
            self.submit_order(close_order)

            self.log.info(
                f"[Close] ✅ 平仓单已提交  "
                f"ClientOrderId={close_order.client_order_id}  "
                f"side={side_str}  qty={quantity}  {instrument_id}  "
                f"{'LMT' if decision.use_limit else 'MKT'}  "
                f"已取消订单数量={canceled_count}"
            )
            return {
                "status": "accepted",
                "symbol": symbol,
                "side": side_str,
                "qty": str(quantity),
                "client_order_id": str(close_order.client_order_id),
                "canceled_stop_orders": canceled_count,
                "note": "平仓单已提交，Redis 仓位将在成交确认后清除",
            }
        except Exception as e:
            self.log.error(f"[Close] 平仓失败: {e}")
            return {"error": str(e)}

    async def _async_cancel_entry(self, symbol: str, client_order_id: str) -> dict:
        """取消挂单中的限价入场单。"""
        try:
            from nautilus_trader.model.identifiers import ClientOrderId
            coid_obj = ClientOrderId(client_order_id)
            order = self.cache.order(coid_obj)
            if order is None:
                return {"error": f"找不到订单 {client_order_id}"}
            order_status = getattr(order.status, "name", str(order.status))
            if order_status in ("FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"):
                return {"error": f"订单 {client_order_id} 已处于终态 {order_status}，无法取消"}
            # 取消入场单
            self.cancel_order(order)
            self.log.info(f"[Entry] 取消限价入场单 {client_order_id} ({symbol})")
            return {"status": "canceled", "client_order_id": client_order_id}
        except Exception as e:
            self.log.error(f"[Entry] 取消入场单失败: {e}")
            return {"error": str(e)}

    async def _async_modify_entry(self, symbol: str, client_order_id: str, price: float) -> dict:
        """修改挂单中的限价入场单价格。"""
        try:
            from nautilus_trader.model.identifiers import ClientOrderId
            coid_obj = ClientOrderId(client_order_id)
            order = self.cache.order(coid_obj)
            if order is None:
                return {"error": f"找不到订单 {client_order_id}"}
            order_status = getattr(order.status, "name", str(order.status))
            if order_status in ("FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"):
                return {"error": f"订单 {client_order_id} 已处于终态 {order_status}"}
            instrument = self.cache.instrument(order.instrument_id)
            if instrument is None:
                return {"error": f"合约未加载"}
            new_price = instrument.make_price(Decimal(str(price)))
            self.modify_order(
                order,
                price=new_price,
                trigger_price=None,
            )
            self.log.info(f"[Entry] 修改限价入场单 {client_order_id} 价格 → {new_price}")
            return {"status": "modified", "client_order_id": client_order_id, "price": str(new_price)}
        except Exception as e:
            self.log.error(f"[Entry] 修改入场单失败: {e}")
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
                elif self.path == "/risk":
                    self._send(200, actor.get_risk_status())
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

                if self.path == "/execute-proposal":
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n)) if n > 0 else {}
                        symbol = str(data.get("symbol") or "").upper()
                        if not symbol:
                            self._send(400, {"error": "缺少 symbol"})
                            return
                        asyncio.run_coroutine_threadsafe(
                            actor._async_bridge_execute_now(data), loop
                        ).result(timeout=3.0)
                        self._send(200, {"status": "ok", "symbol": symbol})
                    except Exception as e:
                        self._send(500, {"error": str(e)})
                    return

                if self.path == "/close-all":
                    # POST /close-all — 风控：全部平仓 + 熔断（禁止当日开仓）
                    try:
                        future = asyncio.run_coroutine_threadsafe(
                            actor._async_close_all(), loop
                        )
                        result = future.result(timeout=10.0)
                        self._send(200, result)
                    except Exception as e:
                        self._send(500, {"error": str(e)})
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

                if self.path.startswith("/cancel-entry"):
                    # POST /cancel-entry  body: {"symbol": "QQQ", "client_order_id": "..."}
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n)) if n > 0 else {}
                        symbol = data.get("symbol", "").upper()
                        coid = data.get("client_order_id", "")
                        if not symbol or not coid:
                            self._send(400, {"error": "缺少 symbol 或 client_order_id"})
                            return
                        future = asyncio.run_coroutine_threadsafe(
                            actor._async_cancel_entry(symbol, coid), loop
                        )
                        result = future.result(timeout=5.0)
                        code = 400 if "error" in result else 200
                        self._send(code, result)
                    except Exception as e:
                        self._send(500, {"error": str(e)})
                    return

                if self.path.startswith("/modify-entry"):
                    # POST /modify-entry  body: {"symbol": "QQQ", "client_order_id": "...", "price": 123.45}
                    try:
                        n = int(self.headers.get("Content-Length", 0))
                        data = json.loads(self.rfile.read(n)) if n > 0 else {}
                        symbol = data.get("symbol", "").upper()
                        coid = data.get("client_order_id", "")
                        price = data.get("price")
                        if not symbol or not coid or price is None:
                            self._send(400, {"error": "缺少 symbol, client_order_id 或 price"})
                            return
                        future = asyncio.run_coroutine_threadsafe(
                            actor._async_modify_entry(symbol, coid, float(price)), loop
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
                self._send(403, {
                    "error": "系统禁止手动开仓",
                    "hint": "请通过 Alpha 审批流程，由 Agent 自动执行",
                })

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




