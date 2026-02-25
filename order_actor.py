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
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer

from nautilus_trader.adapters.interactive_brokers.common import IBOrderTags
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.message import Event
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from decimal import Decimal


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
        import time
        self._id = UUID4()
        self._ts_event = time.time_ns()
        self._ts_init = time.time_ns()
        # 业务字段
        self.instrument_id = instrument_id
        self.side = side.upper()
        self.qty = qty
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

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """启动：注册 MessageBus 订阅 → 启动 HTTP Server"""
        # 获取引擎的 asyncio 事件循环（跨线程通信用）
        self._loop = asyncio.get_event_loop()

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

        if self._http_server:
            threading.Thread(
                target=self._http_server.shutdown, daemon=True
            ).start()

        self.log.info("[Gateway] OrderGatewayActor 已停止")

    # ------------------------------------------------------------------
    # ★ 标准 MessageBus 消息处理器
    # ------------------------------------------------------------------

    def on_external_order_command(self, event: ExternalOrderCommand) -> None:
        """
        处理来自 MessageBus 的外部下单指令
        此函数由 msgbus.publish() 在引擎事件循环中同步触发
        """
        self.log.info(f"[Gateway] 收到 MessageBus 消息: {event}")

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
            self.submit_order(order)
            self._log_submitted(event, order.client_order_id.value)

        elif event.order_type == "BRACKET":
            # 括号单：市价入场 + 止损单，通过 IBKR OCA(ocaGroup) 实现联动取消
            # —— 规避 bracket() 强制构建 tp=LimitOrder(price=None) 的问题
            if event.stop_loss is None:
                self.log.error("[Gateway] BRACKET 单必须提供 stop_loss")
                return

            # 生成唯一 OCA 组名，确保两笔单联动
            import time as _time
            oca_group = f"BKT-{int(_time.time_ns() // 1_000_000)}"
            oca_extra = {"ocaGroup": oca_group, "ocaType": 2}

            # 入场单（市价）+ FA + OCA 字段合并进同一 IBOrderTags tag
            entry_order = self.order_factory.market(
                instrument_id=instrument_id,
                order_side=order_side,
                quantity=quantity,
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(extra_fields=oca_extra),
            )
            sl_side = OrderSide.SELL if order_side == OrderSide.BUY else OrderSide.BUY
            sl_price = instrument.make_price(Decimal(str(event.stop_loss)))
            sl_order = self.order_factory.stop_market(
                instrument_id=instrument_id,
                order_side=sl_side,
                quantity=quantity,
                trigger_price=sl_price,
                time_in_force=TimeInForce.GTC,
                tags=self._fa_tags(extra_fields=oca_extra),
            )

            # 分别提交两笔单
            self.submit_order(entry_order)
            self.submit_order(sl_order)

            self.log.info(
                f"[Gateway] BRACKET 已提交 | OCA={oca_group} | "
                f"Entry={entry_order.client_order_id} | "
                f"SL={sl_order.client_order_id} @ {event.stop_loss} | "
                f"Steps={event.sl_steps} every {event.sl_step_secs}s"
            )

            # 注册止损价定时修改任务（P5: 保存 task 引用）
            for i, new_sl in enumerate(event.sl_steps):
                delay = event.sl_step_secs * (i + 1)
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

    def _start_http_server(self) -> None:
        """在守护线程中启动 HTTP 网关"""
        loop = self._loop
        publish_fn = self._async_bridge  # 使用 self 的 coroutine

        class _Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                # P4: Token 认证（设置环境变量 ORDER_GATEWAY_SECRET 启用）
                import os as _os
                _secret = _os.environ.get("ORDER_GATEWAY_SECRET", "")
                if _secret and self.headers.get("X-Order-Token") != _secret:
                    self._send(403, {"error": "Unauthorized: invalid X-Order-Token"})
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
        import json as _json
        payload = {}

        # 先填入 FA 分配字段
        if self.config.fa_group:
            payload["faGroup"] = self.config.fa_group
            payload["faMethod"] = self.config.fa_method
            self.log.info(
                f"[Gateway] FA 分配 tag: group={self.config.fa_group} "
                f"method={self.config.fa_method}"
            )

        # 合并额外字段（如 ocaGroup/ocaType）
        if extra_fields:
            payload.update(extra_fields)

        if not payload:
            return None

        tag_str = f"IBOrderTags:{_json.dumps(payload)}"
        return [tag_str]   # order_factory 的 tags 参数要求 list[str]




