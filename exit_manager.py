import json
import redis as _redis
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce, OrderType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, STTrailSettingsEvent, EMATrailSettingsEvent

# Redis 连接配置（与 strategy.py / order_actor.py 保持一致）
REDIS_HOST = "localhost"
REDIS_PORT = 6379


class ExitManagerConfig(StrategyConfig, frozen=True):
    """ExitManager 配置"""
    pass


class ExitManager(Strategy):
    """
    跟踪止损 Actor（独立策略模块），支持两套止损机制：

    1. ST 跟踪止损（st_trail）：
       - 每根 M1 K 线收盘后，取最新 M1 SuperTrend 值作为新止损价
       - 棘轮机制：多头只允许止损线上移，空头只允许下移

    2. EMA21 M5 跟踪止损（ema_trail）：
       - 每根 M1 K 线收盘后，从 Redis 读取最新 M5 bar 的 ema21 值作为新止损价
       - 棘轮机制相同（只收紧不放宽）
       - 两套机制可同时开启，各自独立计算，取更优（更紧）的止损价

    设计原则：
      - 与 OrderGatewayActor 完全解耦，通过 cache.orders() + modify_order() 操作
      - 不直接平仓，只修改止损触发价
    """

    def __init__(self, config: ExitManagerConfig) -> None:
        super().__init__(config)
        # symbol -> bool，是否启用 ST 跟踪止损
        self._st_trail_active: dict[str, bool] = {}
        # symbol -> bool，是否启用 EMA21 M5 跟踪止损
        self._ema_trail_active: dict[str, bool] = {}
        # Redis 连接
        self._redis = None

    def on_start(self) -> None:
        # 连接 Redis（用于读写 position 数据）
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info("[ExitManager] Redis 已连接")
        except Exception as e:
            self.log.error(f"[ExitManager] Redis 连接失败: {e}")
            self._redis = None

        # 订阅 M1 K 线收盘事件（strategy.py 每根实时1分钟K线收盘后发布）
        self.msgbus.subscribe(
            topic="bar.collected",
            handler=self._on_bar_collected
        )
        # 订阅 ST 跟踪止损开关事件
        self.msgbus.subscribe(
            topic="settings.st_trail",
            handler=self._on_st_settings_change
        )
        # 订阅 EMA 跟踪止损开关事件
        self.msgbus.subscribe(
            topic="settings.ema_trail",
            handler=self._on_ema_settings_change
        )
        self.log.info("[ExitManager] 已启动，等待 bar.collected / settings.st_trail / settings.ema_trail 事件")

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info("[ExitManager] 已停止")

    # ── ST 开关变更 ──────────────────────────────────────────────────────
    def _on_st_settings_change(self, event: STTrailSettingsEvent) -> None:
        self._st_trail_active[event.symbol] = event.active
        self.log.info(
            f"[ExitManager] {event.symbol} ST 跟踪止损已"
            f"{'开启 ✓' if event.active else '关闭'}"
        )

    # ── EMA 开关变更 ─────────────────────────────────────────────────────
    def _on_ema_settings_change(self, event: EMATrailSettingsEvent) -> None:
        self._ema_trail_active[event.symbol] = event.active
        self.log.info(
            f"[ExitManager] {event.symbol} EMA21 M5 跟踪止损已"
            f"{'开启 ✓' if event.active else '关闭'}"
        )

    # ── K 线收盘 → 评估是否调整止损价 ──────────────────────────────────
    def _on_bar_collected(self, event: BarCollectedEvent) -> None:
        sym = event.symbol
        st_active  = self._st_trail_active.get(sym, False)
        ema_active = self._ema_trail_active.get(sym, False)

        if not st_active and not ema_active:
            return

        bar = event.bar
        instrument_id_str = bar.get("instrument_id", "")
        if not instrument_id_str:
            self.log.warning(f"[ExitManager] {sym}: bar 中无 instrument_id，跳过")
            return

        try:
            instrument_id = InstrumentId.from_str(instrument_id_str)
        except Exception as e:
            self.log.error(f"[ExitManager] {sym}: instrument_id 解析失败: {e}")
            return

        # 找到该标的的开放仓位
        open_positions = [
            p for p in self.cache.positions_open()
            if p.instrument_id == instrument_id
        ]
        if not open_positions:
            return

        for pos in open_positions:
            # 收集候选止损价列表（各机制分别计算，最终取最优）
            candidate_sls = []

            # ─ 机制1：ST 跟踪止损 ─────────────────────────────────────
            if st_active:
                st_val = bar.get("st_value", 0.0)
                st_dir = bar.get("st_dir", 0)
                if st_val:  # ST 已预热
                    sl = self._calc_st_sl(pos, st_val, st_dir, sym)
                    if sl is not None:
                        candidate_sls.append(("ST", sl))

            # ─ 机制2：EMA21 M5 跟踪止损 ──────────────────────────────
            if ema_active:
                ema21 = self._get_m5_ema21(sym)
                if ema21 is not None:
                    sl = self._calc_ema_sl(pos, ema21, sym)
                    if sl is not None:
                        candidate_sls.append(("EMA", sl))

            if not candidate_sls:
                continue

            # 读取当前止损价
            current_sl = self._get_current_sl(sym)
            if current_sl is None:
                self.log.info(f"[ExitManager] {sym}: 未找到当前止损价，跳过")
                continue

            # 取各机制中最优（最紧）的止损价
            if pos.is_long:
                # 多头：取最大值（最高的止损价最好）
                best_name, best_sl = max(candidate_sls, key=lambda x: x[1])
                new_sl = max(current_sl, best_sl)  # 棘轮：只允许上移
            else:
                # 空头：取最小值（最低的止损价最好）
                best_name, best_sl = min(candidate_sls, key=lambda x: x[1])
                new_sl = min(current_sl, best_sl)  # 棘轮：只允许下移

            if abs(new_sl - current_sl) < 0.01:
                continue

            self.log.info(
                f"[ExitManager] {sym}: [{best_name}] 止损价 {current_sl:.2f} → {new_sl:.2f}  "
                f"({'多' if pos.is_long else '空'}头)  "
                f"所有候选: {[(n, f'{v:.2f}') for n, v in candidate_sls]}"
            )
            self._modify_stop_order(pos, instrument_id, new_sl, sym)

    # ── ST 止损价计算 ────────────────────────────────────────────────────
    def _calc_st_sl(self, pos, st_val: float, st_dir: int, sym: str) -> float | None:
        """计算 ST 跟踪止损的候选止损价，ST 转向时返回 None（不参与本次竞选）。"""
        if pos.is_long:
            if st_dir != 1:
                self.log.debug(f"[ExitManager] {sym}: 多头但 ST 转空，ST 不参与本次调整")
                return None
            return st_val
        else:
            if st_dir != -1:
                self.log.debug(f"[ExitManager] {sym}: 空头但 ST 转多，ST 不参与本次调整")
                return None
            return st_val

    # ── EMA21 M5 止损价计算 ──────────────────────────────────────────────
    def _calc_ema_sl(self, pos, ema21: float, sym: str) -> float | None:
        """计算 EMA21 M5 跟踪止损的候选止损价。"""
        # EMA 跟踪：直接将 EMA21 作为止损参考线（无方向过滤，EMA 本身就是趋势线）
        return ema21

    # ── 从 Redis 获取最新 M5 EMA21 ──────────────────────────────────────
    def _get_m5_ema21(self, sym: str) -> float | None:
        """从 Redis bars:5m:{sym} 读取最新一根 M5 bar 的 ema21 字段。"""
        if not self._redis:
            return None
        try:
            raw = self._redis.lindex(f"bars:5m:{sym}", -1)  # 最新一根（列表末尾）
            if not raw:
                return None
            bar = json.loads(raw)
            ema21 = bar.get("ema21")
            if ema21 is None:
                return None
            return float(ema21)
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 读取 M5 EMA21 失败: {e}")
            return None

    # ── 修改 IBKR 止损单 ───────────────────────────────────────────────
    def _modify_stop_order(self, pos, instrument_id, new_sl: float, sym: str) -> None:
        """找到活跃的 STOP_MARKET 单并调用 modify_order() 修改触发价。"""
        TERMINAL_STATUS = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}

        stop_order = None
        # ─ 先从 cache 查找 ──────────────────────────────────────────
        for order in self.cache.orders():
            if order.instrument_id != instrument_id:
                continue
            status_name = getattr(order.status, "name", str(order.status))
            type_name = getattr(order.order_type, "name", str(order.order_type))
            if status_name in TERMINAL_STATUS:
                continue
            if type_name == "STOP_MARKET":
                stop_order = order
                break

        # ─ cache 找不到：尝试 Redis fallback ─────────────────────────
        if stop_order is None and self._redis:
            try:
                stored = self._redis.get(f"order:stop:{sym}")
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
                                stop_order = cached
                                self.log.info(
                                    f"[ExitManager] {sym}: 通过 Redis fallback 找到止损单: {coid_str}"
                                )
                        else:
                            self.log.warning(
                                f"[ExitManager] {sym}: Redis 有记录 ({coid_str}) 但 cache 无此订单，"
                                f"等待 IBKR 推回后重试"
                            )
            except Exception as e:
                self.log.warning(f"[ExitManager] {sym}: Redis fallback 失败: {e}")

        if stop_order is None:
            self.log.warning(f"[ExitManager] {sym}: 未找到活跃 STOP_MARKET 单，跳过修改")
            return

        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            self.log.error(f"[ExitManager] {sym}: 合约未加载，无法修改止损")
            return

        try:
            new_trigger_price = instrument.make_price(new_sl)
            self.modify_order(
                order=stop_order,
                trigger_price=new_trigger_price,
            )
            self.log.info(
                f"[ExitManager] {sym}: ✓ modify_order 已提交  "
                f"触发价={new_trigger_price}  "
                f"订单={stop_order.client_order_id}"
            )
            # 同步更新 Redis
            self._update_redis_sl(sym, new_sl)
        except Exception as e:
            self.log.error(f"[ExitManager] {sym}: modify_order 失败: {e}")


    # ── Redis 辅助 ─────────────────────────────────────────────────────
    def _get_current_sl(self, sym: str) -> float | None:
        """从 Redis position:{sym} 读取当前止损价。"""
        if not self._redis:
            return None
        try:
            raw = self._redis.get(f"position:{sym}")
            if not raw:
                return None
            data = json.loads(raw)
            sl = data.get("stop_loss")
            return float(sl) if sl is not None else None
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 读取 Redis stop_loss 失败: {e}")
            return None

    def _update_redis_sl(self, sym: str, new_sl: float) -> None:
        """更新 Redis position:{sym}.stop_loss 并 PUBLISH position:update 通知前端。"""
        if not self._redis:
            return
        try:
            raw = self._redis.get(f"position:{sym}")
            if not raw:
                return
            data = json.loads(raw)
            data["stop_loss"] = new_sl
            self._redis.set(f"position:{sym}", json.dumps(data))
            # 通知前端更新止损线位置
            self._redis.publish("position:update", json.dumps({
                **data,
                "stop_loss": new_sl,
            }))
            self.log.info(f"[ExitManager] {sym}: Redis stop_loss 已更新为 {new_sl:.2f}")
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 更新 Redis stop_loss 失败: {e}")
