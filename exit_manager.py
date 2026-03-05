import json
import redis as _redis
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, BarCollectedM5Event

# Redis 连接配置（与 strategy.py / order_actor.py 保持一致）
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# trail_mode 枚举值
TRAIL_OFF   = 0   # 全部关闭
TRAIL_M1_ST = 1   # M1 SuperTrend 跟踪（10, 3.5）
TRAIL_M5_ST = 2   # M5 SuperTrend 跟踪（10, 3.0）
TRAIL_EMA   = 3   # M5 EMA21 跟踪


class ExitManagerConfig(StrategyConfig, frozen=True):
    """ExitManager 配置"""
    pass


class ExitManager(Strategy):
    """
    跟踪止盈 Actor。通过 settings:{sym}.trail_mode 控制三套互斥止盈机制：

      0 = 关闭
      1 = M1 ST 跟踪（每根 M1 bar 触发，参数 10, 3.5）
      2 = M5 ST 跟踪（每根 M5 bar 触发，参数 10, 3.0）
      3 = EMA M5 跟踪（每根 M1 bar 触发，使用最新 M5 EMA21）

    设计原则：
      - 每次 bar 收盘直接从 Redis 读 trail_mode，不依赖内存状态
      - 三套互斥，只有一个生效，由前端控制
      - 棘轮机制：多头止损只上移，空头止损只下移（|变化| >= 0.01 才提交）
    """

    def __init__(self, config: ExitManagerConfig) -> None:
        super().__init__(config)
        self._redis = None

    def on_start(self) -> None:
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

        self.msgbus.subscribe(topic="bar.collected",    handler=self._on_m1_bar)
        self.msgbus.subscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
        self.log.info("[ExitManager] 已启动｜trail_mode: 0=关 1=M1-ST 2=M5-ST 3=EMA")

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info("[ExitManager] 已停止")

    # ── 读取 trail_mode ──────────────────────────────────────────────────
    def _get_trail_mode(self, sym: str) -> int:
        """从 Redis settings:{sym} 读取 trail_mode，失败返回 0（关闭）。"""
        if not self._redis:
            return TRAIL_OFF
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return TRAIL_OFF
            return int(json.loads(raw).get("trail_mode", TRAIL_OFF))
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 读取 trail_mode 失败: {e}")
            return TRAIL_OFF

    # ── M1 bar 收盘：处理 mode=1（M1 ST）和 mode=3（EMA）─────────────────
    def _on_m1_bar(self, event: BarCollectedEvent) -> None:
        sym = event.symbol
        mode = self._get_trail_mode(sym)
        if mode == TRAIL_OFF:
            return

        bar = event.bar
        iid_str = bar.get("instrument_id", "")
        if not iid_str:
            return

        if mode == TRAIL_M1_ST:
            st_val = bar.get("st_value", 0.0)
            st_dir = bar.get("st_dir", 0)
            if st_val:
                self._apply_trail(sym, iid_str, st_val, st_dir, "M1-ST")

        elif mode == TRAIL_EMA:
            ema21 = self._get_m5_ema21(sym)
            if ema21:
                # EMA 无方向过滤（st_dir=0 表示跳过方向判断）
                self._apply_trail(sym, iid_str, ema21, 0, "EMA-M5")

    # ── M5 bar 收盘：处理 mode=2（M5 ST）────────────────────────────────
    def _on_m5_bar(self, event: BarCollectedM5Event) -> None:
        sym = event.symbol
        mode = self._get_trail_mode(sym)
        if mode != TRAIL_M5_ST:
            return

        bar = event.bar
        iid_str = bar.get("instrument_id", "")
        st_val = bar.get("st_value", 0.0)
        st_dir = bar.get("st_dir", 0)
        if st_val and iid_str:
            self._apply_trail(sym, iid_str, st_val, st_dir, "M5-ST")

    # ── 核心棘轮逻辑（三套共用）──────────────────────────────────────────
    def _apply_trail(self, sym: str, iid_str: str,
                     indicator_val: float, st_dir: int, label: str) -> None:
        """
        棘轮跟踪：
          - st_dir=1  → 多头看涨方向有效
          - st_dir=-1 → 空头看跌方向有效
          - st_dir=0  → 不过滤方向（EMA 跟踪）
        """
        try:
            instrument_id = InstrumentId.from_str(iid_str)
        except Exception as e:
            self.log.error(f"[ExitManager] {sym}: [{label}] instrument_id 解析失败: {e}")
            return

        positions = [p for p in self.cache.positions_open()
                     if p.instrument_id == instrument_id]
        if not positions:
            return

        for pos in positions:
            # 方向过滤
            if st_dir != 0:
                if pos.is_long and st_dir != 1:
                    continue
                if not pos.is_long and st_dir != -1:
                    continue

            current_sl = self._get_current_sl(sym)
            if current_sl is None:
                self.log.info(f"[ExitManager] {sym}: [{label}] 未找到当前止损价，跳过")
                continue

            # 棘轮
            if pos.is_long:
                new_sl = max(current_sl, indicator_val)
            else:
                new_sl = min(current_sl, indicator_val)

            if abs(new_sl - current_sl) < 0.01:
                continue

            self.log.info(
                f"[ExitManager] {sym}: [{label}] "
                f"{current_sl:.2f} → {new_sl:.2f}  "
                f"({'多' if pos.is_long else '空'}头)  指标={indicator_val:.2f}"
            )
            self._modify_stop_order(pos, instrument_id, new_sl, sym)

    # ── 读最新 M5 EMA21 ──────────────────────────────────────────────────
    def _get_m5_ema21(self, sym: str) -> float | None:
        if not self._redis:
            return None
        try:
            raw = self._redis.lindex(f"bars:5m:{sym}", -1)
            if not raw:
                return None
            ema21 = json.loads(raw).get("ema21")
            return float(ema21) if ema21 is not None else None
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 读取 M5 EMA21 失败: {e}")
            return None

    # ── 读取当前止损价（三重 fallback）──────────────────────────────────
    def _get_current_sl(self, sym: str) -> float | None:
        TERMINAL = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}

        # 方案1：cache 中活跃的 STOP_MARKET 单
        try:
            for order in self.cache.orders():
                if order.instrument_id.symbol.value != sym:
                    continue
                if getattr(order.status, "name", "") in TERMINAL:
                    continue
                if getattr(order.order_type, "name", "") == "STOP_MARKET":
                    tp = getattr(order, "trigger_price", None)
                    if tp is not None:
                        return float(tp)
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: cache trigger_price 读取失败: {e}")

        # 方案2：Redis order:stop:{sym}
        if self._redis:
            try:
                stored = self._redis.get(f"order:stop:{sym}")
                if stored:
                    tp_str = json.loads(stored).get("trigger_price")
                    if tp_str:
                        self.log.info(f"[ExitManager] {sym}: order:stop fallback tp={tp_str}")
                        return float(tp_str)
            except Exception as e:
                self.log.warning(f"[ExitManager] {sym}: order:stop 读取失败: {e}")

        # 方案3：Redis position:{sym}.stop_loss
        if self._redis:
            try:
                raw = self._redis.get(f"position:{sym}")
                if raw:
                    sl = json.loads(raw).get("stop_loss")
                    if sl is not None:
                        self.log.info(f"[ExitManager] {sym}: position fallback sl={sl}")
                        return float(sl)
            except Exception as e:
                self.log.warning(f"[ExitManager] {sym}: position 读取失败: {e}")

        return None

    # ── 修改 IBKR 止损单 ─────────────────────────────────────────────────
    def _modify_stop_order(self, pos, instrument_id, new_sl: float, sym: str) -> None:
        TERMINAL = {"FILLED", "CANCELED", "EXPIRED", "REJECTED", "DENIED"}
        stop_order = None

        for order in self.cache.orders():
            if order.instrument_id != instrument_id:
                continue
            if getattr(order.status, "name", "") in TERMINAL:
                continue
            if getattr(order.order_type, "name", "") == "STOP_MARKET":
                stop_order = order
                break

        if stop_order is None and self._redis:
            try:
                stored = self._redis.get(f"order:stop:{sym}")
                if stored:
                    coid_str = json.loads(stored).get("client_order_id", "")
                    if coid_str:
                        from nautilus_trader.model.identifiers import ClientOrderId
                        cached = self.cache.order(ClientOrderId(coid_str))
                        if cached and getattr(cached.status, "name", "") not in TERMINAL:
                            stop_order = cached
                            self.log.info(f"[ExitManager] {sym}: Redis fallback 止损单: {coid_str}")
            except Exception as e:
                self.log.warning(f"[ExitManager] {sym}: Redis fallback 失败: {e}")

        if stop_order is None:
            self.log.warning(f"[ExitManager] {sym}: 未找到活跃 STOP_MARKET 单，跳过")
            return

        instrument = self.cache.instrument(instrument_id)
        if instrument is None:
            self.log.error(f"[ExitManager] {sym}: 合约未加载")
            return

        try:
            new_trigger_price = instrument.make_price(new_sl)
            self.modify_order(order=stop_order, trigger_price=new_trigger_price)
            self.log.info(
                f"[ExitManager] {sym}: ✓ modify_order  触发价={new_trigger_price}  "
                f"订单={stop_order.client_order_id}"
            )
            self._update_redis_sl(sym, new_sl)
        except Exception as e:
            self.log.error(f"[ExitManager] {sym}: modify_order 失败: {e}")

    # ── 同步更新 Redis 止损价 ─────────────────────────────────────────────
    def _update_redis_sl(self, sym: str, new_sl: float) -> None:
        if not self._redis:
            return
        try:
            stored = self._redis.get(f"order:stop:{sym}")
            if stored:
                data = json.loads(stored)
                data["trigger_price"] = str(new_sl)
                self._redis.set(f"order:stop:{sym}", json.dumps(data))

            pos_raw = self._redis.get(f"position:{sym}")
            if pos_raw:
                pos = json.loads(pos_raw)
                pos["stop_loss"] = new_sl
                self._redis.set(f"position:{sym}", json.dumps(pos))
                self._redis.publish("position:update", json.dumps({**pos, "stop_loss": new_sl}))
            self.log.info(f"[ExitManager] {sym}: Redis 止损价更新 → {new_sl:.2f}")
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 更新 Redis 止损价失败: {e}")
