import json
import http.client
import redis as _redis
from dataclasses import dataclass
from typing import Callable
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, BarCollectedM5Event, TERMINAL_STATUS


# Redis 连接配置（与 strategy.py / order_actor.py 保持一致）
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# trail_mode 枚举值
TRAIL_OFF   = 0   # 全部关闭
TRAIL_M1_ST = 1   # M1 SuperTrend 跟踪（10, 3.5）
TRAIL_M5_ST = 2   # M5 SuperTrend 跟踪（10, 3.0）
TRAIL_EMA   = 3   # M5 EMA20 跟踪

# 尾盘自动平仓时间（ET，15:45 = 945分钟）
EOD_CLOSE_ET_MINUTE = 15 * 60 + 45   # 945


# ── 可插拔跟踪方法注册表 ─────────────────────────────────────────────
# 每个方法声明：展示名 + 触发 bar 类型（m1/m5）+ 计算函数 (em, sym, bar) → (value, st_dir)
# 新增方法（如 N-2 高低点）只需在此注册一条 + 控制台加一个 <option>。
@dataclass(frozen=True)
class TrailMethod:
    name: str
    bar_kind: str          # "m1" | "m5" —— 决定由哪个 bar handler 派发
    compute: Callable      # (em, sym, bar_dict) -> (value: float | None, st_dir: int)


def _st_from_bar(em, sym: str, bar: dict):
    """ST 跟踪：候选值与方向都取自当根 bar 内嵌的 st_value / st_dir。"""
    val = bar.get("st_value") or None
    return (float(val) if val else None, int(bar.get("st_dir", 0)))


def _ema20_from_m5(em, sym: str, bar: dict):
    """EMA20 跟踪：M1 bar 触发，候选值取最新 M5 bar 的 ema20；不过滤方向（st_dir=0）。"""
    return (em._get_m5_ema20(sym), 0)


TRAIL_METHODS: dict[int, TrailMethod] = {
    TRAIL_M1_ST: TrailMethod("M1-ST",    "m1", _st_from_bar),
    TRAIL_M5_ST: TrailMethod("M5-ST",    "m5", _st_from_bar),
    TRAIL_EMA:   TrailMethod("EMA20-M5", "m1", _ema20_from_m5),
}


class ExitManagerConfig(StrategyConfig, frozen=True):
    """ExitManager 配置"""
    pass


class ExitManager(Strategy):
    """
    跟踪止盈 Actor。通过 settings:{sym}.trail_mode 控制三套互斥止盈机制：

      0 = 关闭
      1 = M1 ST 跟踪（每根 M1 bar 触发，参数 10, 3.5）
      2 = M5 ST 跟踪（每根 M5 bar 触发，参数 10, 3.0）
      3 = EMA M5 跟踪（每根 M1 bar 触发，使用最新 M5 EMA20）

    设计原则：
      - 每次 bar 收盘直接从 Redis 读 trail_mode，不依赖内存状态
      - 三套互斥，只有一个生效，由前端控制
      - 棘轮机制：多头止损只上移，空头止损只下移（|变化| >= 0.01 才提交）
      - 尾盘 15:45 ET 自动平掉所有持仓（通过 order_actor HTTP /close 接口）
    """

    def __init__(self, config: ExitManagerConfig) -> None:
        super().__init__(config)
        self._redis = None
        # 防止同一天重复触发尾盘平仓（存 ET date 字符串，如 '2026-03-05'）
        self._eod_closed_dates: set[str] = set()

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
    def _is_auto_managed(self, sym: str) -> bool:
        """全自动策略接管时，ExitManager 不跟踪止盈、不代为 EOD 平仓。"""
        if not self._redis:
            return False
        try:
            raw = self._redis.get(f"settings:{sym}")
            if not raw:
                return False
            s = json.loads(raw)
            return bool(s.get("auto_strategy") or s.get("auto_observe"))
        except Exception:
            return False

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

    # ── M1 bar 收盘：处理 mode=1（M1 ST）和 mode=3（EMA）+ 尾盘平仓检测 ───
    def _on_m1_bar(self, event: BarCollectedEvent) -> None:
        sym = event.symbol
        bar = event.bar

        # ── 尾盘 15:45 ET 自动平仓检测（优先处理）─────────────────────────
        # bar.time 是 ET fake-UTC 秒，当天秒偏移直接对应 ET 时间
        bar_time = bar.get("time", 0)
        et_minute_of_day = (bar_time % 86400) // 60  # 当天分钟数（ET）
        if et_minute_of_day >= EOD_CLOSE_ET_MINUTE:
            self._check_eod_close(bar_time)

        method = TRAIL_METHODS.get(self._get_trail_mode(sym))
        if method is None or method.bar_kind != "m1" or self._is_auto_managed(sym):
            return

        iid_str = bar.get("instrument_id", "")
        if not iid_str:
            return

        val, st_dir = method.compute(self, sym, bar)
        if val:
            self._apply_trail(sym, iid_str, val, st_dir, method.name)

    # ── 尾盘平仓：每天 15:45 ET 后首次 M1 bar 触发，平掉所有持仓 ──────────
    def _check_eod_close(self, bar_time: int) -> None:
        """检查并执行尾盘自动平仓（每交易日只触发一次）。"""
        # 计算当前 ET 日期字符串（ET fake-UTC 直接取 UTC 日期）
        from datetime import datetime, timezone
        et_date = datetime.fromtimestamp(bar_time, tz=timezone.utc).strftime("%Y-%m-%d")

        if et_date in self._eod_closed_dates:
            return  # 今天已经平仓过，不重复触发

        # 查是否有持仓（从 Redis position:* 键判断）
        if not self._redis:
            return
        try:
            syms_with_pos = []
            for key in self._redis.keys("position:*"):
                raw = self._redis.get(key)
                if raw:
                    sym = key.split(":", 1)[1]
                    if not self._is_auto_managed(sym):
                        syms_with_pos.append(sym)
        except Exception as e:
            self.log.warning(f"[EOD] Redis 查询持仓列表失败: {e}")
            return

        if not syms_with_pos:
            self.log.info(f"[EOD] {et_date} 15:45 ET — 无持仓，跳过")
            self._eod_closed_dates.add(et_date)
            return

        self.log.info(
            f"[EOD] {et_date} 15:45 ET — 开始自动平仓: {syms_with_pos}"
        )
        self._eod_closed_dates.add(et_date)  # 先标记，避免重复
        self._eod_close_all(syms_with_pos, et_date)

    def _eod_close_all(self, symbols: list[str], date_label: str) -> None:
        """向 order_actor HTTP /close 接口依次发送平仓请求。"""
        for sym in symbols:
            try:
                body = json.dumps({"symbol": sym}).encode()
                conn = http.client.HTTPConnection("127.0.0.1", 8888, timeout=5)
                conn.request(
                    "POST", "/close",
                    body=body,
                    headers={"Content-Type": "application/json"},
                )
                resp = conn.getresponse()
                result = resp.read().decode()
                conn.close()
                self.log.info(
                    f"[EOD] {date_label} 平仓 {sym}: HTTP {resp.status} → {result}"
                )
            except Exception as e:
                self.log.error(f"[EOD] {date_label} 平仓 {sym} 失败: {e}")

    # ── M5 bar 收盘：处理 mode=2（M5 ST）────────────────────────────────
    def _on_m5_bar(self, event: BarCollectedM5Event) -> None:
        sym = event.symbol
        method = TRAIL_METHODS.get(self._get_trail_mode(sym))
        if method is None or method.bar_kind != "m5" or self._is_auto_managed(sym):
            return

        bar = event.bar
        iid_str = bar.get("instrument_id", "")
        if not iid_str:
            return

        val, st_dir = method.compute(self, sym, bar)
        if val:
            self._apply_trail(sym, iid_str, val, st_dir, method.name)

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

    # ── 读最新 M5 EMA20 ──────────────────────────────────────────────────
    def _get_m5_ema20(self, sym: str) -> float | None:
        if not self._redis:
            return None
        try:
            raw = self._redis.lindex(f"bars:5m:{sym}", -1)
            if not raw:
                return None
            ema20 = json.loads(raw).get("ema20")
            return float(ema20) if ema20 is not None else None
        except Exception as e:
            self.log.warning(f"[ExitManager] {sym}: 读取 M5 EMA20 失败: {e}")
            return None

    # ── 读取当前止损价（三重 fallback）────────────────────────────────────
    def _get_current_sl(self, sym: str) -> float | None:
        # 方案1：cache 中活跃的 STOP_MARKET 单
        try:
            for order in self.cache.orders():
                if order.instrument_id.symbol.value != sym:
                    continue
                if getattr(order.status, "name", "") in TERMINAL_STATUS:
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
        stop_order = None

        for order in self.cache.orders():
            if order.instrument_id != instrument_id:
                continue
            if getattr(order.status, "name", "") in TERMINAL_STATUS:
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
                        if cached and getattr(cached.status, "name", "") not in TERMINAL_STATUS:
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
