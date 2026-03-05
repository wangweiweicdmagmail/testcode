"""
鹦鹉螺引擎 (NautilusTrader) IBKR 策略（回测 / 实盘通用）

主要职责：
  1. on_start() 通过 request_bars() 从 IBKR 拉取历史 K 线预热
     - 实盘模式：拉取当日（今天）历史数据 + 订阅实时 bar/tick
     - 回测模式：拉取上一个交易日历史数据，写完 Redis 后不订阅实时
  2. on_historical_data() 批量处理历史 K 线 → 预热 ST/EMA 状态机 → 写 Redis
  3. on_bar() 实时追加 M1 K 线 → 聚合 M5 → 写 Redis → PUBLISH 通知前端
  4. on_quote_tick() 实时更新当前 K 线 close/high/low → PUBLISH tick（仅实盘）

Redis Key 约定：
  bars:1m:{SYMBOL}       — 1m K 线列表（JSON，含 ema21/st_value/st_dir/st_upper/st_lower）
  bars:5m:{SYMBOL}       — 5m K 线列表（JSON，含同样指标字段）
  kline:1m:{SYMBOL}      — PUBLISH：K 线收盘事件
  bars:1m:tick:{SYMBOL}  — PUBLISH：Tick 实时更新（当前未完成 K 线，仅实盘）
  kline:5m:{SYMBOL}      — PUBLISH：M5 K 线收盘事件

全部数据来自 IBKR，无外部数据源依赖。
"""
import json
import os
import queue
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import redis as _redis
from events import BarCollectedEvent

from nautilus_trader.indicators import AverageTrueRange, ExponentialMovingAverage
from nautilus_trader.indicators.averages import MovingAverageType

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarSpecification, BarType, QuoteTick
from nautilus_trader.model.enums import (
    AggregationSource, BarAggregation, PriceType,
)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, BarCollectedM5Event


# ── Redis 配置 ────────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
MAX_BARS   = 500   # Redis 每个 key 保留最大根数

# FA 子账户 ID（如 "DU123456"），留空则使用 FA 主账户聚合数据
FA_ACCOUNT_ID = os.environ.get("IB_FA_ACCOUNT_ID", "")

# FA Group 名称（如 "dt_test"）：配置后优先用引擎已有 IBKR 连接查询该 Group 余额
FA_GROUP = os.environ.get("IB_FA_GROUP", "dt_test")


# ============================================================
# 工具类 1：SuperTrend 增量状态机（使用鹦鹉螺内置 ATR Wilder 平滑）
# ============================================================
class _STState:
    """
    SuperTrend 在线状态机。
    ATR 使用鹦鹉螺内置 AverageTrueRange(WILDER)，与 data_feeder.py 的
    Wilder's RMA 计算方式一致。Band 收紧逻辑与 data_feeder.py 完全对齐。
    """

    def __init__(self, period: int = 10, mult: float = 3.5):
        self.period = period
        self.mult   = mult
        # 使用鹦鹉螺内置 ATR，Wilder 平滑（指数平滑，与 data_feeder.py 一致）
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close:   float = 0.0
        self._prev_upper_b: float = 0.0
        self._prev_lower_b: float = 0.0
        # TradingView 初始 direction=1 → ST=upperBand（空头），
        # 我们的约定 -1=空头，与 TV 保持一致
        self._prev_dir:     int   = -1
        self._initialized:  bool  = False

    def update(self, o: float, h: float, lo: float, c: float
               ) -> tuple[float, int, float, float]:
        """输入一根 K 线，返回 (st_val, st_dir, upper_b, lower_b)"""
        self._atr.update_raw(h, lo, c)

        if not self._atr.initialized:
            # ATR 尚未预热完成，暂返回占位值，方向维持多头
            self._prev_close = c
            return 0.0, 1, 0.0, 0.0

        atr = self._atr.value
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr

        if not self._initialized:
            # 第一次 ATR 有效：初始化 band
            upper_b = basic_upper
            lower_b = basic_lower
            self._initialized = True
        else:
            prev_upper = self._prev_upper_b
            prev_lower = self._prev_lower_b
            prev_close = self._prev_close

            # 与 data_feeder.py 完全一致的 band 收紧逻辑
            # Upper band：只有 basic_upper 更小 OR 前收盘突破上轨 才允许重置
            upper_b = basic_upper if (
                basic_upper < prev_upper or prev_close > prev_upper
            ) else prev_upper

            # Lower band：只有 basic_lower 更大 OR 前收盘跌破下轨 才允许重置
            lower_b = basic_lower if (
                basic_lower > prev_lower or prev_close < prev_lower
            ) else prev_lower

        # 方向判断（与 data_feeder.py 一致）
        if self._prev_dir == -1 and c > upper_b:
            st_dir = 1
        elif self._prev_dir == 1 and c < lower_b:
            st_dir = -1
        else:
            st_dir = self._prev_dir

        st_val = lower_b if st_dir == 1 else upper_b

        self._prev_close   = c
        self._prev_upper_b = upper_b
        self._prev_lower_b = lower_b
        self._prev_dir     = st_dir
        return round(st_val, 4), st_dir, round(upper_b, 4), round(lower_b, 4)


# ============================================================
# 工具类 2b：M5 归一化15分钟动量状态机
# ============================================================
class _MomentumATRState:
    """
    每根 M5 bar 收盘时计算：
      mom_atr = (close_now - close_2bars_ago) / ATR_14

    其中：
      - 15分钟滑动窗口 = 当前 M5 bar 距上上一根 M5 bar（3根*5min=15min）
      - ATR_14 = 14周期 Wilder ATR 的均值（预热完成后输出非 None）

    该值代表过去15分钟内相对于 ATR 的价格移动幅度，可用于多标的动量排序。
    """

    def __init__(self, atr_period: int = 14):
        self.atr_period = atr_period
        # 使用鹦鹉螺内置 ATR（Wilder 指数平滑，与 SuperTrend 一致）
        self._atr = AverageTrueRange(atr_period, MovingAverageType.WILDER)
        # 滑动 close 队列，保留最近3根（index 0=最旧，index 2=最新）
        self._closes: list[float] = []

    def update(self, h: float, lo: float, c: float
               ) -> Optional[float]:
        """
        喂入 (h, lo, c)，返回 mom_atr（ATR 预热完成且有 ≥3 根历史后才输出）。
        """
        # 更新 ATR
        self._atr.update_raw(h, lo, c)
        # 维护 close 队列，只保最近3根
        self._closes.append(c)
        if len(self._closes) > 3:
            self._closes.pop(0)

        # 预热检查：ATR 就绪 + 至少累积了3根（可取 close_2bars_ago）
        if not self._atr.initialized or len(self._closes) < 3:
            return None

        atr_val = self._atr.value
        if atr_val == 0:
            return None

        mom = (self._closes[-1] - self._closes[0]) / atr_val
        return round(mom, 4)


# ============================================================
# 工具类 2：EMA 增量状态机（使用鹦鹉螺内置 ExponentialMovingAverage）
# ============================================================
class _EMAState:
    """单标的 EMA 在线状态机，使用鹦鹉螺内置 EMA 指标"""

    def __init__(self, period: int = 21):
        self.period = period
        self._ema = ExponentialMovingAverage(period)

    def warmup(self, closes: list[float]) -> None:
        """批量喂入历史 close，完成初始化"""
        for c in closes:
            self.update(c)

    def update(self, close: float) -> Optional[float]:
        """喂入一根 K 线 close，返回当前 EMA 值（不足 period 时返回 None）"""
        self._ema.update_raw(close)
        if not self._ema.initialized:
            return None
        return round(self._ema.value, 4)


# ============================================================
# 工具类 3：M5 K 线实时聚合
# ============================================================
class _M5Bucket:
    """将 M1 K 线按时间戳 mod 300 分桶，满一个 bucket 输出一根 M5 bar"""

    def __init__(self):
        self._cur_bucket: int        = -1
        self._bars:       list[dict] = []

    def push(self, bar: dict) -> Optional[dict]:
        """
        输入一根 M1 bar_dict（含 time/open/high/low/close/volume）。
        返回：已完成的 M5 bar_dict（不含指标字段），或 None（当前 bucket 未满）。
        """
        bucket = bar["time"] - bar["time"] % 300
        out: Optional[dict] = None

        if self._cur_bucket != bucket:
            if self._bars:
                out = self._flush()
            self._cur_bucket = bucket
            self._bars = []

        self._bars.append(bar)
        return out   # 旧 bucket 完成时输出，None 表示当前 bucket 还在累积

    def _flush(self) -> dict:
        bars = self._bars
        return {
            "time":   bars[0]["time"] - bars[0]["time"] % 300,
            "open":   bars[0]["open"],
            "high":   max(b["high"] for b in bars),
            "low":    min(b["low"]  for b in bars),
            "close":  bars[-1]["close"],
            "volume": sum(b["volume"] for b in bars),
        }

    def flush_current(self) -> Optional[dict]:
        """强制输出当前未完成的 bucket（供预热结束时调用）

        注意：输出后清空 _bars，避免后续 push() 遇到 bucket 边界时
        再次调用 _flush() 触发重复计算，导致 EMA/ST 状态机被多喂一次数据。
        """
        if self._bars:
            result = self._flush()
            self._bars = []   # ← 关键：清空缓存，防止后续 push() 重复处理
            return result
        return None


# ============================================================
# 策略配置
# ============================================================
class BarLoggerStrategyConfig(StrategyConfig, frozen=True):
    """
    策略配置（回测 / 实盘通用）

    参数
    ----------
    instrument_id   : 主订阅合约（兼容旧配置）
    instrument_ids  : 多标的列表（优先使用）
    bar_step        : K 线周期步长（分钟）
    st_period/mult  : SuperTrend 参数
    ema_period      : EMA 周期
    history_days    : 预热时拉取 IBKR 历史的天数（默认 1=当天）
    backtest_mode   : True=回测（上一交易日数据，不订阅实时）；False=实盘（今日数据+实时）
    backtest_date   : 回测指定日期 'YYYY-MM-DD'，空则自动选上一个交易日
    """
    instrument_id:  InstrumentId
    instrument_ids: tuple[str, ...] = ()
    bar_step:       int   = 1
    st_period:      int   = 10
    st_mult:        float = 3.5    # M1 ST 乘数
    st_mult_m5:     float = 3.0    # M5 ST 乘数（更敏感，与 TradingView 默认对齐）
    ema_period:     int   = 21
    history_days:   int   = 1
    backtest_mode:  bool  = False
    backtest_date:  str   = ""


# ============================================================
# 策略实现
# ============================================================
class BarLoggerStrategy(Strategy):
    """
    实盘策略：
    - 通过 request_bars() 从 IBKR 拉取当日历史 K 线预热（on_historical_data）
    - 实时接收 1m K 线（on_bar），聚合 M5，写 Redis，发布 WebSocket 事件
    - 支持多标的（P6）
    """

    def __init__(self, config: BarLoggerStrategyConfig) -> None:
        super().__init__(config)
        self._redis: Optional[_redis.Redis] = None
        self._bar_count: int = 0

        # 今日 ET 日期（用于过滤非今日 bar）
        self._today_et_date: Optional[str] = None
        # 每个标的历史数据是否已刷写 Redis
        self._hist_flushed: dict[str, bool] = {}
        # 每个标的 BarType（历史数据刷写完成后用于订阅实时）
        self._bar_types: dict[str, BarType] = {}

        # 每个标的状态机（M1 维度）
        self._st_m1:  dict[str, _STState]  = {}
        self._ema_m1: dict[str, _EMAState] = {}
        self._m5_bucket: dict[str, _M5Bucket] = {}

        # 每个标的状态机（M5 维度）
        self._st_m5:  dict[str, _STState]  = {}
        self._ema_m5: dict[str, _EMAState] = {}
        self._mom_m5: dict[str, _MomentumATRState] = {}  # M5 归一化动量（10, 3.0 ATR14）

        # 历史 bar 缓冲（按标的聚合， flush 时一次性覆盖写入 Redis）
        self._hist_m1: dict[str, list[dict]] = defaultdict(list)
        self._hist_m5: dict[str, list[dict]] = defaultdict(list)  # M5 单独缓冲

        # 当前未完成的 tick K 线
        self._cur_bar: dict[str, Optional[dict]] = defaultdict(lambda: None)

        # 日内连续新高状态：{ sym: {"date": "YYYY-MM-DD", "day_high": float, "count": int} }
        self._nh_state: dict[str, dict] = {}

        # 日K 缓冲（每个标的，合并后取倒数第二根即是昨日）
        self._hist_daily: dict[str, list[dict]] = defaultdict(list)

    # ── 解析所有订阅合约 ────────────────────────────────────────────────
    def _all_instrument_ids(self) -> list[InstrumentId]:
        ids = list(self.config.instrument_ids)
        if not ids:
            ids = [str(self.config.instrument_id)]
        base = str(self.config.instrument_id)
        if base not in ids:
            ids.insert(0, base)
        return [InstrumentId.from_str(i) for i in ids]

    # ── 工具：ET 时区 fake-UTC 时间戳 ───────────────────────────────────
    @staticmethod
    def _et_fake_utc(ts_ns: int) -> int:
        """纳秒级 UTC 时间戳 → ET fake-UTC 秒"""
        ts_utc = ts_ns // 1_000_000_000
        utc_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC"))
        et_dt  = utc_dt.astimezone(ZoneInfo("America/New_York"))
        return ts_utc + int(et_dt.utcoffset().total_seconds())

    @staticmethod
    def _is_rth(et_fake_utc: int) -> bool:
        """判断 ET fake-UTC 时间戳是否处于正式交易时段（09:30-16:00 ET）。

        et_fake_utc 的时分秒直接对应 ET 本地时间，因此无需时区转换。
        """
        from datetime import timezone as _tz
        dt = datetime.fromtimestamp(et_fake_utc, tz=_tz.utc)
        et_minutes = dt.hour * 60 + dt.minute
        return (9 * 60 + 30) <= et_minutes < (16 * 60)   # [09:30, 16:00)

    # ── 生命周期 ──────────────────────────────────────────────────────
    def on_start(self) -> None:
        # 连接 Redis
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info(f"[Strategy] Redis 已连接: {REDIS_HOST}:{REDIS_PORT}")
        except Exception as e:
            self.log.error(f"[Strategy] Redis 连接失败: {e}")
            self._redis = None

        # ── 计算历史数据目标日期 ────────────────────────────────────────
        # 实盘模式：加载今日数据  回测模式：加载上一个交易日（或指定日期）数据
        import datetime as _dt_mod
        now_et = datetime.now(tz=ZoneInfo("America/New_York"))

        if self.config.backtest_mode:
            # 回测：使用指定日期 或 自动选上一个交易日
            if self.config.backtest_date:
                target = _dt_mod.date.fromisoformat(self.config.backtest_date)
                self.log.info(f"[Strategy] 回测模式 — 指定日期: {target}")
            else:
                # 向前找最近的工作日（跳过今天）
                target = now_et.date() - timedelta(days=1)
                while target.weekday() >= 5:
                    target -= timedelta(days=1)
                self.log.info(f"[Strategy] 回测模式 — 自动选上一交易日: {target}")
        else:
            # 实盘：使用今日（若今天是周末则向前找最近工作日）
            target = now_et.date()
            while target.weekday() >= 5:
                target -= timedelta(days=1)
            self.log.info(f"[Strategy] 实盘模式 — 目标日期: {target}")

        self._today_et_date = target.isoformat()
        hist_start_et = datetime(
            target.year, target.month, target.day,
            4, 0, 0, tzinfo=ZoneInfo("America/New_York")   # 04:00 盘前开始
        )
        hist_start_utc = hist_start_et.astimezone(timezone.utc)
        mode_label = "回测" if self.config.backtest_mode else "实盘"
        self.log.info(
            f"[Strategy] 模式={mode_label}  日期={self._today_et_date}  "
            f"历史起点={hist_start_et.strftime('%H:%M')} ET "
            f"({hist_start_utc.strftime('%Y-%m-%d %H:%M UTC')})"
        )

        instrument_ids = self._all_instrument_ids()
        self.log.info(
            f"[Strategy] 初始化 {len(instrument_ids)} 个标的: "
            f"{[str(i) for i in instrument_ids]}  "
            f"M1-ST({self.config.st_period},{self.config.st_mult})  "
            f"M5-ST({self.config.st_period},{self.config.st_mult_m5})  "
            f"EMA{self.config.ema_period}"
        )

        # ──【修复】等待合约加载（针对 IBKR secdefnj 慢的情况） ──
        import time as _time
        wait_start = _time.time()
        while True:
            missing = [str(iid) for iid in instrument_ids if self.cache.instrument(iid) is None]
            if not missing:
                break
            if _time.time() - wait_start > 300:  # 最多等 5 分钟
                self.log.error(f"[Strategy] ✗ 合约加载超时（5分钟上限），部分合约将缺失: {missing}")
                break
            self.log.info(f"[Strategy] ... 等待合约从 IBKR 加载中: {missing}")
            _time.sleep(10.0)

        # ──【异步初始化】将加载合约和订阅逻辑移出 on_start，防止阻塞引擎线程 ──
        init_thread = threading.Thread(target=self._async_init, daemon=True)
        init_thread.start()
        self.log.info("[Strategy] 异步初始化线程已启动 (Awaiting instruments...)")

    def _async_init(self) -> None:
        """异步执行合约加载、历史拉取和实时订阅"""
        instrument_ids = self._all_instrument_ids()
        self.log.info(f"[Strategy][Async] 开始异步初始化 {len(instrument_ids)} 个标的")

        # 1. 循环等待合约进入 cache
        import time as _time
        wait_start = _time.time()
        while True:
            missing = [str(iid) for iid in instrument_ids if self.cache.instrument(iid) is None]
            if not missing:
                self.log.info("[Strategy][Async] ✓ 所有合约已加载")
                break
            if _time.time() - wait_start > 300:  # 最多等 5 分钟
                self.log.error(f"[Strategy][Async] ✗ 合约加载超时（5分钟上限），部分合约将缺失: {missing}")
                break
            _time.sleep(5.0)

        # 2. 对每个就绪的合约执行初始化
        for iid in instrument_ids:
            sym = iid.symbol.value
            instrument = self.cache.instrument(iid)
            if instrument is None:
                continue

            # 初始化状态机（如果之前没初始化过）
            if sym not in self._st_m1:
                self._st_m1[sym]     = _STState(self.config.st_period, self.config.st_mult)
                self._ema_m1[sym]    = _EMAState(self.config.ema_period)
                self._st_m5[sym]     = _STState(self.config.st_period, self.config.st_mult_m5)
                self._ema_m5[sym]    = _EMAState(self.config.ema_period)
                self._mom_m5[sym]    = _MomentumATRState(atr_period=14)
                self._m5_bucket[sym] = _M5Bucket()
                self.log.info(f"[Strategy][Async] {sym}: 状态机初始化完成")

            # 构造 BarType 并请求历史数据
            bar_type = BarType(
                iid,
                BarSpecification(
                    step=self.config.bar_step,
                    aggregation=BarAggregation.MINUTE,
                    price_type=PriceType.LAST,
                ),
                AggregationSource.EXTERNAL,
            )
            self._bar_types[sym] = bar_type

            # 获取历史起点（今日 ET 日期已在 on_start 计算）
            target_str = self._today_et_date
            target_date = datetime.strptime(target_str, "%Y-%m-%d").date()
            hist_start_utc = datetime(
                target_date.year, target_date.month, target_date.day,
                4, 0, 0, tzinfo=ZoneInfo("America/New_York")
            ).astimezone(timezone.utc)

            self.log.info(f"[Strategy][Async] {sym}: → request_bars(M1/DAY)")
            self.request_bars(bar_type, start=hist_start_utc)

            # 请求日K
            daily_bar_type = BarType(
                iid,
                BarSpecification(step=1, aggregation=BarAggregation.DAY, price_type=PriceType.LAST),
                AggregationSource.EXTERNAL,
            )
            self.request_bars(daily_bar_type, start=hist_start_utc - timedelta(days=7))

            # 非回测模式订阅 Tick
            if not self.config.backtest_mode:
                try:
                    self.subscribe_quote_ticks(iid)
                except Exception as e:
                    self.log.warning(f"[Strategy][Async] {sym}: ✗ QuoteTick 订阅失败: {e}")

            # 设置 15s 后刷写历史并订阅实时
            t = threading.Timer(15.0, self._flush_history_for, args=(sym,))
            t.daemon = True
            t.start()

    def on_stop(self) -> None:
        # 完全转交给 OrderGatewayActor 管理，strategy 不再负责账户余额
        if self._redis:
            self._redis.close()
        self.log.info(f"[Strategy] 停止，共收到实时 K 线 {self._bar_count} 根")


        # ── 仓位事件回调：将真实 IBKR 仓位同步到 Redis ───────────────────────
    def on_position_opened(self, event) -> None:
        self._sync_position_to_redis(event.instrument_id)

    def on_position_changed(self, event) -> None:
        self._sync_position_to_redis(event.instrument_id)

    def on_position_closed(self, event) -> None:
        sym = event.instrument_id.symbol.value
        if self._redis:
            try:
                self._redis.delete(f"position:{sym}")
                self._redis.publish("position:update", json.dumps({"symbol": sym, "closed": True}))
                self.log.info(f"[Strategy] 仓位已平仓，Redis key 已删除: {sym}")
            except Exception as e:
                self.log.warning(f"[Strategy] 仓位关闭写 Redis 失败: {e}")

    def _sync_position_to_redis(self, instrument_id) -> None:
        """将 NautilusTrader 缓存中的仓位信息写入 Redis 并 PUBLISH 通知前端"""
        if not self._redis:
            return
        try:
            pos = self.cache.position(self.cache.position_id(instrument_id))
            if pos is None:
                # 尝试从 positions_open 查找
                for p in self.cache.positions_open():
                    if p.instrument_id == instrument_id:
                        pos = p
                        break
            if pos is None:
                return

            sym = instrument_id.symbol.value
            # 获取最新价格用于计算浮盈
            last_price = None
            instrument = self.cache.instrument(instrument_id)
            bars = self.cache.bars(instrument_id)
            if bars and instrument:
                last_price = float(instrument.make_price(bars[-1].close))

            upnl = None
            if last_price and instrument:
                try:
                    price_obj = instrument.make_price(last_price)
                    money = pos.unrealized_pnl(price_obj)
                    upnl = float(money.as_double()) if money else None
                except Exception:
                    pass

            pos_data = {
                "symbol":         sym,
                "side":           "LONG" if pos.is_long else "SHORT",
                "entry_price":    float(pos.avg_px_open),
                "quantity":       float(pos.quantity),
                "stop_loss":      None,          # 止损价由前端开仓时传入，此处留空
                "unrealized_pnl": upnl,
                "realized_pnl":   float(pos.realized_pnl.as_double()) if pos.realized_pnl else 0.0,
                "last_price":     last_price,
            }
            self._redis.set(f"position:{sym}", json.dumps(pos_data))
            self._redis.publish("position:update", json.dumps(pos_data))
            self.log.info(f"[Strategy] 仓位已同步到 Redis: {sym} {pos_data['side']} x{pos_data['quantity']}")
        except Exception as e:
            self.log.warning(f"[Strategy] _sync_position_to_redis 失败: {e}")

    # ── 历史 K 线回调（IBKR request_bars 响应）─────────────────────────
    def on_historical_data(self, data) -> None:
        """
        NautilusTrader 将 request_bars() 返回的历史 bar 逐根推送到此方法。
        批量处理完成后（_flush_history_for）统一写入 Redis。
        """
        if not isinstance(data, Bar):
            self.log.debug(f"[HIST] 收到非 Bar 数据: {type(data).__name__}，跳过")
            return

        sym = data.bar_type.instrument_id.symbol.value

        # ─ 日K：瘶取昨日 H/L/C 写入 Redis prev_day:{sym} ──────────────────
        if data.bar_type.spec.aggregation == BarAggregation.DAY:
            o, h, lo, c = float(data.open), float(data.high), float(data.low), float(data.close)
            et = self._et_fake_utc(data.ts_event)
            self._hist_daily[sym].append({"time": et, "high": h, "low": lo, "close": c})
            # 对每根日K，实时功合并后尝试写入（则取倒数第二个，即昨日）
            today_str = self._today_et_date  # 'YYYY-MM-DD'
            daily_bars = self._hist_daily[sym]
            # 找到最近一个不是今天的 bar
            prev = None
            for bar in reversed(daily_bars):
                bar_date = datetime.fromtimestamp(bar["time"], tz=timezone.utc).strftime("%Y-%m-%d")
                if bar_date != today_str:
                    prev = bar
                    break
            if prev and self._redis:
                try:
                    pd_data = {"high": prev["high"], "low": prev["low"], "close": prev["close"]}
                    self._redis.set(f"prev_day:{sym}", json.dumps(pd_data))
                    self.log.info(
                        f"[HIST-DAY] {sym}: 昨日围栏已写入 Redis — "
                        f"PDH={prev['high']:.2f}  PDL={prev['low']:.2f}  PDC={prev['close']:.2f}"
                    )
                except Exception as e:
                    self.log.error(f"[HIST-DAY] {sym}: 写入 Redis 失败: {e}")
            return  # 日K 不进入 M1/M5 流程

        o, h, lo, c = float(data.open), float(data.high), float(data.low), float(data.close)
        v  = int(data.volume)
        # ts_event 是 bar 开始时间（即 bar 所属分钟），直接使用
        et = self._et_fake_utc(data.ts_event)

        # ─ 盘后 bar（≥16:00 ET）：直接跳过，不计算指标不写 Redis ─
        RTH_CLOSE = 16 * 3600
        if et % 86400 >= RTH_CLOSE:
            self.log.debug(f"[HIST] {sym}: 跳过盘后 bar time={et}")
            return

        n = len(self._hist_m1[sym])   # 当前已缓冲数量
        if n == 0:
            self.log.info(
                f"[HIST] {sym}: ← 第一根历史 K 线到达  "
                f"time={et}  O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f}"
            )

        # M1 指标计算
        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)

        bar_dict = {
            "time":     et,
            "open":     round(o, 4),
            "high":     round(h, 4),
            "low":      round(lo, 4),
            "close":    round(c, 4),
            "volume":   v,
            "ema21":    ema21,
            "st_value": st_val,
            "st_dir":   st_dir,
            "st_upper": st_up,
            "st_lower": st_lo,
        }
        self._hist_m1[sym].append(bar_dict)

        # ──【增量更新】如果初始历史已刷完，则后续进来的历史 bar 直接更新 Redis（用于轮询模式） ──
        if self._hist_flushed.get(sym):
            self._inc_update_redis(sym, bar_dict)

        # 每 50 根打一次进度日志
        n += 1
        if n % 50 == 0:
            self.log.info(
                f"[HIST] {sym}: 已缓冲 {n} 根历史 K 线  "
                f"最新 C={c:.2f}  ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"EMA21={ema21:.2f}" if ema21 else
                f"[HIST] {sym}: 已缓冲 {n} 根  C={c:.2f}  EMA 预热中({self.config.ema_period}期)"
            )

        # M5 聚合（历史模式：不写 Redis，存内存缓冲，等 flush 时一次性覆盖写入）
        m5_out = self._m5_bucket[sym].push(bar_dict)
        if m5_out:
            # 计算 M5 指标，但不写 Redis
            o5, h5, lo5, c5 = m5_out["open"], m5_out["high"], m5_out["low"], m5_out["close"]
            if sym not in self._st_m5:
                self._st_m5[sym]  = _STState(self.config.st_period, self.config.st_mult_m5)
                self._ema_m5[sym] = _EMAState(self.config.ema_period)
                self._mom_m5[sym] = _MomentumATRState(atr_period=14)
            if sym not in self._mom_m5:
                self._mom_m5[sym] = _MomentumATRState(atr_period=14)
            st_val5, st_dir5, st_up5, st_lo5 = self._st_m5[sym].update(o5, h5, lo5, c5)
            ema21_5 = self._ema_m5[sym].update(c5)
            mom_atr5 = self._mom_m5[sym].update(h5, lo5, c5)
            m5_bar = {
                **m5_out,
                "ema21":    ema21_5,
                "st_value": st_val5,
                "st_dir":   st_dir5,
                "st_upper": st_up5,
                "st_lower": st_lo5,
                "mom_atr":  mom_atr5,
            }
            self._hist_m5[sym].append(m5_bar)
            self.log.debug(
                f"[HIST-M5] {sym}: 缓冲 M5 bar  "
                f"time={m5_out['time']}  C={c5:.2f}  "
                f"ST={st_val5:.2f}({'↑' if st_dir5==1 else '↓'})"
            )

    def _flush_history_for(self, sym: str) -> None:
        """
        在 on_start() 设置的 Timer 触发后执行（默认 15s）：
        1. 将 _hist_m1 内存缓冲批量写入 Redis（bars:1m:{sym}）
        2. 补刷最后一个未完成的 M5 bucket，写入 Redis（bars:5m:{sym}）
        3. 订阅实时 K 线（subscribe_bars），后续 on_bar() 开始接收实时数据
        
        重要：此方法只执行一次（_hist_flushed 防止重复）。
        """
        # 防止重复执行（Timer 可能被意外触发两次）
        if self._hist_flushed.get(sym):
            self.log.debug(f"[FLUSH] {sym}: 已刷写过，跳过")
            return
        self._hist_flushed[sym] = True

        bars = self._hist_m1.get(sym, [])
        if not bars:
            self.log.warning(
                f"[FLUSH] {sym}: ⚠ 未收到任何历史 K 线！"
                f"（IBKR 无数据或 request_bars 未完成）"
            )
        else:
            self.log.info(f"[FLUSH] {sym}: 开始批写 Redis，缓冲 M1={len(bars)} 根")

            if not self._redis:
                self.log.warning(f"[FLUSH] {sym}: ⚠ Redis 不可用，跳过写入")
            else:
                # ── 步骤1：写入 M1 历史 K 线（仅 RTH：09:30-16:00 ET）────
                # 盘前数据已用于指标预热（on_historical_data），此处只写正式交易时段
                rth_m1 = [b for b in bars if self._is_rth(b["time"])]
                self.log.info(
                    f"[FLUSH] {sym}: 过滤 RTH  "
                    f"总缓冲={len(bars)} 根  RTH={len(rth_m1)} 根  "
                    f"盘前={len(bars)-len(rth_m1)} 根（已用于指标预热，不写图表）"
                )
                try:
                    key_m1 = f"bars:1m:{sym}"
                    written = rth_m1[-MAX_BARS:]
                    # 切换为 Redis List 结构
                    self._redis.delete(key_m1)
                    if written:
                        self._redis.rpush(key_m1, *[json.dumps(b) for b in written])
                    
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:1m 写入完成  "
                        f"写入={len(written)} 根  "
                        + (f"最新 C={written[-1]['close']}  " if written else "（无有效数据）")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:1m Redis 写入失败: {e}")

                # ── 步骤2：补刷最后一个未完成的 M5 bucket ─────────────────
                m5_bars = self._hist_m5.get(sym, [])
                # 记录最后一根完整 M5 bar 的指标值（未完成 bar 展示时直接复用，不更新状态机）
                prev_complete_m5 = m5_bars[-1] if m5_bars else None
                last_m5 = self._m5_bucket[sym].flush_current()
                if last_m5:
                    c5 = last_m5["close"]
                    # 未完成 bar 仅用于展示：复用最后一根完整 M5 的指标值，不调用 update()
                    # 原因：未完成 bar 的 close 此时只是临时值，后续实时 on_bar() 还会继续更新；
                    # 若此处调用 update() 会将这个临时 close 喂入 EMA/ST 状态机，导致累计偏差
                    ema21_5 = prev_complete_m5.get("ema21")    if prev_complete_m5 else None
                    st_val5 = prev_complete_m5.get("st_value") if prev_complete_m5 else 0.0
                    st_dir5 = prev_complete_m5.get("st_dir")   if prev_complete_m5 else -1
                    st_up5  = prev_complete_m5.get("st_upper") if prev_complete_m5 else 0.0
                    st_lo5  = prev_complete_m5.get("st_lower") if prev_complete_m5 else 0.0
                    m5_bars.append({
                        **last_m5,
                        "ema21":    ema21_5,
                        "st_value": st_val5,
                        "st_dir":   st_dir5,
                        "st_upper": st_up5,
                        "st_lower": st_lo5,
                    })
                    self.log.info(f"[FLUSH] {sym}: 补刷未完成 M5 bucket（仅展示）  C={c5:.2f}  EMA21={ema21_5}")

                # ── 步骤3：整体覆盖写入 M5 历史 K 线（仅 RTH）────────────
                rth_m5 = [b for b in m5_bars if self._is_rth(b["time"])]
                key_m5 = f"bars:5m:{sym}"
                written_m5 = rth_m5[-MAX_BARS:]
                try:
                    self._redis.delete(key_m5)
                    if written_m5:
                        self._redis.rpush(key_m5, *[json.dumps(b) for b in written_m5])
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:5m 覆盖写入完成  "
                        f"RTH={len(rth_m5)} 根"
                        + (f"  最新 C={written_m5[-1]['close']}" if written_m5 else "")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:5m Redis 写入失败: {e}")

        # ── 步骤4：订阅实时 K 线（仅实盘模式）──────────────────────────
        if self.config.backtest_mode:
            self.log.info(f"[FLUSH] {sym}: ✓ 回测完成")
        else:
            bar_type = self._bar_types.get(sym)
            if bar_type:
                self.subscribe_bars(bar_type)
                self.log.info(f"[FLUSH] {sym}: ✓ 已订阅实时 M1 (Source: EXTERNAL)")
                # ──【兜底】启动每 60s 轮询拉取历史数据，防止 IBKR 实时流断路 ──
                self._schedule_poll(sym)
            else:
                self.log.error(f"[FLUSH] {sym}: ✗ 无法订阅实时")

    def _schedule_poll(self, sym: str) -> None:
        """安排下一次历史数据轮询（60s 后）"""
        t = threading.Timer(60.0, self._poll_history, args=(sym,))
        t.daemon = True
        t.start()

    def _poll_history(self, sym: str) -> None:
        """主动拉取最近 2 分钟历史数据作为实时流的兜底"""
        if self.config.backtest_mode: return
        bar_type = self._bar_types.get(sym)
        if not bar_type: return
        
        # 请求最近 45 分钟，覆盖 IBKR 服务器常见的滞后（30min 左右）
        start_utc = self.clock.utc_now() - timedelta(minutes=45)
        self.log.info(f"[POLL] {sym}: 拉取最近 45min 数据 (Start={start_utc})...")
        self.request_bars(bar_type, start=start_utc)
        # 递归安排下一次
        self._schedule_poll(sym)

    def _inc_update_redis(self, sym: str, bar_dict: dict) -> None:
        """增量更新 Redis（去重 + M1 写入 + M5 聚合 + PUBLISH），仅 RTH 正市数据"""
        if not self._redis: return
        # 盘后 bar（≥16:00 ET）：不写 Redis，不影响指标
        RTH_CLOSE = 16 * 3600
        if bar_dict["time"] % 86400 >= RTH_CLOSE:
            return
        try:
            key_m1 = f"bars:1m:{sym}"
            # 1. 去重检查：只处理比 Redis 中最后一根更晚的 bar
            last_json = self._redis.execute_command("LINDEX", key_m1, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if bar_dict["time"] <= last_bar["time"]:
                    return  # 已存在

            # 2. 写入 M1
            data_json = json.dumps(bar_dict)
            self._redis.rpush(key_m1, data_json)
            self._redis.ltrim(key_m1, -MAX_BARS, -1)
            self._redis.publish(f"kline:1m:{sym}", data_json)
            self.log.info(f"[INC-UPDATE] {sym}: M1 增量更新 (Time={bar_dict['time']} C={bar_dict['close']})")

            # 3. 驱动 M5 聚合
            m5_out = self._m5_bucket[sym].push(bar_dict)
            if m5_out:
                o5, h5, lo5, c5 = m5_out["open"], m5_out["high"], m5_out["low"], m5_out["close"]
                st_val5, st_dir5, st_up5, st_lo5 = self._st_m5[sym].update(o5, h5, lo5, c5)
                ema21_5 = self._ema_m5[sym].update(c5)
                mom_atr5 = self._mom_m5[sym].update(h5, lo5, c5) if sym in self._mom_m5 else None
                m5_bar = {
                    **m5_out, "symbol": sym, "ema21": ema21_5,
                    "st_value": st_val5, "st_dir": st_dir5, "st_upper": st_up5, "st_lower": st_lo5,
                    "mom_atr": mom_atr5,
                }
                m5_json = json.dumps(m5_bar)
                key_m5 = f"bars:5m:{sym}"
                self._redis.rpush(key_m5, m5_json)
                self._redis.ltrim(key_m5, -MAX_BARS, -1)
                self._redis.publish(f"kline:5m:{sym}", m5_json)
                self.log.info(f"[INC-UPDATE] {sym}: M5 聚合完成 (C={c5:.2f})")
        except Exception as e:
            self.log.error(f"[INC-UPDATE] {sym}: 失败 - {e}")

    # ── 实时 K 线收盘（P1）────────────────────────────────────
    def on_bar(self, bar: Bar) -> None:
        """
        实时 1分钟 K 线收盘回调（subscribe_bars 订阅后触发）。
        """
        self._bar_count += 1
        sym = bar.bar_type.instrument_id.symbol.value
        o, h, lo, c = float(bar.open), float(bar.high), float(bar.low), float(bar.close)
        v  = int(bar.volume)
        # ts_event 是 bar 开始时间（即 bar 所属分钟），直接使用，与 tick 的 et_min 对齐
        et = self._et_fake_utc(bar.ts_event)

        # ─ 过滤非今日实时 bar（防止 IBKR 追倒推送历史）────────────
        if self._today_et_date:
            ts_utc = bar.ts_event // 1_000_000_000
            bar_et_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(
                ZoneInfo("America/New_York")
            )
            if bar_et_dt.date().isoformat() != self._today_et_date:
                self.log.debug(
                    f"[BAR] {sym}: 跳过非今日 bar  "
                    f"{bar_et_dt.strftime('%Y-%m-%d %H:%M')} ET"
                )
                return

        # ─ RTH 过滤 ──────────────────────────────────────────────────────
        # 对盘前（<09:30 ET）：更新指标状态机（预热）但不写 Redis，不显示在图表
        # 对盘后（≥16:00 ET）：完全跳过（忽略）
        is_rth      = self._is_rth(et)
        is_premarket = not is_rth and (
            datetime.fromtimestamp(et, tz=timezone.utc).hour * 60
            + datetime.fromtimestamp(et, tz=timezone.utc).minute
        ) < (9 * 60 + 30)

        if not is_rth and not is_premarket:
            # 盘后 bar（≥16:00 ET）：完全跳过
            self.log.debug(
                f"[BAR] {sym}: 跳过盘后 bar  "
                f"et_hour={datetime.fromtimestamp(et, tz=timezone.utc).hour:02d}:"
                f"{datetime.fromtimestamp(et, tz=timezone.utc).minute:02d}"
            )
            return

        # ─ M1 指标计算（极端情况：历史未到就收到实时 bar，临时初始化）────
        if sym not in self._st_m1:
            self.log.warning(
                f"[BAR] {sym}: ⚠ 实时 bar 到达时历史尚未预热，临时初始化 ST/EMA 状态机"
            )
            self._st_m1[sym]  = _STState(self.config.st_period, self.config.st_mult)
            self._ema_m1[sym] = _EMAState(self.config.ema_period)

        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)

        # 盘前 bar：指标已更新（继续预热），但不写 Redis / 不显示图表
        if is_premarket:
            self._cur_bar[sym] = None   # 清空 tick，避免盘前 tick 残留
            self.log.debug(
                f"[BAR] {sym}: 盘前 bar 指标预热（不写 Redis）  "
                f"C={c:.2f}  ST={st_val:.2f}"
            )
            return

        bar_dict = {
            "symbol":   sym,    # 前端二次校验，防止数据串台
            "time":     et,     # ET fake-UTC 秒级时间戳（bar 开始时刻）
            "open":     round(o, 4),
            "high":     round(h, 4),
            "low":      round(lo, 4),
            "close":    round(c, 4),
            "volume":   v,
            "ema21":    ema21,   # None 表示 EMA 尚未预热完成
            "st_value": st_val,  # 0.0 表示 ATR 尚未预热
            "st_dir":   st_dir,  # 1=多头 -1=空头
            "st_upper": st_up,
            "st_lower": st_lo,
        }

        # 实时 K 线日志
        if ema21 is not None:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym}  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"ST_UP={st_up:.2f}  ST_LO={st_lo:.2f}  EMA21={ema21:.2f}"
            )
        else:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym}  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"EMA21=预热中"
            )

        # ─ 清空 tick K 线（该分钟已收盘，下一根分钟事件到来时重新初始化）───
        self._cur_bar[sym] = None

        # ─ M5 聚合（push 返回不为 None 表示一个 M5 bucket 已满）─────────
        if sym in self._m5_bucket:
            m5_out = self._m5_bucket[sym].push(bar_dict)
            if m5_out:
                self.log.info(
                    f"[BAR→M5] {sym}: M5 bucket 输出  "
                    f"time={m5_out['time']}  O={m5_out['open']:.2f} C={m5_out['close']:.2f}"
                )
                self._process_m5_bar(sym, m5_out, publish=True)

        # ─ 写入 Redis + PUBLISH kline:1m:──────────────────────────────
        if not self._redis:
            self.log.warning(f"[BAR] {sym}: Redis 不可用，跳过写入")
            return

        key = f"bars:1m:{sym}"
        ch  = f"kline:1m:{sym}"
        try:
            # 去重：与最后一根时间戳相同时替换（防止 IBKR 重复推送）
            last_json = self._redis.lindex(key, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if last_bar["time"] == bar_dict["time"]:
                    # 用 lset 替换末尾元素
                    self._redis.lset(key, -1, json.dumps(bar_dict))
                    self.log.debug(f"[BAR] {sym}: 替换重复时间戳 bar time={bar_dict['time']}")
                else:
                    self._redis.rpush(key, json.dumps(bar_dict))
                    self._redis.ltrim(key, -MAX_BARS, -1)
            else:
                self._redis.rpush(key, json.dumps(bar_dict))
            # kline:1m: 事件通过 Redis PubSub 推送到 server.js，再由 WebSocket 广播到前端
            self._redis.publish(ch, json.dumps(bar_dict))
            self.log.debug(
                f"[BAR] {sym}: ✓ Redis RPUSH bars:1m + PUBLISH {ch}"
            )
        except Exception as e:
            self.log.error(f"[BAR] {sym}: ✗ Redis 写入失败: {e}")

        # ★ 发布内部事件供 ExitManager 止盈逻辑使用（在 try 外面，不被 Redis 异常影响）
        bar_dict_with_id = {**bar_dict, "instrument_id": str(bar.bar_type.instrument_id)}
        self.msgbus.publish("bar.collected", BarCollectedEvent(sym, bar_dict_with_id))


    # ── M5 处理（历史/实时共用）────────────────────────────────────────
    def _process_m5_bar(self, sym: str, m5_raw: dict, publish: bool) -> None:
        """计算 M5 指标并写 Redis（publish=True 时同时 PUBLISH WebSocket 事件）"""
        o, h, lo, c = m5_raw["open"], m5_raw["high"], m5_raw["low"], m5_raw["close"]

        if sym not in self._st_m5:
            self.log.warning(f"[M5] {sym}: M5 状态机未初始化，临时创建")
            self._st_m5[sym]  = _STState(self.config.st_period, self.config.st_mult_m5)
            self._ema_m5[sym] = _EMAState(self.config.ema_period)

        st_val, st_dir, st_up, st_lo = self._st_m5[sym].update(o, h, lo, c)
        ema21_m5 = self._ema_m5[sym].update(c)

        # ─ 计算日内连续新高  ─────────────────────────────────────────────
        today_str = self._today_et_date  # 'YYYY-MM-DD'
        nh = self._nh_state.get(sym)
        if nh is None or nh["date"] != today_str:
            # 新的一天（或首次）：从 bars 重算，避免重启导致状态丢失
            nh = {"date": today_str, "day_high": c, "count": 1}
        else:
            if c > nh["day_high"]:
                nh["day_high"] = c
                nh["count"] += 1
            else:
                nh["count"] = 0  # 跌破新高，清零
        self._nh_state[sym] = nh
        nh_score = nh["count"]

        m5_bar = {
            **m5_raw,
            "ema21":    ema21_m5,
            "st_value": st_val,
            "st_dir":   st_dir,
            "st_upper": st_up,
            "st_lower": st_lo,
            "nh_score": nh_score,   # 日内连续新高计数（跌破清零）
        }

        self.log.info(
            f"[M5] {sym}: {'PUBLISH' if publish else 'HIST'}  "
            f"time={m5_raw['time']}  O={o:.2f} H={m5_raw['high']:.2f} "
            f"L={m5_raw['low']:.2f} C={c:.2f}  "
            f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
            + (f"EMA21={ema21_m5:.2f}" if ema21_m5 else "EMA21=预热中")
        )

        if not self._redis:
            self.log.warning(f"[M5] {sym}: Redis 不可用，跳过写入")
            return
        key = f"bars:5m:{sym}"
        ch  = f"kline:5m:{sym}"
        try:
            # 去重：与最后一根时间戳相同时替换
            last_json = self._redis.lindex(key, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if last_bar["time"] == m5_bar["time"]:
                    self._redis.lset(key, -1, json.dumps(m5_bar))
                else:
                    self._redis.rpush(key, json.dumps(m5_bar))
                    self._redis.ltrim(key, -MAX_BARS, -1)
            else:
                self._redis.rpush(key, json.dumps(m5_bar))
            if publish:
                self._redis.publish(ch, json.dumps(m5_bar))
            self.log.debug(
                f"[M5] {sym}: ✓ Redis RPUSH bars:5m"
                + (f" + PUBLISH {ch}" if publish else "")
            )
        except Exception as e:
            self.log.error(f"[M5] {sym}: ✗ Redis 写入失败: {e}")

        # ★ 发布内部事件供 ExitManager 执行 M5 ST 跟踪止盈（publish M5 bar 才触发）
        if publish:
            bar_type = self._bar_types.get(sym)
            iid_str = str(bar_type.instrument_id) if bar_type else f"{sym}.NASDAQ"
            self.msgbus.publish(
                "bar.collected.m5",
                BarCollectedM5Event(sym, {**m5_bar, "instrument_id": iid_str})
            )


    # ── QuoteTick 实时更新（P2）────────────────────────────────────
    def on_quote_tick(self, tick: QuoteTick) -> None:
        """
        实时 Bid/Ask Tick 回调（訂阅 subscribe_quote_ticks 后触发）。
        主要职责：展示当前分钟的实时 K 线跳动（运用中间价 mid 近似 OHLCV）。

        数据流程：
          1. 计算 bid/ask 中间价（mid）
          2. 维护 _cur_bar：首次 tick 初始化，后续 tick 更新 H/L/C
          3. PUBLISH bars:1m:tick:{sym} 到 Redis，由 server.js 广播到前端

        注意：
          - tick 的 time = et_min = et - et%60，与 on_bar() 的 et 对齐到同一分钟
          - open 只在首次 tick 时设定，后续 tick 不更新 open（人为开盘价）
          - on_bar() 收盘时设 _cur_bar[sym] = None，下一个 tick 会重新初始化
        """
        sym = tick.instrument_id.symbol.value
        # 盘后 tick（≥16:00 ET）：不推送到前端
        et_tick = self._et_fake_utc(tick.ts_event)
        if not self._is_rth(et_tick):
            return
        # 使用 bid/ask 中间价作为证券实时价格近似
        mid = (float(tick.bid_price) + float(tick.ask_price)) / 2

        cur = self._cur_bar.get(sym)
        if cur is None:
            # ― 首次 tick：初始化当前未完成 K 线 ――――――――――――――――
            et     = self._et_fake_utc(tick.ts_event)
            et_min = et - (et % 60)   # 对齐到当前分钟起始（与 on_bar 时间戳一致）
            cur    = {"symbol": sym, "time": et_min, "open": mid, "high": mid, "low": mid, "close": mid}
            self._cur_bar[sym] = cur
            self.log.debug(
                f"[TICK] {sym}: 新 tick bar 初始化  time={et_min}  mid={mid:.2f}"
            )
        else:
            # ― 后续 tick：更新最高价/最低价/收盘价；open 不变 ――――――
            cur["high"]  = max(cur["high"], mid)
            cur["low"]   = min(cur["low"],  mid)
            cur["close"] = mid

        if not self._redis:
            return
        try:
            # PUBLISH 实时 tick K 线到 Redis PubSub→server.js→WebSocket→前端
            self._redis.publish(f"bars:1m:tick:{sym}", json.dumps(cur))
        except Exception as e:
            self.log.warning(f"[TICK] {sym}: PUBLISH 失败: {e}")
