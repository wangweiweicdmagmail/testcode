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
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import redis as _redis

from nautilus_trader.indicators import AverageTrueRange, ExponentialMovingAverage
from nautilus_trader.indicators.averages import MovingAverageType

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarSpecification, BarType, QuoteTick
from nautilus_trader.model.enums import (
    AggregationSource, BarAggregation, PriceType,
)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent


# ── Redis 配置 ────────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
MAX_BARS   = 500   # Redis 每个 key 保留最大根数


# ============================================================
# 工具类 1：SuperTrend 增量状态机（使用鹦鹉螺内置 ATR Wilder 平滑）
# ============================================================
class _STState:
    """
    SuperTrend 在线状态机。
    ATR 使用鹦鹉螺内置 AverageTrueRange(WILDER)，与 data_feeder.py 的
    Wilder's RMA 计算方式一致。Band 收紧逻辑与 data_feeder.py 完全对齐。
    """

    def __init__(self, period: int = 10, mult: float = 2.0):
        self.period = period
        self.mult   = mult
        # 使用鹦鹉螺内置 ATR，Wilder 平滑（指数平滑，与 data_feeder.py 一致）
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close:   float = 0.0
        self._prev_upper_b: float = 0.0
        self._prev_lower_b: float = 0.0
        self._prev_dir:     int   = 1    # 1=多头, -1=空头
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
        """强制输出当前未完成的 bucket（供预热结束时调用）"""
        if self._bars:
            return self._flush()
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
    st_mult:        float = 2.0
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

        # ── 订阅账户余额更新事件（IBKR execution client 自动 reqAccountSummary）──
        # topic="events.account.*" 匹配 events.account.{account_id}
        try:
            self.msgbus.subscribe(topic="events.account.*", handler=self._on_account_state)
            self.log.info("[Strategy] ✓ 账户事件订阅成功 (events.account.*)")
        except Exception as e:
            self.log.warning(f"[Strategy] 账户事件订阅失败（引擎尚未就绪？）: {e}")

        # 延迟 10s 后做一次初始账户余额同步（等待 execution client 完成账户摘要加载）
        t = threading.Timer(10.0, self._sync_account_to_redis)
        t.daemon = True
        t.start()

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
            f"ST({self.config.st_period},{self.config.st_mult})  EMA{self.config.ema_period}"
        )

        for iid in instrument_ids:
            sym = iid.symbol.value
            instrument = self.cache.instrument(iid)
            if instrument is None:
                self.log.error(f"[Strategy] 合约未加载: {iid}")
                continue

            # 初始化状态机
            self._st_m1[sym]     = _STState(self.config.st_period, self.config.st_mult)
            self._ema_m1[sym]    = _EMAState(self.config.ema_period)
            self._st_m5[sym]     = _STState(self.config.st_period, self.config.st_mult)
            self._ema_m5[sym]    = _EMAState(self.config.ema_period)
            self._m5_bucket[sym] = _M5Bucket()
            self.log.info(f"[Strategy] {sym}: 状态机初始化完成（ST M1/M5 + EMA21 M1/M5 + M5 聚合桶）")

            # 构造 BarType
            bar_type = BarType(
                iid,
                BarSpecification(
                    step=self.config.bar_step,
                    aggregation=BarAggregation.MINUTE,
                    price_type=PriceType.LAST,
                ),
                AggregationSource.EXTERNAL,
            )

            # 向 IBKR 拉取历史 K 线
            self._bar_types[sym] = bar_type   # 存储，刷写完成后视模式决定是否订阅实时
            self.log.info(
                f"[Strategy] {sym}: → request_bars() 起点 {hist_start_utc.strftime('%Y-%m-%d %H:%M UTC')}"
            )
            self.request_bars(bar_type, start=hist_start_utc)

            # 额外请求日K（取昨日 H/L/C 围栏）
            daily_bar_type = BarType(
                iid,
                BarSpecification(
                    step=1,
                    aggregation=BarAggregation.DAY,
                    price_type=PriceType.LAST,
                ),
                AggregationSource.EXTERNAL,
            )
            # 拉取最近 5 个交易日的日K，确保能拿到昨日（周一可拉到上周五）
            daily_start = hist_start_utc - timedelta(days=7)
            self.request_bars(daily_bar_type, start=daily_start)
            self.log.info(f"[Strategy] {sym}: → request_bars(DAY) 起点 {daily_start.strftime('%Y-%m-%d UTC')}")

            # 实盘模式才订阅 Tick（回测无需实时 tick）
            if not self.config.backtest_mode:
                try:
                    self.subscribe_quote_ticks(iid)
                    self.log.info(f"[Strategy] {sym}: ✓ 订阅 QuoteTick（实盘）")
                except Exception as e:
                    self.log.warning(f"[Strategy] {sym}: ✗ QuoteTick 订阅失败 — {e}")

            # 安排 Timer：15s 后刷写历史数据（实盘还会订阅实时）
            delay = 15.0
            t = threading.Timer(delay, self._flush_history_for, args=(sym,))
            t.daemon = True
            t.start()
            self.log.info(
                f"[Strategy] {sym}: ✓ Timer 已设置 "
                f"({delay:.0f}s 后刷写历史{'→不订阅实时' if self.config.backtest_mode else '→订阅实时'})"
            )

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info(f"[Strategy] 停止，共收到实时 K 线 {self._bar_count} 根")

    # ── 账户余额事件回调（NautilusTrader msgbus 广播）─────────────────────────
    def _on_account_state(self, event) -> None:
        """
        接收 NautilusTrader 账户余额更新事件 (AccountState)。
        IBKR execution client 每次从 reqAccountSummary 拿到最新 AccountBalance 后
        会发布此事件（约每 3 分钟一次），将余额写入 Redis 并通知前端。
        """
        try:
            from nautilus_trader.model.identifiers import Venue
            account_id_str = str(event.account_id) if hasattr(event, 'account_id') else "unknown"

            balances_data = []
            if hasattr(event, 'balances') and event.balances:
                for bal in event.balances:
                    currency = str(bal.currency)
                    total  = float(bal.total.as_double())  if bal.total  else 0.0
                    free   = float(bal.free.as_double())   if bal.free   else 0.0
                    locked = float(bal.locked.as_double()) if bal.locked else 0.0
                    balances_data.append({
                        "currency": currency,
                        "total":    round(total,  2),
                        "free":     round(free,   2),
                        "locked":   round(locked, 2),
                    })

            if not balances_data:
                self.log.debug("[Account] AccountState 事件无余额数据，跳过")
                return

            self._write_account_to_redis(account_id_str, balances_data)
        except Exception as e:
            self.log.warning(f"[Account] _on_account_state 处理失败: {e}")

    def _sync_account_to_redis(self) -> None:
        """
        初始化时主动从 cache 读取账户余额并写入 Redis（避免等待第一次事件推送）。
        在 on_start() 里延迟 10s 执行，等待 execution client 完成账户摘要加载。
        """
        try:
            from nautilus_trader.model.identifiers import Venue

            # 通过 Venue 获取账户对象
            account = self.cache.account_for_venue(Venue("IB"))
            if account is None:
                self.log.warning("[Account] cache.account_for_venue('IB') 返回 None，账户尚未注册")
                return

            account_id_str = str(account.id)
            balances_data = []

            # balances() 返回 dict[Currency, AccountBalance]
            for currency, bal in account.balances().items():
                balances_data.append({
                    "currency": str(currency),
                    "total":    round(float(bal.total.as_double()),  2),
                    "free":     round(float(bal.free.as_double()),   2),
                    "locked":   round(float(bal.locked.as_double()), 2),
                })

            if not balances_data:
                self.log.warning("[Account] 账户余额为空（可能 account summary 尚未加载），跳过")
                return

            self._write_account_to_redis(account_id_str, balances_data)
        except Exception as e:
            self.log.warning(f"[Account] _sync_account_to_redis 失败: {e}")

    def _write_account_to_redis(self, account_id_str: str, balances_data: list) -> None:
        """将账户余额数据写入 Redis 并 PUBLISH 通知前端"""
        if not self._redis:
            return
        import time
        payload = {
            "account_id": account_id_str,
            "balances": balances_data,
            "ts": int(time.time()),
        }
        try:
            self._redis.set("account:funds", json.dumps(payload))
            self._redis.publish("account:update", json.dumps(payload))
            # 打印主要 USD 余额
            usd = next((b for b in balances_data if b["currency"] == "USD"), None)
            if usd:
                self.log.info(
                    f"[Account] 余额已同步 Redis  "
                    f"total={usd['total']:,.2f}  free={usd['free']:,.2f}  "
                    f"locked={usd['locked']:,.2f}  USD"
                )
            else:
                self.log.info(f"[Account] 余额已同步 Redis  {balances_data}")
        except Exception as e:
            self.log.warning(f"[Account] Redis 写入失败: {e}")

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
                self._st_m5[sym]  = _STState(self.config.st_period, self.config.st_mult)
                self._ema_m5[sym] = _EMAState(self.config.ema_period)
            st_val5, st_dir5, st_up5, st_lo5 = self._st_m5[sym].update(o5, h5, lo5, c5)
            ema21_5 = self._ema_m5[sym].update(c5)
            m5_bar = {
                **m5_out,
                "ema21":    ema21_5,
                "st_value": st_val5,
                "st_dir":   st_dir5,
                "st_upper": st_up5,
                "st_lower": st_lo5,
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
                    rth_last = rth_m1[-1] if rth_m1 else None
                    key  = f"bars:1m:{sym}"
                    written = rth_m1[-MAX_BARS:]
                    self._redis.set(key, json.dumps(written))
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:1m 写入完成  "
                        f"写入={len(written)} 根  "
                        + (f"最新 C={rth_last['close']}  "
                           f"ST={rth_last.get('st_value','?')}({'↑' if rth_last.get('st_dir')==1 else '↓'})  "
                           f"EMA21={rth_last.get('ema21','?')}" if rth_last else "（无 RTH 数据）")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:1m Redis 写入失败: {e}")

                # ── 步骤2：补刷最后一个未完成的 M5 bucket ─────────────────
                m5_bars = self._hist_m5.get(sym, [])
                last_m5 = self._m5_bucket[sym].flush_current()
                if last_m5:
                    o5, h5, lo5, c5 = last_m5["open"], last_m5["high"], last_m5["low"], last_m5["close"]
                    st_val5, st_dir5, st_up5, st_lo5 = self._st_m5[sym].update(o5, h5, lo5, c5)
                    ema21_5 = self._ema_m5[sym].update(c5)
                    m5_bars.append({
                        **last_m5,
                        "ema21":    ema21_5,
                        "st_value": st_val5,
                        "st_dir":   st_dir5,
                        "st_upper": st_up5,
                        "st_lower": st_lo5,
                    })
                    self.log.info(f"[FLUSH] {sym}: 补刷未完成 M5 bucket  C={c5:.2f}")

                # ── 步骤3：整体覆盖写入 M5 历史 K 线（仅 RTH）────────────
                rth_m5 = [b for b in m5_bars if self._is_rth(b["time"])]
                try:
                    self._redis.set(f"bars:5m:{sym}", json.dumps(rth_m5[-MAX_BARS:]))
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:5m 覆盖写入完成  "
                        f"RTH={len(rth_m5)} 根"
                        + (f"  最新 C={rth_m5[-1]['close']}" if rth_m5 else "")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:5m Redis 写入失败: {e}")

        # ── 步骤4：订阅实时 K 线（仅实盘模式）──────────────────────────
        # 回测模式：数据已写入 Redis，不需要实时订阅，数据回放完毕
        # 实盘模式：历史数据写完后才订阅实时 bar，确保数据顺序正确
        if self.config.backtest_mode:
            self.log.info(
                f"[FLUSH] {sym}: ✓ 回测完成，数据已写入 Redis（不订阅实时）"
            )
        else:
            bar_type = self._bar_types.get(sym)
            if bar_type:
                self.subscribe_bars(bar_type)
                self.log.info(
                    f"[FLUSH] {sym}: ✓ 订阅实时 M1 K 线（历史预热完成）"
                )
            else:
                self.log.error(f"[FLUSH] {sym}: ✗ bar_type 未记录，无法订阅实时")

    # ── 实时 K 线收盘（P1）────────────────────────────────────
    def on_bar(self, bar: Bar) -> None:
        """
        实时 1分钟 K 线收盘回调（subscribe_bars 订阅后触发）。
        居然流程：
          1. 取消当前未完成 tick bar（该分钟已完结）
          2. 计算 M1 ST/EMA 指标
          3. 尝试 M5 聚合（满 5 分钟输出一根 M5 bar）
          4. 将 M1 bar 写入 Redis（防重复时间戳）
          5. PUBLISH kline:1m:{sym}（前端 WebSocket 世订阅）
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
            raw  = self._redis.get(key)
            all_ = json.loads(raw) if raw else []
            # 防止历史/实时 bar 时间戳重叠：末尾相同时间戳则替换，否则追加
            # （历史 flush 后立即订阅实时，IBKR 可能重复推送最后一根）
            if all_ and all_[-1]['time'] == bar_dict['time']:
                self.log.debug(f"[BAR] {sym}: 替换重复时间戳 bar time={bar_dict['time']}")
                all_[-1] = bar_dict
            else:
                all_.append(bar_dict)
            if len(all_) > MAX_BARS:
                all_ = all_[-MAX_BARS:]
            self._redis.set(key, json.dumps(all_))
            # kline:1m: 事件通过 Redis PubSub 推送到 server.js，再由 WebSocket 广播到前端
            self._redis.publish(ch, json.dumps(bar_dict))
            self.log.debug(
                f"[BAR] {sym}: ✓ Redis SET bars:1m ({len(all_)} 根) + PUBLISH {ch}"
            )

            # ★ 发布内部事件供 ExitManager 止盈逻辑使用
            bar_dict_with_id = {**bar_dict, "instrument_id": str(self.config.instrument_id)}
            self.msgbus.publish("bar.collected", BarCollectedEvent(sym, bar_dict_with_id))
        except Exception as e:
            self.log.error(f"[BAR] {sym}: ✗ Redis 写入失败: {e}")

    # ── M5 处理（历史/实时共用）────────────────────────────────────────
    def _process_m5_bar(self, sym: str, m5_raw: dict, publish: bool) -> None:
        """计算 M5 指标并写 Redis（publish=True 时同时 PUBLISH WebSocket 事件）"""
        o, h, lo, c = m5_raw["open"], m5_raw["high"], m5_raw["low"], m5_raw["close"]

        if sym not in self._st_m5:
            self.log.warning(f"[M5] {sym}: M5 状态机未初始化，临时创建")
            self._st_m5[sym]  = _STState(self.config.st_period, self.config.st_mult)
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
            raw  = self._redis.get(key)
            all_ = json.loads(raw) if raw else []
            all_.append(m5_bar)
            if len(all_) > MAX_BARS:
                all_ = all_[-MAX_BARS:]
            self._redis.set(key, json.dumps(all_))
            if publish:
                self._redis.publish(ch, json.dumps(m5_bar))
            self.log.debug(
                f"[M5] {sym}: ✓ Redis SET bars:5m ({len(all_)} 根)"
                + (f" + PUBLISH {ch}" if publish else "")
            )
        except Exception as e:
            self.log.error(f"[M5] {sym}: ✗ Redis 写入失败: {e}")

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
