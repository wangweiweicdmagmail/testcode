"""
鹦鹉螺引擎 (NautilusTrader) IBKR 实盘策略

主要职责：
  1. on_start() 通过 request_bars() 从 IBKR 拉取当日历史 K 线进行预热
  2. on_historical_data() 批量处理历史 K 线 → 预热 ST/EMA 状态机 → 写 Redis
  3. on_bar() 实时追加 M1 K 线 → 聚合 M5 → 写 Redis → PUBLISH 通知前端
  4. on_quote_tick() 实时更新当前 K 线 close/high/low → PUBLISH tick

Redis Key 约定：
  bars:1m:{SYMBOL}       — 1m K 线列表（JSON，含 ema21/st_value/st_dir/st_upper/st_lower）
  bars:5m:{SYMBOL}       — 5m K 线列表（JSON，含同样指标字段）
  kline:1m:{SYMBOL}      — PUBLISH：K 线收盘事件
  bars:1m:tick:{SYMBOL} — PUBLISH：Tick 实时更新（当前未完成 K 线）
  kline:5m:{SYMBOL}      — PUBLISH：M5 K 线收盘事件

全部数据来自 IBKR，无 yfinance 依赖。
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
    实盘策略配置

    参数
    ----------
    instrument_id   : 主订阅合约（兼容旧配置）
    instrument_ids  : 多标的列表（优先使用）
    bar_step        : K 线周期步长（分钟）
    st_period/mult  : SuperTrend 参数
    ema_period      : EMA 周期
    history_days    : 预热时拉取 IBKR 历史的天数（默认 1=当天）
    """
    instrument_id:  InstrumentId
    instrument_ids: tuple[str, ...] = ()
    bar_step:       int   = 1
    st_period:      int   = 10
    st_mult:        float = 2.0
    ema_period:     int   = 21
    history_days:   int   = 1


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
        """纳秒级 UTC 时间戳 → ET fake-UTC 秒（与 data_feeder.py 一致）"""
        ts_utc = ts_ns // 1_000_000_000
        utc_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC"))
        et_dt  = utc_dt.astimezone(ZoneInfo("America/New_York"))
        return ts_utc + int(et_dt.utcoffset().total_seconds())

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

        # 计算历史数据起始时间：今日盘前 04:00 ET（包含盘前数据）
        now_et  = datetime.now(tz=ZoneInfo("America/New_York"))
        # 找最近交易日（周末就向前找工作日）
        target  = now_et.date()
        while target.weekday() >= 5:
            target -= timedelta(days=1)
        self._today_et_date = target.isoformat()   # 管个标共用同一个今日
        hist_start_et = datetime(
            target.year, target.month, target.day,
            4, 0, 0, tzinfo=ZoneInfo("America/New_York")   # 04:00 盘前开始
        )
        hist_start_utc = hist_start_et.astimezone(timezone.utc)
        self.log.info(
            f"[Strategy] 今日日期: {self._today_et_date}  "
            f"历史起点: {hist_start_et.strftime('%H:%M')} ET ({hist_start_utc.strftime('%Y-%m-%d %H:%M UTC')})"
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
                instrument_id=iid,
                bar_spec=BarSpecification(
                    step=self.config.bar_step,
                    aggregation=BarAggregation.MINUTE,
                    price_type=PriceType.LAST,
                ),
                aggregation_source=AggregationSource.EXTERNAL,
            )

            # 从 IBKR 拉取当日历史 K 线
            self._bar_types[sym] = bar_type   # 存储，刷写完成后用于订阅实时
            self.log.info(
                f"[Strategy] {sym}: → request_bars() 历史起点 {hist_start_utc.strftime('%Y-%m-%d %H:%M UTC')}"
            )
            self.request_bars(bar_type, start=hist_start_utc)

            # 注意：不在这里 subscribe_bars()
            # 将在 _flush_history_for() 写完历史数据后才订阅实时

            # 订阅 Tick（展示当前价格用，与历史/实时 bar 无冲突）
            try:
                self.subscribe_quote_ticks(iid)
                self.log.info(f"[Strategy] {sym}: ✓ 订阅 QuoteTick")
            except Exception as e:
                self.log.warning(f"[Strategy] {sym}: ✗ QuoteTick 订阅失败 — {e}")

            # 安排 Timer：15s 后刷写历史数据 + 订阅实时
            delay = 15.0
            t = threading.Timer(delay, self._flush_history_for, args=(sym,))
            t.daemon = True
            t.start()
            self.log.info(
                f"[Strategy] {sym}: ✓ Timer 已设置 ({delay:.0f}s 后刷写历史 + 订阅实时)"
            )

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info(f"[Strategy] 停止，共收到实时 K 线 {self._bar_count} 根")

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
        """历史数据加载完成后，批写 Redis，然后订阅实时 K 线"""
        # 防止重复执行
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

            # 补刷最后一个未完成的 M5 bucket
            last_m5 = self._m5_bucket[sym].flush_current()
            if last_m5:
                self.log.info(f"[FLUSH] {sym}: 补刷未完成 M5 bucket  C={last_m5['close']:.2f}")
                self._process_m5_bar(sym, last_m5, publish=False)

            if not self._redis:
                self.log.warning(f"[FLUSH] {sym}: ⚠ Redis 不可用，跳过写入")
            else:
                try:
                    last = bars[-1]
                    key  = f"bars:1m:{sym}"
                    written = bars[-MAX_BARS:]
                    self._redis.set(key, json.dumps(written))
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:1m 写入完成  "
                        f"写入={len(written)} 根  "
                        f"最新 C={last['close']}  "
                        f"ST={last.get('st_value','?')}({'↑' if last.get('st_dir')==1 else '↓'})  "
                        f"EMA21={last.get('ema21','?')}"
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:1m Redis 写入失败: {e}")

                # M5 历史数据一次性覆盖写入（清除昨日旧数据）
                m5_bars = self._hist_m5.get(sym, [])
                # 补刷最后一个未完成的 M5 bucket
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
                try:
                    self._redis.set(f"bars:5m:{sym}", json.dumps(m5_bars[-MAX_BARS:]))
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:5m 覆盖写入完成  写入={len(m5_bars)} 根"
                        + (f"  最新 C={m5_bars[-1]['close']}" if m5_bars else "")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:5m Redis 写入失败: {e}")

        # 历史数据处理完成，现在订阅实时 K 线
        bar_type = self._bar_types.get(sym)
        if bar_type:
            self.subscribe_bars(bar_type)
            self.log.info(
                f"[FLUSH] {sym}: ✓ 订阅实时 M1 K 线（历史预热完成）"
            )
        else:
            self.log.error(f"[FLUSH] {sym}: ✗ bar_type 未记录，无法订阅实时")

    # ── 实时 K 线收盘（P1）────────────────────────────────────────────
    def on_bar(self, bar: Bar) -> None:
        self._bar_count += 1
        sym = bar.bar_type.instrument_id.symbol.value
        o, h, lo, c = float(bar.open), float(bar.high), float(bar.low), float(bar.close)
        v  = int(bar.volume)
        # ts_event 是 bar 开始时间（即 bar 所属分钟），直接使用，与 tick 的 et_min 对齐
        et = self._et_fake_utc(bar.ts_event)

        # 过滤非今日实时 bar（防止 IBKR 追倒推送历史）
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

        # M1 指标计算（极端情况：历史未到就收到实时 bar，临时初始化）
        if sym not in self._st_m1:
            self.log.warning(
                f"[BAR] {sym}: ⚠ 实时 bar 到达时历史尚未预热，临时初始化 ST/EMA 状态机"
            )
            self._st_m1[sym]  = _STState(self.config.st_period, self.config.st_mult)
            self._ema_m1[sym] = _EMAState(self.config.ema_period)

        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)

        bar_dict = {
            "symbol":   sym,    # 前端二次校验，防止数据串台
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

        # 实时 K 线日志（修正三元表达式 bug）
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

        # 重置 tick bar（下一根 K 线开始前清空）
        self._cur_bar[sym] = None

        # M5 聚合
        if sym in self._m5_bucket:
            m5_out = self._m5_bucket[sym].push(bar_dict)
            if m5_out:
                self.log.info(
                    f"[BAR→M5] {sym}: M5 bucket 输出  "
                    f"time={m5_out['time']}  O={m5_out['open']:.2f} C={m5_out['close']:.2f}"
                )
                self._process_m5_bar(sym, m5_out, publish=True)

        if not self._redis:
            self.log.warning(f"[BAR] {sym}: Redis 不可用，跳过写入")
            return

        key = f"bars:1m:{sym}"
        ch  = f"kline:1m:{sym}"
        try:
            raw  = self._redis.get(key)
            all_ = json.loads(raw) if raw else []
            # 防止历史/实时 bar 时间戳重叠：末尾相同时间戳则替换，否则追加
            if all_ and all_[-1]['time'] == bar_dict['time']:
                self.log.debug(f"[BAR] {sym}: 替换重复时间戳 bar time={bar_dict['time']}")
                all_[-1] = bar_dict
            else:
                all_.append(bar_dict)
            if len(all_) > MAX_BARS:
                all_ = all_[-MAX_BARS:]
            self._redis.set(key, json.dumps(all_))
            self._redis.publish(ch, json.dumps(bar_dict))
            self.log.debug(
                f"[BAR] {sym}: ✓ Redis SET bars:1m ({len(all_)} 根) + PUBLISH {ch}"
            )
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

        m5_bar = {
            **m5_raw,
            "ema21":    ema21_m5,
            "st_value": st_val,
            "st_dir":   st_dir,
            "st_upper": st_up,
            "st_lower": st_lo,
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

    # ── QuoteTick 实时更新（P2）────────────────────────────────────────
    def on_quote_tick(self, tick: QuoteTick) -> None:
        sym = tick.instrument_id.symbol.value
        mid = (float(tick.bid_price) + float(tick.ask_price)) / 2

        cur = self._cur_bar.get(sym)
        if cur is None:
            # 首次 tick：初始化当前未完成 K 线
            et     = self._et_fake_utc(tick.ts_event)
            et_min = et - (et % 60)
            cur    = {"symbol": sym, "time": et_min, "open": mid, "high": mid, "low": mid, "close": mid}
            self._cur_bar[sym] = cur
            self.log.debug(
                f"[TICK] {sym}: 新 tick bar 初始化  time={et_min}  mid={mid:.2f}"
            )
        else:
            cur["high"]  = max(cur["high"], mid)
            cur["low"]   = min(cur["low"],  mid)
            cur["close"] = mid

        if not self._redis:
            return
        try:
            self._redis.publish(f"bars:1m:tick:{sym}", json.dumps(cur))
        except Exception as e:
            self.log.warning(f"[TICK] {sym}: PUBLISH 失败: {e}")
