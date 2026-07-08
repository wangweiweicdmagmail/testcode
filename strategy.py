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
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import redis as _redis

from nautilus_trader.indicators import AverageTrueRange, DoubleExponentialMovingAverage, ExponentialMovingAverage
from nautilus_trader.indicators.averages import MovingAverageType

from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarSpecification, BarType, QuoteTick
from nautilus_trader.model.enums import (
    AggregationSource, BarAggregation, PriceType,
)
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, BarCollectedM5Event, BarsHistoryFlushedEvent


# ── Redis 配置 ────────────────────────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
MAX_BARS   = 500   # Redis 每个 key 保留最大根数
# 图表展示：盘前最后 60 分钟（08:30–09:30 ET）+ RTH（09:30–16:00）
PREMARKET_CHART_START_ET_SEC = 8 * 3600 + 30 * 60  # 08:30 ET
RTH_OPEN_ET_SEC = 9 * 3600 + 30 * 60

# FA 子账户 ID（如 "DU123456"），留空则使用 FA 主账户聚合数据
FA_ACCOUNT_ID = os.environ.get("IB_FA_ACCOUNT_ID", "")

# FA Group 名称（如 "dt_test"）：配置后优先用引擎已有 IBKR 连接查询该 Group 余额
FA_GROUP = os.environ.get("IB_FA_GROUP", "dt_test")


# ============================================================
# 工具类 1：SuperTrend 增量状态机（使用鹦鹉螺内置 ATR Wilder 平滑）
# ============================================================
class _STState:
    """
    SuperTrend 在线状态机（与 TradingView ta.supertrend 转向规则对齐）。
    ATR 使用鹦鹉螺内置 AverageTrueRange(WILDER)。
    转向判定：收盘价 vs 上一根 K 线的 band（非当根收紧后的 band）。
    """

    def __init__(self, period: int = 10, mult: float = 3.5):
        self.period = period
        self.mult   = mult
        # 使用鹦鹉螺内置 ATR，Wilder 平滑（指数平滑，与 data_feeder.py 一致）
        self._atr = AverageTrueRange(period, MovingAverageType.WILDER)
        self._prev_close:   float = 0.0
        self._prev_upper_b: float = 0.0
        self._prev_lower_b: float = 0.0
        self._prev_st_val:  float = 0.0   # 上一根 K 线收盘时的 ST 线（图上那条线）
        # TradingView 初始 direction=1 → ST=upperBand（空头），
        # 我们的约定 -1=空头，与 TV 保持一致
        self._prev_dir:     int   = -1
        self._initialized:  bool  = False

    def update(self, o: float, h: float, lo: float, c: float
               ) -> tuple[float, int, float, float]:
        """输入一根 K 线，返回 (st_val, st_dir, upper_b, lower_b)"""
        self._atr.update_raw(h, lo, c)

        if not self._atr.initialized:
            # ATR 尚未预热完成，暂返回占位值，方向未定（0=无方向，预热期不输出假信号）
            self._prev_close = c
            return 0.0, 0, 0.0, 0.0

        atr = self._atr.value
        hl2 = (h + lo) / 2
        basic_upper = hl2 + self.mult * atr
        basic_lower = hl2 - self.mult * atr

        # 保存上一根 ST 线（转向判断必须用这根，不能用当根收紧后的 band）
        flip_upper = self._prev_upper_b
        flip_lower = self._prev_lower_b

        if not self._initialized:
            # 第一次 ATR 有效：初始化 band
            upper_b = basic_upper
            lower_b = basic_lower
            self._initialized = True
        else:
            prev_upper = self._prev_upper_b
            prev_lower = self._prev_lower_b
            prev_close = self._prev_close

            # 与 TradingView 一致的 band 收紧逻辑
            upper_b = basic_upper if (
                basic_upper < prev_upper or prev_close > prev_upper
            ) else prev_upper

            lower_b = basic_lower if (
                basic_lower > prev_lower or prev_close < prev_lower
            ) else prev_lower

        # 转向：收盘价 vs 上一根 K 线的 band（TV: finalBand[1]），非当根 band
        # 多头时 ST 在下轨 → 翻空须 close < 上一根 lower band（即上一根 ST 值）
        # 空头时 ST 在上轨 → 翻多须 close > 上一根 upper band
        if self._prev_dir == -1 and c > flip_upper:
            st_dir = 1
        elif self._prev_dir == 1 and c < flip_lower:
            st_dir = -1
        else:
            st_dir = self._prev_dir

        st_val = lower_b if st_dir == 1 else upper_b

        self._prev_close   = c
        self._prev_upper_b = upper_b
        self._prev_lower_b = lower_b
        self._prev_st_val  = st_val
        self._prev_dir     = st_dir
        return round(st_val, 4), st_dir, round(upper_b, 4), round(lower_b, 4)


# ============================================================
# 工具类 2b：M5 归一化15分钟动量状态机
# ============================================================
class _MomentumATRState:
    """
    每根 M5 bar 收盘时计算：
      mom_atr = (M5_close_now - M5_open_1bar_ago) / M1_ATR_14

    其中：
      - 10分钟K线实体 = 当前 M5 bar 的收盘价 − 上一根 M5 bar 的开盘价
        （相当于把2根 M5 bar 组合成一根 10分钟 K 线，取其实体大小）
      - M1_ATR_14 = 外部传入的 M1 ATR（14周期 Wilder 平滑），每根 M1 bar 更新

    该值代表过去10分钟内相对于 ATR 的价格移动幅度，可用于多标的动量排序和背离计算。
    """

    def __init__(self):
        # 滑动 M5 bar open 队列，保留最近2根（index 0=最旧，index 1=最新）
        # 用 open（而非 close）构成 10分钟K线的起始价
        self._opens: list[float] = []

    def update(self, o: float, c: float, m1_atr: Optional[float]
               ) -> Optional[float]:
        """
        喂入 M5 bar 的 (open, close) 及当前 M1_ATR，
        返回 mom_atr（M1_ATR 有效且累积 ≥2 根 M5 bar 后才输出）。
        """
        # 维护 M5 bar open 队列，只保最近2根
        self._opens.append(o)
        if len(self._opens) > 2:
            self._opens.pop(0)

        # 预热检查：M1 ATR 有值 + 至少累积了2根 M5 bar（可取 open_1bar_ago）
        if m1_atr is None or m1_atr == 0 or len(self._opens) < 2:
            return None

        # 10分钟K线实体 = 当前 close − 上一根 M5 bar 的 open
        mom = (c - self._opens[0]) / m1_atr
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
# 工具类 2c：Session VWAP（M1 增量，RTH 日切 reset）
# ============================================================
class _SessionVWAPState:
    """Session VWAP = Σ(typical_price × volume) / Σ(volume)，新交易日 reset。"""

    def __init__(self) -> None:
        self._date: Optional[str] = None
        self._pv = 0.0
        self._vol = 0.0

    def reset(self) -> None:
        self._date = None
        self._pv = 0.0
        self._vol = 0.0

    def update(
        self, session_date: str, high: float, low: float, close: float, volume: int,
    ) -> Optional[float]:
        if self._date != session_date:
            self._date = session_date
            self._pv = 0.0
            self._vol = 0.0
        if volume <= 0:
            # IBKR 偶发 volume=0：不纳入累计，避免 v=1 虚假权重拉歪 VWAP
            if self._vol > 0:
                return round(self._pv / self._vol, 4)
            return None
        tp = (high + low + close) / 3.0
        self._pv += tp * float(volume)
        self._vol += float(volume)
        return round(self._pv / self._vol, 4)


# ============================================================
# 工具类 2d：DEMA(M5) — 鹦鹉螺内置 DoubleExponentialMovingAverage
# ============================================================
class _DEMA20State:
    def __init__(self, period: int = 20) -> None:
        self._dema = DoubleExponentialMovingAverage(period)

    def update(self, close: float) -> Optional[float]:
        self._dema.update_raw(close)
        if not self._dema.initialized:
            return None
        return round(self._dema.value, 4)


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

        # 同一分钟修订（IBKR 重复推送）：替换而非追加，避免 M5 OHLC/ST 双喂
        if self._bars and self._bars[-1]["time"] == bar["time"]:
            self._bars[-1] = bar
        else:
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
    st_mult:        float = 3.0    # M1 ST 乘数（与超级信号 st_super 一致）
    st_mult_m5:     float = 3.5    # M5 ST 乘数（定方向，与超级信号一致）
    ema_period:     int   = 21
    dema_period_m5: int   = 20   # M5 DEMA 周期
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
        self._ema_m5: dict[str, _EMAState] = {}  # EMA21 on M5
        self._ema9_m5: dict[str, _EMAState] = {}  # EMA9 on M5（供 ema_diff_int 使用）
        self._atr_m5: dict[str, AverageTrueRange] = {}  # ATR14 on M5（供 ema_diff_int 归一化）
        self._ema_diff_win: dict[str, list] = {}  # 滚动最多12根 (EMA9-EMA21) 窗口（1小时积分）
        self._mom_m5: dict[str, _MomentumATRState] = {}  # M5 归一化动量（15分钟K线实体/M1_ATR）

        # 每个标的 M1 ATR（14周期 Wilder，按 M1 bar 更新，供 mom_atr 使用）
        self._atr_m1: dict[str, AverageTrueRange] = {}

        # Session VWAP（M1 更新）/ DEMA20（M5 更新）
        self._vwap: dict[str, _SessionVWAPState] = {}
        self._dema20_m5: dict[str, _DEMA20State] = {}

        # 历史 bar 缓冲（按标的聚合， flush 时一次性覆盖写入 Redis）
        self._hist_m1: dict[str, list[dict]] = defaultdict(list)
        self._hist_m5: dict[str, list[dict]] = defaultdict(list)  # M5 单独缓冲

        # 当前未完成的 tick K 线
        self._cur_bar: dict[str, Optional[dict]] = defaultdict(lambda: None)

        # 日内连续新高状态：{ sym: {"date": "YYYY-MM-DD", "day_high": float, "count": int} }
        self._nh_state: dict[str, dict] = {}

        # 日K 缓冲（每个标的，合并后取倒数第二根即是昨日）
        self._hist_daily: dict[str, list[dict]] = defaultdict(list)

        # 盘前锚定（开盘突破策略）：最后一根盘前 M1 / M5
        self._premarket_last_m1: dict[str, dict] = {}
        self._premarket_last_m5: dict[str, dict] = {}
        self._premarket_ref_date: dict[str, str] = {}
        # 1m ST 对外发布快照（每 M5 桶收盘刷新，桶内 M1 复用）
        self._m1_st_published: dict[str, dict] = {}

    @staticmethod
    def _bucket5m(et_fake_utc: int) -> int:
        return int(et_fake_utc) - (int(et_fake_utc) % 300)

    def _refresh_m1_st_publish(
        self,
        sym: str,
        *,
        st_val: float,
        st_dir: int,
        st_up: float,
        st_lo: float,
        et: int,
        close: float,
        o: float,
        h: float,
        lo: float,
        bucket_closed: bool,
    ) -> dict:
        """1m ST 每根 M1 内部计算，仅在 M5 桶收盘时刷新对外字段。"""
        if bucket_closed or sym not in self._m1_st_published:
            self._m1_st_published[sym] = {
                "st_value": st_val,
                "st_dir": st_dir,
                "st_upper": st_up,
                "st_lower": st_lo,
                "m1_bar_time": et,
                "m1_close": close,
                "m1_open": o,
                "m1_high": h,
                "m1_low": lo,
            }
        return self._m1_st_published[sym]

    def _attach_m1_snap_to_m5(self, sym: str, m5_bar: dict) -> dict:
        snap = self._m1_st_published.get(sym)
        if not snap:
            return m5_bar
        return {
            **m5_bar,
            "m1_st_value": snap.get("st_value"),
            "m1_st_dir": snap.get("st_dir"),
            "m1_bar_time": snap.get("m1_bar_time"),
            "m1_close": snap.get("m1_close"),
            "m1_open": snap.get("m1_open"),
            "m1_high": snap.get("m1_high"),
            "m1_low": snap.get("m1_low"),
        }

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

    @staticmethod
    def _is_premarket_chart_et(et_fake_utc: int) -> bool:
        """盘前图表窗口：开盘前 60 分钟（08:30–09:30 ET）。"""
        if BarLoggerStrategy._is_rth(et_fake_utc):
            return False
        from datetime import timezone as _tz
        dt = datetime.fromtimestamp(et_fake_utc, tz=_tz.utc)
        sec = dt.hour * 3600 + dt.minute * 60 + dt.second
        return PREMARKET_CHART_START_ET_SEC <= sec < RTH_OPEN_ET_SEC

    @staticmethod
    def _is_chart_et(et_fake_utc: int) -> bool:
        """应写入 Redis / 前端图表的 K 线时段。"""
        return (
            BarLoggerStrategy._is_rth(et_fake_utc)
            or BarLoggerStrategy._is_premarket_chart_et(et_fake_utc)
        )

    @staticmethod
    def _is_premarket_et(et_fake_utc: int) -> bool:
        """盘前时段（< 09:30 ET），与 _is_rth 互斥。"""
        if BarLoggerStrategy._is_rth(et_fake_utc):
            return False
        from datetime import timezone as _tz
        dt = datetime.fromtimestamp(et_fake_utc, tz=_tz.utc)
        return dt.hour * 60 + dt.minute < 9 * 60 + 30

    def _session_date_from_et(self, et_fake_utc: int) -> str:
        from datetime import timezone as _tz
        dt = datetime.fromtimestamp(et_fake_utc, tz=_tz.utc)
        return dt.strftime("%Y-%m-%d")

    def _capture_premarket_from_hist(self, sym: str) -> None:
        """历史 flush 后：从缓冲中取最后一根盘前 M1/M5，写入 premarket:ref。"""
        if not self._premarket_last_m1.get(sym):
            for b in reversed(self._hist_m1.get(sym, [])):
                if self._is_premarket_et(b["time"]):
                    self._premarket_last_m1[sym] = b
                    break
        if not self._premarket_last_m5.get(sym):
            for b in reversed(self._hist_m5.get(sym, [])):
                if self._is_premarket_et(b["time"]):
                    self._premarket_last_m5[sym] = b
                    break
        session_date = self._today_et_date
        if not session_date and self._premarket_last_m1.get(sym):
            session_date = self._session_date_from_et(int(self._premarket_last_m1[sym]["time"]))
        if session_date:
            self._write_premarket_ref(sym, session_date)

    def _write_premarket_ref(self, sym: str, session_date: str) -> None:
        """盘前锚定 → Redis premarket:ref:{sym}（开盘突破策略用）。"""
        m1 = self._resolve_last_premarket_m1(sym)
        m5 = self._resolve_last_premarket_m5(sym)
        if not m1 or not m5:
            self.log.warning(f"[PremarketRef] {sym}: 缺少盘前 M1/M5，跳过写入")
            return
        if self._premarket_ref_date.get(sym) == session_date:
            return

        st_dir = int(m5.get("st_dir") or 0)
        m5_open, m5_close = float(m5["open"]), float(m5["close"])
        m5_bull = m5_close > m5_open
        m5_bear = m5_close < m5_open
        payload = {
            "symbol": sym,
            "session_date": session_date,
            "m1_anchor": {
                "time": int(m1["time"]),
                "open": float(m1["open"]),
                "high": float(m1["high"]),
                "low": float(m1["low"]),
                "close": float(m1["close"]),
            },
            "m5_last": {
                "time": int(m5["time"]),
                "open": m5_open,
                "high": float(m5["high"]),
                "low": float(m5["low"]),
                "close": m5_close,
                "st_dir": st_dir,
                "st_value": float(m5.get("st_value") or 0),
            },
            "armed_short": st_dir == -1 and m5_bear,
            "armed_long": st_dir == 1 and m5_bull,
            "ts": int(time.time()),
        }
        self._premarket_ref_date[sym] = session_date
        if not self._redis:
            return
        try:
            key = f"premarket:ref:{sym}"
            self._redis.set(key, json.dumps(payload, ensure_ascii=False))
            self._redis.publish(key, json.dumps(payload, ensure_ascii=False))
            self.log.info(
                f"[PremarketRef] {sym} {session_date}  "
                f"anchor H={payload['m1_anchor']['high']:.2f} L={payload['m1_anchor']['low']:.2f}  "
                f"armed↑={payload['armed_long']} ↓={payload['armed_short']}"
            )
        except Exception as e:
            self.log.error(f"[PremarketRef] {sym}: Redis 写入失败: {e}")

    def _resolve_last_premarket_m1(self, sym: str) -> Optional[dict]:
        """最后一根盘前 M1（< 09:30 ET），优先内存，回退历史缓冲。"""
        m1 = self._premarket_last_m1.get(sym)
        if m1 and self._is_premarket_et(m1["time"]):
            return m1
        for b in reversed(self._hist_m1.get(sym, [])):
            if self._is_premarket_et(b["time"]):
                return b
        return None

    def _resolve_last_premarket_m5(self, sym: str) -> Optional[dict]:
        m5 = self._premarket_last_m5.get(sym)
        if m5 and self._is_premarket_et(m5["time"]):
            return m5
        for b in reversed(self._hist_m5.get(sym, [])):
            if self._is_premarket_et(b["time"]):
                return b
        return None

    def _apply_session_vwap(self, sym: str, bar: dict) -> Optional[float]:
        """M1 收盘：增量 Session VWAP → 写入 bar['vwap']。"""
        if sym not in self._vwap:
            self._vwap[sym] = _SessionVWAPState()
        session_date = self._session_date_from_et(int(bar["time"]))
        vwap = self._vwap[sym].update(
            session_date,
            float(bar["high"]),
            float(bar["low"]),
            float(bar["close"]),
            int(bar.get("volume") or 0),
        )
        if vwap is not None:
            bar["vwap"] = vwap
        return vwap

    def _write_indicators_active(
        self,
        sym: str,
        m5_bar_time: int,
        st_val: float,
        st_dir: int,
        dema20: Optional[float],
    ) -> None:
        """M5 收盘：写入冻结水平线 indicators:active:{sym}。"""
        if not self._redis:
            return
        payload = {
            "m5_bar_time": m5_bar_time,
            "supertrend": {"value": st_val, "dir": st_dir},
            "dema20": dema20,
            "updated_at": int(time.time()),
        }
        try:
            self._redis.set(
                f"indicators:active:{sym}",
                json.dumps(payload, ensure_ascii=False),
            )
        except Exception as e:
            self.log.warning(f"[M5] {sym}: indicators:active 写入失败: {e}")

    def _push_ema_diff_int(self, sym: str, ema9: float | None, ema21: float | None) -> float | None:
        """向滑动窗口追加 (EMA9-EMA21) 并计算归一化积分。

        算法（方案B）：始终用实际根数求均值，窗口上限 12 根（约 1 小时）。
        - 若 ema9 / ema21 任一为 None（预热未完成），窗口不更新，返回 None
        - 若 M5 ATR14 未初始化或 ATR=0，返回 None（不除零）
        - ema9 不在窗口中时（如未完成 bar 不喂入 EMA9），不调用此方法，上层直接取上一根值

        返回：round(mean(win[-12:]) / ATR14, 4)  或  None
        """
        if ema9 is None or ema21 is None:
            return None
        win = self._ema_diff_win.setdefault(sym, [])
        win.append(ema9 - ema21)
        if len(win) > 12:
            win.pop(0)
        atr_state = self._atr_m5.get(sym)
        if atr_state is None or not atr_state.initialized:
            return None
        atr_val = atr_state.value
        if not atr_val or atr_val <= 0:
            return None
        return round(sum(win) / len(win) / atr_val, 4)

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
                self._st_m1[sym]       = _STState(self.config.st_period, self.config.st_mult)
                self._ema_m1[sym]      = _EMAState(self.config.ema_period)
                self._st_m5[sym]       = _STState(self.config.st_period, self.config.st_mult_m5)
                self._ema_m5[sym]      = _EMAState(self.config.ema_period)   # EMA21 on M5
                self._ema9_m5[sym]     = _EMAState(9)                         # EMA9 on M5
                self._atr_m5[sym]      = AverageTrueRange(14, MovingAverageType.WILDER)  # ATR14 on M5
                self._ema_diff_win[sym] = []                                   # 滚动6根差值窗口
                self._mom_m5[sym]      = _MomentumATRState()
                self._atr_m1[sym]      = AverageTrueRange(14, MovingAverageType.WILDER)  # M1 ATR 供动量窗口使用
                self._m5_bucket[sym]   = _M5Bucket()
                self._vwap[sym]        = _SessionVWAPState()
                self._dema20_m5[sym]   = _DEMA20State()
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
            # 回测模式：必须加 end 参数，否则 IBKR 默认 end=now（当前时间）
            # 若今天尚未开盘，则 IBKR 只返回目标日 04:00 → 今天 now 的数据，全部是盘前，RTH=0
            if self.config.backtest_mode:
                hist_end_utc = datetime(
                    target_date.year, target_date.month, target_date.day,
                    20, 0, 0, tzinfo=ZoneInfo("America/New_York")   # 16:00 ET + 4h 缓冲（IBKR end 是开区间）
                ).astimezone(timezone.utc)
                self.request_bars(bar_type, start=hist_start_utc, end=hist_end_utc)
            else:
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

            # ── 保留 Redis 中已有的 stop_loss（由前端/ExitManager 写入），不覆写为 None ──
            existing_sl = None
            try:
                existing_raw = self._redis.get(f"position:{sym}")
                if existing_raw:
                    existing_sl = json.loads(existing_raw).get("stop_loss")
            except Exception:
                pass

            pos_data = {
                "symbol":         sym,
                "side":           "LONG" if pos.is_long else "SHORT",
                "entry_price":    float(pos.avg_px_open),
                "quantity":       float(pos.quantity),
                "stop_loss":      existing_sl,   # 保留已有止损价，不清零
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

        # flush 后：仅增量写 Redis（poll 回放），不得先喂 VWAP/ST 再在去重处 return
        if self._hist_flushed.get(sym):
            if self._is_chart_et(et):
                self._inc_update_redis(sym, {
                    "time": et,
                    "open": round(o, 4),
                    "high": round(h, 4),
                    "low": round(lo, 4),
                    "close": round(c, 4),
                    "volume": v,
                })
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
        if sym in self._atr_m1:
            self._atr_m1[sym].update_raw(h, lo, c)

        bar_internal = {
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
        m5_out = self._m5_bucket[sym].push(bar_internal) if sym in self._m5_bucket else None
        pub = self._refresh_m1_st_publish(
            sym,
            st_val=st_val, st_dir=st_dir, st_up=st_up, st_lo=st_lo,
            et=et, close=c, o=o, h=h, lo=lo,
            bucket_closed=m5_out is not None,
        )
        bar_dict = {
            **bar_internal,
            "st_value": pub["st_value"],
            "st_dir": pub["st_dir"],
            "st_upper": pub["st_upper"],
            "st_lower": pub["st_lower"],
        }
        if self._is_rth(et):
            self._apply_session_vwap(sym, bar_dict)
        self._hist_m1[sym].append(bar_dict)
        if self._is_premarket_et(et):
            self._premarket_last_m1[sym] = bar_dict

        # 每 50 根打一次进度日志
        n += 1
        if n % 50 == 0:
            self.log.info(
                f"[HIST] {sym}: 已缓冲 {n} 根历史 K 线  "
                f"最新 C={c:.2f}  ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"EMA21={ema21:.2f}" if ema21 else
                f"[HIST] {sym}: 已缓冲 {n} 根  C={c:.2f}  EMA 预热中({self.config.ema_period}期)"
            )

        # M5 聚合（历史预热：仅内存，flush 时批量写 Redis）
        if m5_out:
            m5_bar = self._process_m5_bar(
                sym, m5_out, publish=False, write_redis=False, update_active=True,
            )
            if m5_bar:
                self._hist_m5[sym].append(m5_bar)
                if self._is_premarket_et(m5_bar["time"]):
                    self._premarket_last_m5[sym] = m5_bar
                self.log.debug(
                    f"[HIST-M5] {sym}: 缓冲 M5 bar  "
                    f"time={m5_out['time']}  C={m5_out['close']:.2f}  "
                    f"ST={m5_bar['st_value']:.2f}({'↑' if m5_bar['st_dir']==1 else '↓'})"
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
                # ── 步骤1：写入 M1 历史 K 线（盘前60分 + RTH）────
                chart_m1 = [b for b in bars if self._is_chart_et(b["time"])]
                pm_chart_n = sum(1 for b in chart_m1 if self._is_premarket_chart_et(b["time"]))
                rth_n = len(chart_m1) - pm_chart_n
                self.log.info(
                    f"[FLUSH] {sym}: 过滤图表时段  "
                    f"总缓冲={len(bars)} 根  盘前60分={pm_chart_n}  RTH={rth_n}  "
                    f"更早盘前={len(bars)-len(chart_m1)} 根（仅指标预热）"
                )
                try:
                    key_m1 = f"bars:1m:{sym}"
                    written = []
                    for b in chart_m1[-MAX_BARS:]:
                        row = dict(b)
                        if self._is_premarket_chart_et(b["time"]):
                            row["premarket"] = True
                        written.append(row)
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
                    ema21_5       = prev_complete_m5.get("ema21")        if prev_complete_m5 else None
                    st_val5       = prev_complete_m5.get("st_value")     if prev_complete_m5 else 0.0
                    st_dir5       = prev_complete_m5.get("st_dir")       if prev_complete_m5 else -1
                    st_up5        = prev_complete_m5.get("st_upper")     if prev_complete_m5 else 0.0
                    st_lo5        = prev_complete_m5.get("st_lower")     if prev_complete_m5 else 0.0
                    ema_diff_int5 = prev_complete_m5.get("ema_diff_int") if prev_complete_m5 else None
                    m5_bars.append({
                        **last_m5,
                        "ema21":        ema21_5,
                        "ema9":         None,          # 未完成 bar 不计算 EMA9
                        "st_value":     st_val5,
                        "st_dir":       st_dir5,
                        "st_upper":     st_up5,
                        "st_lower":     st_lo5,
                        "ema_diff_int": ema_diff_int5,
                    })
                    self.log.info(f"[FLUSH] {sym}: 补刷未完成 M5 bucket（仅展示）  C={c5:.2f}  EMA21={ema21_5}")

                # ── 步骤3：整体覆盖写入 M5 历史 K 线（盘前60分 + RTH）────────────
                chart_m5 = [b for b in m5_bars if self._is_chart_et(b["time"])]
                key_m5 = f"bars:5m:{sym}"
                written_m5 = []
                for b in chart_m5[-MAX_BARS:]:
                    row = dict(b)
                    if self._is_premarket_chart_et(b["time"]):
                        row["premarket"] = True
                    written_m5.append(row)
                try:
                    self._redis.delete(key_m5)
                    if written_m5:
                        self._redis.rpush(key_m5, *[json.dumps(b) for b in written_m5])
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:5m 覆盖写入完成  "
                        f"图表={len(chart_m5)} 根"
                        + (f"  最新 C={written_m5[-1]['close']}" if written_m5 else "")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:5m Redis 写入失败: {e}")

        self._capture_premarket_from_hist(sym)

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

        self.msgbus.publish(
            "bars.history.flushed",
            BarsHistoryFlushedEvent(sym, self._today_et_date or ""),
        )

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
        """增量更新 Redis（去重优先 → 再算指标 → M1 写入 + M5 聚合），盘前60分 + RTH。"""
        if not self._redis:
            return
        et = int(bar_dict["time"])
        sec = et % 86400
        if sec >= 16 * 3600:
            return
        if not self._is_chart_et(et):
            return
        try:
            key_m1 = f"bars:1m:{sym}"
            last_json = self._redis.execute_command("LINDEX", key_m1, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if bar_dict["time"] <= last_bar["time"]:
                    return

            o = float(bar_dict["open"])
            h = float(bar_dict["high"])
            lo = float(bar_dict["low"])
            c = float(bar_dict["close"])
            v = int(bar_dict.get("volume") or 0)

            if sym not in self._st_m1:
                self._st_m1[sym] = _STState(self.config.st_period, self.config.st_mult)
                self._ema_m1[sym] = _EMAState(self.config.ema_period)
                self._atr_m1[sym] = AverageTrueRange(14, MovingAverageType.WILDER)

            st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
            ema21 = self._ema_m1[sym].update(c)
            if sym in self._atr_m1:
                self._atr_m1[sym].update_raw(h, lo, c)

            enriched = {
                **bar_dict,
                "symbol": sym,
                "ema21": ema21,
                "st_value": st_val,
                "st_dir": st_dir,
                "st_upper": st_up,
                "st_lower": st_lo,
            }
            if self._is_rth(et):
                self._apply_session_vwap(sym, enriched)
            elif self._is_premarket_chart_et(et):
                enriched["premarket"] = True

            data_json = json.dumps(enriched)
            self._redis.rpush(key_m1, data_json)
            self._redis.ltrim(key_m1, -MAX_BARS, -1)
            self._redis.publish(f"kline:1m:{sym}", data_json)
            self.log.info(
                f"[INC-UPDATE] {sym}: M1 增量更新 (Time={et} C={c} vwap={enriched.get('vwap')})"
            )

            m5_out = self._m5_bucket[sym].push(enriched)
            if m5_out:
                self._process_m5_bar(sym, m5_out, publish=True)
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

        # ─ RTH / 盘前图表 过滤 ───────────────────────────────────────────
        # 04:00–08:30：仅指标预热；08:30–09:30：写入 Redis 并展示；≥16:00：忽略
        is_rth       = self._is_rth(et)
        is_pm_chart  = self._is_premarket_chart_et(et)
        is_premarket = self._is_premarket_et(et)

        if not is_rth and not is_premarket:
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
            self._atr_m1[sym] = AverageTrueRange(14, MovingAverageType.WILDER)

        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)
        if sym in self._atr_m1:
            self._atr_m1[sym].update_raw(h, lo, c)

        bar_internal = {
            "time": et,
            "open": round(o, 4),
            "high": round(h, 4),
            "low": round(lo, 4),
            "close": round(c, 4),
            "volume": v,
            "ema21": ema21,
            "st_value": st_val,
            "st_dir": st_dir,
            "st_upper": st_up,
            "st_lower": st_lo,
        }

        # 更早盘前（04:00–08:30）：只预热指标，不写 Redis
        if is_premarket and not is_pm_chart:
            pre_dict = dict(bar_internal)
            self._premarket_last_m1[sym] = pre_dict
            if sym in self._m5_bucket:
                m5_out = self._m5_bucket[sym].push(pre_dict)
                if m5_out:
                    m5_bar = self._process_m5_bar(sym, m5_out, publish=False, write_redis=False)
                    if m5_bar:
                        self._premarket_last_m5[sym] = m5_bar
            self._cur_bar[sym] = None
            self.log.debug(
                f"[BAR] {sym}: 盘前预热（<08:30，不写 Redis）  C={c:.2f}  ST={st_val:.2f}"
            )
            return

        m5_out = None
        if sym in self._m5_bucket:
            m5_out = self._m5_bucket[sym].push(bar_internal)

        pub = self._refresh_m1_st_publish(
            sym,
            st_val=st_val, st_dir=st_dir, st_up=st_up, st_lo=st_lo,
            et=et, close=c, o=o, h=h, lo=lo,
            bucket_closed=m5_out is not None,
        )

        if is_rth:
            session_date = self._session_date_from_et(et)
            if self._premarket_ref_date.get(sym) != session_date:
                self._write_premarket_ref(sym, session_date)

        bar_dict = {
            "symbol": sym,
            **bar_internal,
            "st_value": pub["st_value"],
            "st_dir": pub["st_dir"],
            "st_upper": pub["st_upper"],
            "st_lower": pub["st_lower"],
        }
        if is_rth:
            self._apply_session_vwap(sym, bar_dict)
        else:
            bar_dict["premarket"] = True
            self._premarket_last_m1[sym] = dict(bar_dict)

        # 实时 K 线日志
        tag = "盘前" if is_pm_chart else "RTH"
        if ema21 is not None:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym} [{tag}]  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"ST_UP={st_up:.2f}  ST_LO={st_lo:.2f}  EMA21={ema21:.2f}"
            )
        else:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym} [{tag}]  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"EMA21=预热中"
            )

        self._cur_bar[sym] = None

        if m5_out:
            self.log.info(
                f"[BAR→M5] {sym}: M5 bucket 输出  "
                f"time={m5_out['time']}  O={m5_out['open']:.2f} C={m5_out['close']:.2f}"
            )
            m5_bar = self._process_m5_bar(sym, m5_out, publish=True)
            if m5_bar and is_pm_chart:
                self._premarket_last_m5[sym] = m5_bar

        if not self._redis:
            self.log.warning(f"[BAR] {sym}: Redis 不可用，跳过写入")
            return

        key = f"bars:1m:{sym}"
        ch  = f"kline:1m:{sym}"
        try:
            last_json = self._redis.lindex(key, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if last_bar["time"] == bar_dict["time"]:
                    self._redis.lset(key, -1, json.dumps(bar_dict))
                    self.log.debug(f"[BAR] {sym}: 替换重复时间戳 bar time={bar_dict['time']}")
                else:
                    self._redis.rpush(key, json.dumps(bar_dict))
                    self._redis.ltrim(key, -MAX_BARS, -1)
            else:
                self._redis.rpush(key, json.dumps(bar_dict))
            self._redis.publish(ch, json.dumps(bar_dict))
            self.log.debug(
                f"[BAR] {sym}: ✓ Redis RPUSH bars:1m + PUBLISH {ch}"
            )
        except Exception as e:
            self.log.error(f"[BAR] {sym}: ✗ Redis 写入失败: {e}")

        if is_rth:
            bar_dict_with_id = {**bar_dict, "instrument_id": str(bar.bar_type.instrument_id)}
            self.msgbus.publish("bar.collected", BarCollectedEvent(sym, bar_dict_with_id))


    # ── M5 处理（历史/实时共用）────────────────────────────────────────
    def _process_m5_bar(
        self,
        sym: str,
        m5_raw: dict,
        publish: bool,
        *,
        write_redis: bool = True,
        update_active: Optional[bool] = None,
    ) -> Optional[dict]:
        """计算 M5 指标；返回带指标的 m5_bar（历史缓冲用）。publish=True 时 PUBLISH + 内部事件。"""
        o, h, lo, c = m5_raw["open"], m5_raw["high"], m5_raw["low"], m5_raw["close"]

        if sym not in self._st_m5:
            self.log.warning(f"[M5] {sym}: M5 状态机未初始化，临时创建")
            self._st_m5[sym]       = _STState(self.config.st_period, self.config.st_mult_m5)
            self._ema_m5[sym]      = _EMAState(self.config.ema_period)
            self._ema9_m5[sym]     = _EMAState(9)
            self._atr_m5[sym]      = AverageTrueRange(14, MovingAverageType.WILDER)
            self._ema_diff_win[sym] = []

        # 动量窗口状态机（如果不存在则临时初始化）
        if sym not in self._mom_m5:
            self._mom_m5[sym] = _MomentumATRState()
        if sym not in self._atr_m1:
            self._atr_m1[sym] = AverageTrueRange(14, MovingAverageType.WILDER)

        if sym not in self._dema20_m5:
            self._dema20_m5[sym] = _DEMA20State()

        st_val, st_dir, st_up, st_lo = self._st_m5[sym].update(o, h, lo, c)
        ema21_m5 = self._ema_m5[sym].update(c)
        ema9_m5  = self._ema9_m5[sym].update(c)
        dema20_m5 = self._dema20_m5[sym].update(c)
        # 更新 M5 ATR
        self._atr_m5[sym].update_raw(h, lo, c)
        # 计算 ema_diff_int（公共方法，含窗口截断 + ATR 归一化）
        ema_diff_int = self._push_ema_diff_int(sym, ema9_m5, ema21_m5)
        # 动量 = (当前M5 close − 2根前M5 bar的open) / M1_ATR_14（15分钟K线实体归一化）
        m1_atr_val = self._atr_m1[sym].value if self._atr_m1[sym].initialized else None
        mom_atr = self._mom_m5[sym].update(o, c, m1_atr_val)

        # ─ 计算日内连续新高（仅 RTH M5，盘前不计入）────────────────────
        today_str = self._today_et_date
        nh = self._nh_state.get(sym)
        if self._is_rth(m5_raw["time"]):
            if nh is None or nh["date"] != today_str:
                nh = {"date": today_str, "day_high": c, "count": 1}
            else:
                if c > nh["day_high"]:
                    nh["day_high"] = c
                    nh["count"] += 1
                else:
                    nh["count"] = 0
            self._nh_state[sym] = nh
            nh_score = nh["count"]
        else:
            nh_score = nh["count"] if nh and nh.get("date") == today_str else 0

        m5_bar = {
            **m5_raw,
            "ema21":        ema21_m5,
            "ema9":         ema9_m5,      # EMA9 on M5（供前端分类判断）
            "st_value":     st_val,
            "st_dir":       st_dir,
            "st_upper":     st_up,
            "st_lower":     st_lo,
            "dema20":       dema20_m5,
            "nh_score":     nh_score,     # 日内连续新高计数（跌破清零）
            "mom_atr":      mom_atr,      # 归一化动量窗口 = (C₀-C₋₂)/ATR₁₄
            "ema_diff_int": ema_diff_int, # 30分钟EMA差值积分/M5_ATR14
        }
        if self._is_premarket_chart_et(m5_raw["time"]):
            m5_bar["premarket"] = True

        self.log.info(
            f"[M5] {sym}: {'PUBLISH' if publish else 'HIST'}  "
            f"time={m5_raw['time']}  O={o:.2f} H={m5_raw['high']:.2f} "
            f"L={m5_raw['low']:.2f} C={c:.2f}  "
            f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
            + (f"EMA21={ema21_m5:.2f}" if ema21_m5 else "EMA21=预热中")
        )

        write_active = publish if update_active is None else update_active
        if write_active and self._is_rth(m5_raw["time"]) and st_dir in (1, -1) and st_val:
            self._write_indicators_active(sym, m5_raw["time"], st_val, st_dir, dema20_m5)

        if publish:
            bar_type = self._bar_types.get(sym)
            iid_str = str(bar_type.instrument_id) if bar_type else ""
            m5_pub = self._attach_m1_snap_to_m5(sym, {**m5_bar, "instrument_id": iid_str})
            self.msgbus.publish(
                "bar.collected.m5",
                BarCollectedM5Event(sym, m5_pub),
            )

        if not write_redis:
            return m5_bar
        if not self._redis:
            self.log.warning(f"[M5] {sym}: Redis 不可用，跳过写入")
            return m5_bar
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
                self._redis.publish(ch, json.dumps(self._attach_m1_snap_to_m5(sym, m5_bar)))
            self.log.debug(
                f"[M5] {sym}: ✓ Redis RPUSH bars:5m"
                + (f" + PUBLISH {ch}" if publish else "")
            )
        except Exception as e:
            self.log.error(f"[M5] {sym}: ✗ Redis 写入失败: {e}")

        return m5_bar


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
        if not (self._is_rth(et_tick) or self._is_premarket_chart_et(et_tick)):
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
