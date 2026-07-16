"""M5IndicatorStrategy — M5 周期指标计算策略（设计原则 #1：每周期一个 Strategy）。

取代老 ``strategy.BarLoggerStrategy`` 的 M5 职责，**关键变化：M5 改引擎原生
``subscribe_bars(step=5, MINUTE, EXTERNAL)`` 订阅**，不再用 ``_M5Bucket`` 自聚合。

  - 历史 / 实时 M5 K 线 → ST / EMA21 / EMA9 / ATR14 / DEMA20 / SessionVWAP 衍生
    （Momentum / ema_diff_int / 日内新高 nh_score）
  - 写 Redis ``bars:5m`` / ``kline:5m`` + 发布 ``bar.collected.m5`` 事件（字段不变）
  - 写 ``indicators:active``（M5 ST 冻结水平线）
  - **跨周期输入**：从共享 ``IndicatorRegistry`` 读 M1 ST 快照（贴 m1_* 字段，取代
    老 _attach_m1_snap_to_m5）、M1 ATR（喂 mom_atr）、M1 盘前锚定（组装 premarket:ref）

对外接口（消费者零改动）：
  - ``bar.collected.m5`` 事件 bar_dict 字段：ema21/ema9/st_*/dema20/mom_atr/ema_diff_int
    /nh_score + m1_* 快照 + instrument_id
  - Redis：bars:5m / kline:5m / indicators:active / premarket:ref
"""
from __future__ import annotations

import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import redis as _redis

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import AverageTrueRange, DonchianChannel
from nautilus_trader.indicators.averages import MovingAverageType
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from indicators import (
    DEMAState,
    EMAState,
    IndicatorRegistry,
    MomentumATRState,
    STState,
)
from strategies._common import (
    MAX_BARS,
    REDIS_HOST,
    REDIS_PORT,
    dedup_rpush_bar,
    et_fake_utc,
    is_chart_et,
    is_premarket_chart_et,
    is_premarket_et,
    is_rth,
    session_date_from_et,
)
from events import BarCollectedM5Event


class M5IndicatorStrategyConfig(StrategyConfig, frozen=True):
    """M5 指标策略配置。registry 由构造时注入（同 M1）。"""
    instrument_id: InstrumentId
    instrument_ids: tuple[str, ...] = ()
    bar_step: int = 5
    st_period: int = 10
    st_mult: float = 3.5     # M5 ST 乘数（定方向，与超级信号一致）
    ema_period: int = 21
    ema9_period: int = 9
    atr_period: int = 14     # M5 ATR 周期（ema_diff_int 归一化）
    dema_period: int = 20
    dc_period: int = 20           # 唐安琪通道周期（定势状态机用，值传前端）
    trend_ema_period: int = 20    # 定势状态机 EMA20（价值中枢，值传前端）
    history_days: int = 2
    backtest_mode: bool = False
    backtest_date: str = ""


class M5IndicatorStrategy(Strategy):
    """M5 周期指标 + 数据落盘策略（引擎原生订阅，跨周期读 registry）。"""

    def __init__(
        self,
        config: M5IndicatorStrategyConfig,
        registry: Optional[IndicatorRegistry] = None,
    ) -> None:
        super().__init__(config)
        self._registry: Optional[IndicatorRegistry] = registry

        self._redis: Optional[_redis.Redis] = None
        self._bar_count: int = 0

        self._today_et_date: Optional[str] = None
        self._hist_flushed: dict[str, bool] = {}
        self._bar_types: dict[str, BarType] = {}

        # 每个标的 M5 状态机
        self._st_m5: dict[str, STState] = {}
        self._ema_m5: dict[str, EMAState] = {}       # EMA21 on M5
        self._ema9_m5: dict[str, EMAState] = {}      # EMA9 on M5（ema_diff_int）
        self._atr_m5: dict[str, AverageTrueRange] = {}  # ATR14 on M5（ema_diff_int 归一化）
        self._ema_diff_win: dict[str, list] = {}     # 滚动最多 12 根 (EMA9-EMA21)
        self._mom_m5: dict[str, MomentumATRState] = {}
        self._dema20_m5: dict[str, DEMAState] = {}
        # 定势状态机指标（DC 通道 + EMA20，值随 m5_bar 传前端，状态机逻辑在前端 JS 重放）
        self._dc_m5: dict[str, DonchianChannel] = {}
        self._ema20_m5: dict[str, EMAState] = {}
        # 日内连续新高：{ sym: {"date","day_high","count"} }
        self._nh_state: dict[str, dict] = {}

        # 历史 M5 缓冲（flush 时批量覆盖写 Redis）
        self._hist_m5: dict[str, list[dict]] = defaultdict(list)

        # 盘前锚定：最后一根盘前 M5（premarket:ref 由本策略组装写入）
        self._premarket_last_m5: dict[str, dict] = {}
        self._premarket_ref_date: dict[str, str] = {}

    # ── 解析所有订阅合约 ────────────────────────────────────────────────
    def _all_instrument_ids(self) -> list[InstrumentId]:
        ids = list(self.config.instrument_ids)
        base = str(self.config.instrument_id)
        if base not in ids:
            ids.insert(0, base)
        return [InstrumentId.from_str(i) for i in ids]

    def _init_m5_states(self, sym: str) -> None:
        """兜底初始化 M5 全套状态机（正常由 _async_init 预先初始化）。"""
        self._st_m5[sym] = STState(self.config.st_period, self.config.st_mult)
        self._ema_m5[sym] = EMAState(self.config.ema_period)
        self._ema9_m5[sym] = EMAState(self.config.ema9_period)
        self._atr_m5[sym] = AverageTrueRange(self.config.atr_period, MovingAverageType.WILDER)
        self._ema_diff_win[sym] = []
        self._mom_m5[sym] = MomentumATRState()
        self._dema20_m5[sym] = DEMAState(self.config.dema_period)
        self._dc_m5[sym] = DonchianChannel(self.config.dc_period)
        self._ema20_m5[sym] = EMAState(self.config.trend_ema_period)

    def _push_ema_diff_int(self, sym: str, ema9: float | None, ema21: float | None) -> float | None:
        """向滑动窗口追加 (EMA9-EMA21) 并用 M5 ATR14 归一化（方案 B：实际根数均值，上限 12 根）。"""
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

    def _update_nh(self, sym: str, bar_time: int, close: float) -> int:
        """日内连续新高计数（仅 RTH，跌破清零），返回 nh_score。"""
        today_str = self._today_et_date
        nh = self._nh_state.get(sym)
        if is_rth(bar_time):
            if nh is None or nh["date"] != today_str:
                nh = {"date": today_str, "day_high": close, "count": 1}
            else:
                if close > nh["day_high"]:
                    nh["day_high"] = close
                    nh["count"] += 1
                else:
                    nh["count"] = 0
            self._nh_state[sym] = nh
            return nh["count"]
        return nh["count"] if nh and nh.get("date") == today_str else 0

    def _write_indicators_active(
        self, sym: str, m5_bar_time: int, st_val: float, st_dir: int, dema20: Optional[float],
        atr: Optional[float] = None,
    ) -> None:
        """M5 收盘：写入冻结水平线 indicators:active:{sym}（含 M5 ATR14，供控制台止损默认）。"""
        if not self._redis:
            return
        payload = {
            "m5_bar_time": m5_bar_time,
            "supertrend": {"value": st_val, "dir": st_dir},
            "dema20": dema20,
            "atr": atr,
            "updated_at": int(__import__("time").time()),
        }
        try:
            self._redis.set(f"indicators:active:{sym}", json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            self.log.warning(f"[M5] {sym}: indicators:active 写入失败: {e}")

    def _attach_m1_snap_from_registry(self, sym: str, m5_bar: dict) -> dict:
        """从 registry 读 M1 ST 快照，贴 m1_* 字段（取代老 _attach_m1_snap_to_m5）。"""
        snap = self._registry.get_all(sym, "m1") if self._registry else None
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

    def _publish_m5_snapshot_to_registry(self, sym: str, m5_bar: dict) -> None:
        """把 M5 指标快照写入共享 registry，供 signal_detector 等读取（取代读 bar_dict）。

        单点调用（_compute_m5_bar 尾部），统一覆盖历史预热 / 增量 / 实时三条路径。
        字段与 M1 快照同构 + dema20/atr；不写 ema21/ema9/mom_atr/ema_diff_int（无跨周期消费者）。
        """
        if not self._registry:
            return
        atr_val = (
            self._atr_m5[sym].value
            if sym in self._atr_m5 and self._atr_m5[sym].initialized
            else None
        )
        self._registry.set_many(sym, "m5", {
            "st_value": m5_bar.get("st_value"),
            "st_dir": m5_bar.get("st_dir"),
            "st_upper": m5_bar.get("st_upper"),
            "st_lower": m5_bar.get("st_lower"),
            "dema20": m5_bar.get("dema20"),
            "atr": atr_val,
            "m5_bar_time": m5_bar.get("time"),
            "m5_open": m5_bar.get("open"),
            "m5_high": m5_bar.get("high"),
            "m5_low": m5_bar.get("low"),
            "m5_close": m5_bar.get("close"),
        })

    def _compute_m5_bar(self, sym: str, m5_raw: dict) -> dict:
        """算 M5 全套指标，返回带指标的 m5_bar（不含 m1_*，attach 单独做）。

        历史 / 实时共用：先喂状态机，再组装 bar_dict。
        """
        o, h, lo, c = m5_raw["open"], m5_raw["high"], m5_raw["low"], m5_raw["close"]
        if sym not in self._st_m5:
            self.log.warning(f"[M5] {sym}: M5 状态机未初始化，临时创建")
            self._init_m5_states(sym)

        st_val, st_dir, st_up, st_lo = self._st_m5[sym].update(o, h, lo, c)
        ema21_m5 = self._ema_m5[sym].update(c)
        ema9_m5 = self._ema9_m5[sym].update(c)
        dema20_m5 = self._dema20_m5[sym].update(c)
        self._atr_m5[sym].update_raw(h, lo, c)
        # 定势状态机指标：DC 通道 + EMA20（值传前端，状态机逻辑在前端 JS 重放）
        self._dc_m5[sym].update_raw(h, lo)
        ema20_m5 = self._ema20_m5[sym].update(c)
        dc = self._dc_m5[sym]
        dc_init = dc.initialized
        dc_upper = round(dc.upper, 4) if dc_init else None
        dc_lower = round(dc.lower, 4) if dc_init else None
        dc_mid = round(dc.middle, 4) if dc_init else None
        atr_val = self._atr_m5[sym].value if self._atr_m5[sym].initialized else None
        ema_diff_int = self._push_ema_diff_int(sym, ema9_m5, ema21_m5)
        # M1 ATR 从 registry 读（跨周期，预热期可能 None → mom_atr 返回 None）
        m1_atr_val = self._registry.get(sym, "m1", "atr") if self._registry else None
        mom_atr = self._mom_m5[sym].update(o, c, m1_atr_val)
        nh_score = self._update_nh(sym, m5_raw["time"], c)

        m5_bar = {
            **m5_raw,
            "ema21": ema21_m5,
            "ema9": ema9_m5,
            "st_value": st_val, "st_dir": st_dir, "st_upper": st_up, "st_lower": st_lo,
            "dema20": dema20_m5,
            "nh_score": nh_score,
            "mom_atr": mom_atr,
            "ema_diff_int": ema_diff_int,
            # 定势状态机字段（DC 通道 + EMA20 + ATR，状态机逻辑在前端 JS 重放）
            "dc_upper": dc_upper,
            "dc_lower": dc_lower,
            "dc_mid": dc_mid,
            "ema20": ema20_m5,
            "atr": atr_val,
        }
        if is_premarket_chart_et(m5_raw["time"]):
            m5_bar["premarket"] = True
        self._publish_m5_snapshot_to_registry(sym, m5_bar)
        return m5_bar

    # ── 盘前锚定（premarket:ref 由 M5 组装写入）────────────────────────
    def _resolve_last_premarket_m5(self, sym: str) -> Optional[dict]:
        """最后一根盘前 M5（< 09:30 ET），优先内存，回退历史缓冲。"""
        m5 = self._premarket_last_m5.get(sym)
        if m5 and is_premarket_et(m5["time"]):
            return m5
        for b in reversed(self._hist_m5.get(sym, [])):
            if is_premarket_et(b["time"]):
                return b
        return None

    def _resolve_m1_premarket_from_registry(self, sym: str) -> Optional[dict]:
        """从 registry 读 M1 策略写入的最后一根盘前 M1（premarket:ref 的 m1_anchor 来源）。"""
        if not self._registry:
            return None
        m1 = self._registry.get(sym, "m1", "premarket_last")
        if m1 and is_premarket_et(int(m1["time"])):
            return m1
        return None

    def _capture_premarket_m5(self, sym: str) -> None:
        """历史 flush 后：取最后一根盘前 M5 到内存（premarket:ref 在 RTH 首根 on_bar 时写）。"""
        if not self._premarket_last_m5.get(sym):
            for b in reversed(self._hist_m5.get(sym, [])):
                if is_premarket_et(b["time"]):
                    self._premarket_last_m5[sym] = b
                    break

    def _write_premarket_ref(self, sym: str, session_date: str) -> None:
        """盘前锚定 → Redis premarket:ref:{sym}（开盘突破策略用）。

        m1_anchor 从 registry 读（M1 策略写入），m5_last 取本策略内存。
        """
        m1 = self._resolve_m1_premarket_from_registry(sym)
        m5 = self._resolve_last_premarket_m5(sym)
        if not m1 or not m5:
            self.log.warning(f"[PremarketRef] {sym}: 缺少盘前 M1/M5，跳过写入")
            return
        if self._premarket_ref_date.get(sym) == session_date:
            return

        st_dir = int(m5.get("st_dir") or 0)
        m5_open, m5_close = float(m5["open"]), float(m5["close"])
        payload = {
            "symbol": sym,
            "session_date": session_date,
            "m1_anchor": {
                "time": int(m1["time"]),
                "open": float(m1["open"]), "high": float(m1["high"]),
                "low": float(m1["low"]), "close": float(m1["close"]),
            },
            "m5_last": {
                "time": int(m5["time"]),
                "open": m5_open, "high": float(m5["high"]),
                "low": float(m5["low"]), "close": m5_close,
                "st_dir": st_dir, "st_value": float(m5.get("st_value") or 0),
            },
            "armed_short": st_dir == -1 and m5_close < m5_open,
            "armed_long": st_dir == 1 and m5_close > m5_open,
            "ts": int(__import__("time").time()),
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

    # ── 生命周期 ──────────────────────────────────────────────────────
    def on_start(self) -> None:
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info(f"[M5] Redis 已连接: {REDIS_HOST}:{REDIS_PORT}")
        except Exception as e:
            self.log.error(f"[M5] Redis 连接失败: {e}")
            self._redis = None

        # 计算历史数据目标日期（与 M1 同口径，两个策略各自算，结果一致）
        import datetime as _dt_mod
        now_et = datetime.now(tz=ZoneInfo("America/New_York"))
        if self.config.backtest_mode:
            if self.config.backtest_date:
                target = _dt_mod.date.fromisoformat(self.config.backtest_date)
            else:
                target = now_et.date() - timedelta(days=1)
                while target.weekday() >= 5:
                    target -= timedelta(days=1)
            self.log.info(f"[M5] 回测模式 — 日期: {target}")
        else:
            target = now_et.date()
            while target.weekday() >= 5:
                target -= timedelta(days=1)
            self.log.info(f"[M5] 实盘模式 — 目标日期: {target}")
        self._today_et_date = target.isoformat()

        instrument_ids = self._all_instrument_ids()
        self.log.info(
            f"[M5] 初始化 {len(instrument_ids)} 个标的  "
            f"M5-ST({self.config.st_period},{self.config.st_mult})  "
            f"EMA{self.config.ema_period}/EMA{self.config.ema9_period}  DEMA{self.config.dema_period}"
        )

        init_thread = threading.Thread(target=self._async_init, daemon=True)
        init_thread.start()
        self.log.info("[M5] 异步初始化线程已启动 (Awaiting instruments...)")

    def _async_init(self) -> None:
        """异步执行合约加载、历史拉取和实时订阅（M5 引擎原生订阅）。"""
        import time as _time
        instrument_ids = self._all_instrument_ids()
        self.log.info(f"[M5][Async] 开始异步初始化 {len(instrument_ids)} 个标的")

        wait_start = _time.time()
        while True:
            missing = [str(iid) for iid in instrument_ids if self.cache.instrument(iid) is None]
            if not missing:
                self.log.info("[M5][Async] ✓ 所有合约已加载")
                break
            if _time.time() - wait_start > 300:
                self.log.error(f"[M5][Async] ✗ 合约加载超时（5分钟上限），部分合约将缺失: {missing}")
                break
            _time.sleep(5.0)

        for iid in instrument_ids:
            sym = iid.symbol.value
            if self.cache.instrument(iid) is None:
                continue

            if sym not in self._st_m5:
                self._init_m5_states(sym)
                self.log.info(f"[M5][Async] {sym}: 状态机初始化完成")

            # 构造 M5 BarType（引擎原生 step=5 订阅，取代老 _M5Bucket 自聚合）
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

            target_date = datetime.strptime(self._today_et_date, "%Y-%m-%d").date()
            hist_start_utc = datetime(
                target_date.year, target_date.month, target_date.day,
                4, 0, 0, tzinfo=ZoneInfo("America/New_York"),
            ).astimezone(timezone.utc)

            self.log.info(f"[M5][Async] {sym}: → request_bars(M5)")
            if self.config.backtest_mode:
                hist_end_utc = datetime(
                    target_date.year, target_date.month, target_date.day,
                    20, 0, 0, tzinfo=ZoneInfo("America/New_York"),
                ).astimezone(timezone.utc)
                self.request_bars(bar_type, start=hist_start_utc, end=hist_end_utc)
            else:
                self.request_bars(bar_type, start=hist_start_utc)

            t = threading.Timer(15.0, self._flush_history_for, args=(sym,))
            t.daemon = True
            t.start()

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info(f"[M5] 停止，共收到实时 M5 K 线 {self._bar_count} 根")

    # ── 历史 M5 K 线回调 ──────────────────────────────────────────────
    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            return

        sym = data.bar_type.instrument_id.symbol.value
        # M5 策略只处理 M5 bar（日K / M1 由 M1 策略负责）
        if data.bar_type.spec.aggregation != BarAggregation.MINUTE:
            return
        if data.bar_type.spec.step != self.config.bar_step:
            return

        o, h, lo, c = float(data.open), float(data.high), float(data.low), float(data.close)
        v = int(data.volume)
        et = et_fake_utc(data.ts_event)

        if et % 86400 >= 16 * 3600:
            self.log.debug(f"[HIST-M5] {sym}: 跳过盘后 bar time={et}")
            return

        # flush 后：增量处理（poll 回放）
        if self._hist_flushed.get(sym):
            if is_chart_et(et):
                self._inc_update_m5(sym, {
                    "time": et, "open": round(o, 4), "high": round(h, 4),
                    "low": round(lo, 4), "close": round(c, 4), "volume": v,
                })
            return

        m5_raw = {
            "time": et, "open": round(o, 4), "high": round(h, 4),
            "low": round(lo, 4), "close": round(c, 4), "volume": v,
        }
        m5_bar = self._compute_m5_bar(sym, m5_raw)

        n = len(self._hist_m5[sym]) + 1
        if n == 1:
            self.log.info(
                f"[HIST-M5] {sym}: ← 第一根历史 M5  time={et}  C={c:.2f}  "
                f"ST={m5_bar['st_value']:.2f}({'↑' if m5_bar['st_dir']==1 else '↓'})"
            )

        # 历史 M5 也刷新 indicators:active（让 flush 后为最新值）
        if is_rth(et) and m5_bar["st_dir"] in (1, -1) and m5_bar["st_value"]:
            self._write_indicators_active(sym, et, m5_bar["st_value"], m5_bar["st_dir"], m5_bar["dema20"], m5_bar["atr"])

        self._hist_m5[sym].append(m5_bar)
        if is_premarket_et(et):
            self._premarket_last_m5[sym] = m5_bar

        if n % 20 == 0:
            self.log.info(f"[HIST-M5] {sym}: 已缓冲 {n} 根 M5")

    def _flush_history_for(self, sym: str) -> None:
        """Timer 触发（默认 15s）：批量写 Redis bars:5m + capture 盘前 + 订阅实时 M5。"""
        if self._hist_flushed.get(sym):
            self.log.debug(f"[FLUSH-M5] {sym}: 已刷写过，跳过")
            return
        self._hist_flushed[sym] = True

        bars = self._hist_m5.get(sym, [])
        if not bars:
            self.log.warning(f"[FLUSH-M5] {sym}: ⚠ 未收到任何历史 M5 K 线！")
        else:
            self.log.info(f"[FLUSH-M5] {sym}: 开始批写 Redis，缓冲 M5={len(bars)} 根")
            if self._redis:
                chart_m5 = [b for b in bars if is_chart_et(b["time"])]
                try:
                    key_m5 = f"bars:5m:{sym}"
                    written = []
                    for b in chart_m5[-MAX_BARS:]:
                        row = dict(b)
                        if is_premarket_chart_et(b["time"]):
                            row["premarket"] = True
                        written.append(row)
                    self._redis.delete(key_m5)
                    if written:
                        self._redis.rpush(key_m5, *[json.dumps(b) for b in written])
                    self.log.info(
                        f"[FLUSH-M5] {sym}: ✓ bars:5m 写入完成  图表={len(chart_m5)} 根  "
                        + (f"最新 C={written[-1]['close']}" if written else "")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH-M5] {sym}: ✗ bars:5m Redis 写入失败: {e}")

        self._capture_premarket_m5(sym)

        if self.config.backtest_mode:
            self.log.info(f"[FLUSH-M5] {sym}: ✓ M5 回测完成")
        else:
            bar_type = self._bar_types.get(sym)
            if bar_type:
                self.subscribe_bars(bar_type)
                self.log.info(f"[FLUSH-M5] {sym}: ✓ 已订阅实时 M5 (step={self.config.bar_step}, Source: EXTERNAL)")
                self._schedule_poll(sym)
            else:
                self.log.error(f"[FLUSH-M5] {sym}: ✗ 无法订阅实时 M5")

    def _schedule_poll(self, sym: str) -> None:
        t = threading.Timer(60.0, self._poll_history, args=(sym,))
        t.daemon = True
        t.start()

    def _poll_history(self, sym: str) -> None:
        """主动拉取最近 45 分钟 M5 历史，作为实时流兜底。"""
        if self.config.backtest_mode:
            return
        bar_type = self._bar_types.get(sym)
        if not bar_type:
            return
        start_utc = self.clock.utc_now() - timedelta(minutes=45)
        self.log.info(f"[POLL-M5] {sym}: 拉取最近 45min M5 (Start={start_utc})...")
        self.request_bars(bar_type, start=start_utc)
        self._schedule_poll(sym)

    def _inc_update_m5(self, sym: str, m5_raw: dict) -> None:
        """增量更新 M5（poll 回放去重 → 算指标 → 写 bars:5m + publish + indicators:active）。"""
        if not self._redis:
            return
        et = int(m5_raw["time"])
        if et % 86400 >= 16 * 3600:
            return
        if not is_chart_et(et):
            return
        try:
            key_m5 = f"bars:5m:{sym}"
            last_json = self._redis.lindex(key_m5, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if m5_raw["time"] <= last_bar["time"]:
                    return

            m5_bar = self._compute_m5_bar(sym, m5_raw)
            if is_rth(et) and m5_bar["st_dir"] in (1, -1) and m5_bar["st_value"]:
                self._write_indicators_active(sym, et, m5_bar["st_value"], m5_bar["st_dir"], m5_bar["dema20"], m5_bar["atr"])

            m5_pub = self._attach_m1_snap_from_registry(sym, m5_bar)
            dedup_rpush_bar(self._redis, key_m5, m5_bar)
            self._redis.publish(f"kline:5m:{sym}", json.dumps(m5_pub))
            iid_str = str(self._bar_types[sym].instrument_id) if sym in self._bar_types else ""
            self.msgbus.publish("bar.collected.m5", BarCollectedM5Event(sym, {**m5_pub, "instrument_id": iid_str}))
            self.log.info(f"[INC-M5] {sym}: M5 增量更新 (Time={et} C={m5_bar['close']})")
        except Exception as e:
            self.log.error(f"[INC-M5] {sym}: 失败 - {e}")

    # ── 实时 M5 K 线收盘 ────────────────────────────────────────────
    def on_bar(self, bar: Bar) -> None:
        self._bar_count += 1
        sym = bar.bar_type.instrument_id.symbol.value
        o, h, lo, c = float(bar.open), float(bar.high), float(bar.low), float(bar.close)
        v = int(bar.volume)
        et = et_fake_utc(bar.ts_event)

        # 过滤非今日实时 bar
        if self._today_et_date:
            ts_utc = bar.ts_event // 1_000_000_000
            bar_et_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(
                ZoneInfo("America/New_York")
            )
            if bar_et_dt.date().isoformat() != self._today_et_date:
                self.log.debug(f"[BAR-M5] {sym}: 跳过非今日 bar {bar_et_dt.strftime('%Y-%m-%d %H:%M')} ET")
                return

        is_rth_bar = is_rth(et)
        is_pm_chart = is_premarket_chart_et(et)
        is_premarket = is_premarket_et(et)

        if not is_rth_bar and not is_premarket:
            self.log.debug(f"[BAR-M5] {sym}: 跳过盘后 bar")
            return

        m5_raw = {
            "time": et, "open": round(o, 4), "high": round(h, 4),
            "low": round(lo, 4), "close": round(c, 4), "volume": v,
        }
        m5_bar = self._compute_m5_bar(sym, m5_raw)

        # 更早盘前（04:00–08:30）：只预热指标 + 盘前锚定，不写 Redis/不 publish
        if is_premarket and not is_pm_chart:
            self._premarket_last_m5[sym] = m5_bar
            self.log.debug(f"[BAR-M5] {sym}: 盘前预热（<08:30，不写 Redis）  C={c:.2f}  ST={m5_bar['st_value']:.2f}")
            return

        # RTH 首根：组装写入 premarket:ref（每交易日一次）
        if is_rth_bar:
            session_date = session_date_from_et(et)
            if self._premarket_ref_date.get(sym) != session_date:
                self._write_premarket_ref(sym, session_date)

        # 盘前图表段也更新盘前锚定内存
        if is_pm_chart:
            self._premarket_last_m5[sym] = m5_bar

        # indicators:active（M5 ST 冻结水平线）
        if is_rth_bar and m5_bar["st_dir"] in (1, -1) and m5_bar["st_value"]:
            self._write_indicators_active(sym, et, m5_bar["st_value"], m5_bar["st_dir"], m5_bar["dema20"], m5_bar["atr"])

        tag = "盘前" if is_pm_chart else "RTH"
        self.log.info(
            f"[M5 #{self._bar_count}] {sym} [{tag}]  "
            f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f}  "
            f"ST={m5_bar['st_value']:.2f}({'↑' if m5_bar['st_dir']==1 else '↓'})  "
            + (f"EMA21={m5_bar['ema21']:.2f}" if m5_bar["ema21"] else "EMA21=预热中")
        )

        # 贴 M1 快照（从 registry 读）
        m5_pub = self._attach_m1_snap_from_registry(sym, m5_bar)

        if not self._redis:
            self.log.warning(f"[M5] {sym}: Redis 不可用，跳过写入")
        else:
            try:
                key = f"bars:5m:{sym}"
                dedup_rpush_bar(self._redis, key, m5_bar)
                self._redis.publish(f"kline:5m:{sym}", json.dumps(m5_pub))
                self.log.debug(f"[M5] {sym}: ✓ Redis RPUSH bars:5m + PUBLISH kline:5m")
            except Exception as e:
                self.log.error(f"[M5] {sym}: ✗ Redis 写入失败: {e}")

        iid_str = str(bar.bar_type.instrument_id)
        self.msgbus.publish("bar.collected.m5", BarCollectedM5Event(sym, {**m5_pub, "instrument_id": iid_str}))
