"""M1IndicatorStrategy — M1 周期指标计算策略（设计原则 #1：每周期一个 Strategy）。

取代老 ``strategy.BarLoggerStrategy`` 的 M1 职责（单实例多周期胖策略拆分）：

  - 历史 / 实时 M1 K 线 → ST / EMA21 / ATR14 / SessionVWAP
  - 写 Redis ``bars:1m`` / ``kline:1m`` + 发布 ``bar.collected`` 事件（字段格式不变）
  - 日K → ``prev_day``；仓位同步（on_position_*）；quote_tick 实时跳动
  - **跨周期输出**：每根 M1 把 ST 快照（保留冻结语义）+ ATR + 盘前锚定写入共享
    ``IndicatorRegistry``，供 M5IndicatorStrategy 读取（拉模式，取代老 _attach_m1_snap_to_m5）

对外接口（消费者零改动）：
  - ``bar.collected`` 事件 bar_dict 字段：st_value/st_dir/st_upper/st_lower/ema21/vwap
  - Redis：bars:1m / kline:1m / prev_day / premarket:ref（premarket:ref 由 M5 策略组装写入）
"""
from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional
from zoneinfo import ZoneInfo

import redis as _redis

from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import AverageTrueRange
from nautilus_trader.indicators.averages import MovingAverageType
from nautilus_trader.model.data import Bar, BarSpecification, BarType, QuoteTick
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from indicators import EMAState, IndicatorRegistry, STState, SessionVWAPState
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
from events import BarCollectedEvent, BarsHistoryFlushedEvent


class M1IndicatorStrategyConfig(StrategyConfig, frozen=True):
    """M1 指标策略配置。

    registry 不放 frozen config（避免可变对象序列化问题），由构造时注入。
    """
    instrument_id: InstrumentId
    instrument_ids: tuple[str, ...] = ()
    bar_step: int = 1
    st_period: int = 10
    st_mult: float = 3.0     # M1 ST 乘数（与超级信号 st_super 一致）
    ema_period: int = 21
    atr_period: int = 14     # M1 ATR 周期（供 M5 mom_atr 归一化）
    history_days: int = 2
    backtest_mode: bool = False
    backtest_date: str = ""


class M1IndicatorStrategy(Strategy):
    """M1 周期指标 + 数据落盘策略。

    registry 经构造参数注入（main.py 构造单例），跨周期共享。
    """

    def __init__(
        self,
        config: M1IndicatorStrategyConfig,
        registry: Optional[IndicatorRegistry] = None,
    ) -> None:
        super().__init__(config)
        self._registry: Optional[IndicatorRegistry] = registry

        self._redis: Optional[_redis.Redis] = None
        self._bar_count: int = 0

        # 今日 ET 日期（过滤非今日 bar + Session VWAP 日切）
        self._today_et_date: Optional[str] = None
        # 每个标的历史是否已刷写 Redis
        self._hist_flushed: dict[str, bool] = {}
        # 每个标的 BarType（flush 后用于订阅实时 + poll 兜底）
        self._bar_types: dict[str, BarType] = {}

        # 每个标的 M1 状态机
        self._st_m1: dict[str, STState] = {}
        self._ema_m1: dict[str, EMAState] = {}
        self._atr_m1: dict[str, AverageTrueRange] = {}    # M1 ATR14（供 M5 mom_atr）
        self._vwap: dict[str, SessionVWAPState] = {}

        # 历史 bar 缓冲（flush 时批量覆盖写 Redis）
        self._hist_m1: dict[str, list[dict]] = defaultdict(list)
        # 日K 缓冲（取倒数第二个 = 昨日 → prev_day）
        self._hist_daily: dict[str, list[dict]] = defaultdict(list)

        # 当前未完成的 tick K 线（quote_tick 实时跳动）
        self._cur_bar: dict[str, Optional[dict]] = defaultdict(lambda: None)

        # 盘前锚定：最后一根盘前 M1（写入 registry 供 M5 组装 premarket:ref）
        self._premarket_last_m1: dict[str, dict] = {}
        # 1m ST 对外发布快照（M5 桶边界刷新，桶内冻结，保留老 _refresh_m1_st_publish 语义）
        self._m1_st_published: dict[str, dict] = {}

    # ── 1m ST 对外发布快照（桶内冻结，桶边界刷新）────────────────────────
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
        """1m ST 每根 M1 内部计算，仅在 M5 桶边界刷新对外字段。

        保留老 strategy._refresh_m1_st_publish 的冻结语义：M5 桶第一根 M1 刷新，
        桶内其余 4 根复用，使前端 ST 线在 5 分钟内保持稳定。该快照同时写入
        registry 供 M5 策略读取（取代老 _attach_m1_snap_to_m5）。
        """
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

    def _publish_m1_snapshot_to_registry(self, sym: str, pub: dict, atr_val: Optional[float]) -> None:
        """把 M1 ST 快照 + ATR 写入共享 registry，供 M5 策略读取。"""
        if not self._registry:
            return
        self._registry.set_many(sym, "m1", {
            "st_value": pub["st_value"],
            "st_dir": pub["st_dir"],
            "st_upper": pub["st_upper"],
            "st_lower": pub["st_lower"],
            "m1_bar_time": pub["m1_bar_time"],
            "m1_open": pub["m1_open"],
            "m1_high": pub["m1_high"],
            "m1_low": pub["m1_low"],
            "m1_close": pub["m1_close"],
            "atr": atr_val,
        })

    # ── 解析所有订阅合约 ────────────────────────────────────────────────
    def _all_instrument_ids(self) -> list[InstrumentId]:
        ids = list(self.config.instrument_ids)
        base = str(self.config.instrument_id)
        if base not in ids:
            ids.insert(0, base)
        return [InstrumentId.from_str(i) for i in ids]

    def _apply_session_vwap(self, sym: str, bar: dict) -> Optional[float]:
        """M1 收盘：增量 Session VWAP → 写入 bar['vwap']。"""
        if sym not in self._vwap:
            self._vwap[sym] = SessionVWAPState()
        session_date = session_date_from_et(int(bar["time"]))
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

    def _capture_premarket_m1(self, sym: str) -> None:
        """历史 flush 后：取最后一根盘前 M1，写入 registry 供 M5 组装 premarket:ref。"""
        if not self._premarket_last_m1.get(sym):
            for b in reversed(self._hist_m1.get(sym, [])):
                if is_premarket_et(b["time"]):
                    self._premarket_last_m1[sym] = b
                    break
        last = self._premarket_last_m1.get(sym)
        if last and self._registry:
            self._registry.set(sym, "m1", "premarket_last", last)
            self.log.info(
                f"[PremarketRef] {sym}: M1 盘前锚定写入 registry  "
                f"time={last['time']}  C={last['close']}"
            )

    # ── 生命周期 ──────────────────────────────────────────────────────
    def on_start(self) -> None:
        # 连接 Redis
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info(f"[M1] Redis 已连接: {REDIS_HOST}:{REDIS_PORT}")
        except Exception as e:
            self.log.error(f"[M1] Redis 连接失败: {e}")
            self._redis = None

        # ── 计算历史数据目标日期 ────────────────────────────────────────
        import datetime as _dt_mod
        now_et = datetime.now(tz=ZoneInfo("America/New_York"))

        if self.config.backtest_mode:
            if self.config.backtest_date:
                target = _dt_mod.date.fromisoformat(self.config.backtest_date)
                self.log.info(f"[M1] 回测模式 — 指定日期: {target}")
            else:
                target = now_et.date() - timedelta(days=1)
                while target.weekday() >= 5:
                    target -= timedelta(days=1)
                self.log.info(f"[M1] 回测模式 — 自动选上一交易日: {target}")
        else:
            target = now_et.date()
            while target.weekday() >= 5:
                target -= timedelta(days=1)
            self.log.info(f"[M1] 实盘模式 — 目标日期: {target}")

        self._today_et_date = target.isoformat()
        hist_start_et = datetime(
            target.year, target.month, target.day,
            4, 0, 0, tzinfo=ZoneInfo("America/New_York"),   # 04:00 盘前开始
        )
        hist_start_utc = hist_start_et.astimezone(timezone.utc)
        mode_label = "回测" if self.config.backtest_mode else "实盘"
        self.log.info(
            f"[M1] 模式={mode_label}  日期={self._today_et_date}  "
            f"历史起点={hist_start_et.strftime('%H:%M')} ET "
            f"({hist_start_utc.strftime('%Y-%m-%d %H:%M UTC')})"
        )

        instrument_ids = self._all_instrument_ids()
        self.log.info(
            f"[M1] 初始化 {len(instrument_ids)} 个标的: "
            f"{[str(i) for i in instrument_ids]}  "
            f"M1-ST({self.config.st_period},{self.config.st_mult})  "
            f"EMA{self.config.ema_period}"
        )

        # 异步初始化（合约加载 + 历史拉取 + 实时订阅），不阻塞引擎线程
        init_thread = threading.Thread(target=self._async_init, daemon=True)
        init_thread.start()
        self.log.info("[M1] 异步初始化线程已启动 (Awaiting instruments...)")

    def _async_init(self) -> None:
        """异步执行合约加载、历史拉取和实时订阅。"""
        import time as _time
        instrument_ids = self._all_instrument_ids()
        self.log.info(f"[M1][Async] 开始异步初始化 {len(instrument_ids)} 个标的")

        # 1. 循环等待合约进入 cache
        wait_start = _time.time()
        while True:
            missing = [str(iid) for iid in instrument_ids if self.cache.instrument(iid) is None]
            if not missing:
                self.log.info("[M1][Async] ✓ 所有合约已加载")
                break
            if _time.time() - wait_start > 300:
                self.log.error(f"[M1][Async] ✗ 合约加载超时（5分钟上限），部分合约将缺失: {missing}")
                break
            _time.sleep(5.0)

        # 2. 对每个就绪合约执行初始化
        for iid in instrument_ids:
            sym = iid.symbol.value
            if self.cache.instrument(iid) is None:
                continue

            # 初始化 M1 状态机
            if sym not in self._st_m1:
                self._st_m1[sym] = STState(self.config.st_period, self.config.st_mult)
                self._ema_m1[sym] = EMAState(self.config.ema_period)
                self._atr_m1[sym] = AverageTrueRange(self.config.atr_period, MovingAverageType.WILDER)
                self._vwap[sym] = SessionVWAPState()
                self.log.info(f"[M1][Async] {sym}: 状态机初始化完成")

            # 构造 M1 BarType
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

            target_str = self._today_et_date
            target_date = datetime.strptime(target_str, "%Y-%m-%d").date()
            hist_start_utc = datetime(
                target_date.year, target_date.month, target_date.day,
                4, 0, 0, tzinfo=ZoneInfo("America/New_York"),
            ).astimezone(timezone.utc)

            self.log.info(f"[M1][Async] {sym}: → request_bars(M1)")
            if self.config.backtest_mode:
                hist_end_utc = datetime(
                    target_date.year, target_date.month, target_date.day,
                    20, 0, 0, tzinfo=ZoneInfo("America/New_York"),
                ).astimezone(timezone.utc)
                self.request_bars(bar_type, start=hist_start_utc, end=hist_end_utc)
            else:
                self.request_bars(bar_type, start=hist_start_utc)

            # 请求日K（prev_day 围栏）—— 日K 归 M1 策略
            daily_bar_type = BarType(
                iid,
                BarSpecification(step=1, aggregation=BarAggregation.DAY, price_type=PriceType.LAST),
                AggregationSource.EXTERNAL,
            )
            self.request_bars(daily_bar_type, start=hist_start_utc - timedelta(days=7))

            # 非回测模式订阅 Tick（实时跳动归 M1）
            if not self.config.backtest_mode:
                try:
                    self.subscribe_quote_ticks(iid)
                except Exception as e:
                    self.log.warning(f"[M1][Async] {sym}: ✗ QuoteTick 订阅失败: {e}")

            # 15s 后刷写历史并订阅实时
            t = threading.Timer(15.0, self._flush_history_for, args=(sym,))
            t.daemon = True
            t.start()

    def on_stop(self) -> None:
        if self._redis:
            self._redis.close()
        self.log.info(f"[M1] 停止，共收到实时 K 线 {self._bar_count} 根")

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
                self.log.info(f"[M1] 仓位已平仓，Redis key 已删除: {sym}")
            except Exception as e:
                self.log.warning(f"[M1] 仓位关闭写 Redis 失败: {e}")

    def _sync_position_to_redis(self, instrument_id) -> None:
        """将 NautilusTrader 缓存中的仓位信息写入 Redis 并 PUBLISH 通知前端。"""
        if not self._redis:
            return
        try:
            pos = self.cache.position(self.cache.position_id(instrument_id))
            if pos is None:
                for p in self.cache.positions_open():
                    if p.instrument_id == instrument_id:
                        pos = p
                        break
            if pos is None:
                return

            sym = instrument_id.symbol.value
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

            # 保留 Redis 中已有的 stop_loss（由前端/ExitManager 写入），不覆写为 None
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
                "stop_loss":      existing_sl,
                "unrealized_pnl": upnl,
                "realized_pnl":   float(pos.realized_pnl.as_double()) if pos.realized_pnl else 0.0,
                "last_price":     last_price,
            }
            self._redis.set(f"position:{sym}", json.dumps(pos_data))
            self._redis.publish("position:update", json.dumps(pos_data))
            self.log.info(f"[M1] 仓位已同步到 Redis: {sym} {pos_data['side']} x{pos_data['quantity']}")
        except Exception as e:
            self.log.warning(f"[M1] _sync_position_to_redis 失败: {e}")

    # ── 历史 K 线回调（IBKR request_bars 响应）─────────────────────────
    def on_historical_data(self, data) -> None:
        if not isinstance(data, Bar):
            self.log.debug(f"[HIST] 收到非 Bar 数据: {type(data).__name__}，跳过")
            return

        sym = data.bar_type.instrument_id.symbol.value

        # ─ 日K：取昨日 H/L/C 写入 Redis prev_day:{sym} ──────────────────
        if data.bar_type.spec.aggregation == BarAggregation.DAY:
            h, lo, c = float(data.high), float(data.low), float(data.close)
            et = et_fake_utc(data.ts_event)
            self._hist_daily[sym].append({"time": et, "high": h, "low": lo, "close": c})
            today_str = self._today_et_date
            prev = None
            for bar in reversed(self._hist_daily[sym]):
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
            return  # 日K 不进入 M1 流程

        o, h, lo, c = float(data.open), float(data.high), float(data.low), float(data.close)
        v = int(data.volume)
        et = et_fake_utc(data.ts_event)

        # 盘后 bar（≥16:00 ET）：直接跳过
        if et % 86400 >= 16 * 3600:
            self.log.debug(f"[HIST] {sym}: 跳过盘后 bar time={et}")
            return

        # flush 后：仅增量写 Redis（poll 回放），不得先喂指标再 return
        if self._hist_flushed.get(sym):
            if is_chart_et(et):
                self._inc_update_redis(sym, {
                    "time": et, "open": round(o, 4), "high": round(h, 4),
                    "low": round(lo, 4), "close": round(c, 4), "volume": v,
                })
            return

        n = len(self._hist_m1[sym])
        if n == 0:
            self.log.info(
                f"[HIST] {sym}: ← 第一根历史 K 线到达  "
                f"time={et}  O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f}"
            )

        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)
        self._atr_m1[sym].update_raw(h, lo, c)
        atr_val = self._atr_m1[sym].value if self._atr_m1[sym].initialized else None

        bar_internal = {
            "time": et, "open": round(o, 4), "high": round(h, 4),
            "low": round(lo, 4), "close": round(c, 4), "volume": v,
            "ema21": ema21,
            "st_value": st_val, "st_dir": st_dir, "st_upper": st_up, "st_lower": st_lo,
        }

        # M5 桶边界 = et 落在新桶第一根（取代老 _m5_bucket.push 的 flush 触发点）
        bucket_closed = (et % 300 == 0)
        pub = self._refresh_m1_st_publish(
            sym, st_val=st_val, st_dir=st_dir, st_up=st_up, st_lo=st_lo,
            et=et, close=c, o=o, h=h, lo=lo, bucket_closed=bucket_closed,
        )
        self._publish_m1_snapshot_to_registry(sym, pub, atr_val)

        bar_dict = {
            **bar_internal,
            "st_value": pub["st_value"], "st_dir": pub["st_dir"],
            "st_upper": pub["st_upper"], "st_lower": pub["st_lower"],
        }
        if is_rth(et):
            self._apply_session_vwap(sym, bar_dict)
        self._hist_m1[sym].append(bar_dict)
        if is_premarket_et(et):
            self._premarket_last_m1[sym] = bar_dict

        n += 1
        if n % 50 == 0:
            self.log.info(
                f"[HIST] {sym}: 已缓冲 {n} 根历史 K 线  "
                f"最新 C={c:.2f}  ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  "
                f"EMA21={ema21:.2f}" if ema21 else
                f"[HIST] {sym}: 已缓冲 {n} 根  C={c:.2f}  EMA 预热中({self.config.ema_period}期)"
            )

    def _flush_history_for(self, sym: str) -> None:
        """Timer 触发（默认 15s）：批量写 Redis bars:1m + capture 盘前锚定 + 订阅实时。

        只执行一次（_hist_flushed 防重复）。
        """
        if self._hist_flushed.get(sym):
            self.log.debug(f"[FLUSH] {sym}: 已刷写过，跳过")
            return
        self._hist_flushed[sym] = True

        bars = self._hist_m1.get(sym, [])
        if not bars:
            self.log.warning(f"[FLUSH] {sym}: ⚠ 未收到任何历史 M1 K 线！（IBKR 无数据或 request_bars 未完成）")
        else:
            self.log.info(f"[FLUSH] {sym}: 开始批写 Redis，缓冲 M1={len(bars)} 根")
            if not self._redis:
                self.log.warning(f"[FLUSH] {sym}: ⚠ Redis 不可用，跳过写入")
            else:
                chart_m1 = [b for b in bars if is_chart_et(b["time"])]
                pm_chart_n = sum(1 for b in chart_m1 if is_premarket_chart_et(b["time"]))
                rth_n = len(chart_m1) - pm_chart_n
                self.log.info(
                    f"[FLUSH] {sym}: 过滤图表时段  总缓冲={len(bars)} 根  "
                    f"盘前60分={pm_chart_n}  RTH={rth_n}  "
                    f"更早盘前={len(bars)-len(chart_m1)} 根（仅指标预热）"
                )
                try:
                    key_m1 = f"bars:1m:{sym}"
                    written = []
                    for b in chart_m1[-MAX_BARS:]:
                        row = dict(b)
                        if is_premarket_chart_et(b["time"]):
                            row["premarket"] = True
                        written.append(row)
                    self._redis.delete(key_m1)
                    if written:
                        self._redis.rpush(key_m1, *[json.dumps(b) for b in written])
                    self.log.info(
                        f"[FLUSH] {sym}: ✓ bars:1m 写入完成  写入={len(written)} 根  "
                        + (f"最新 C={written[-1]['close']}" if written else "（无有效数据）")
                    )
                except Exception as e:
                    self.log.error(f"[FLUSH] {sym}: ✗ bars:1m Redis 写入失败: {e}")

        # 盘前锚定写入 registry（供 M5 组装 premarket:ref）
        self._capture_premarket_m1(sym)

        # 订阅实时 K 线（仅实盘）
        if self.config.backtest_mode:
            self.log.info(f"[FLUSH] {sym}: ✓ M1 回测完成")
        else:
            bar_type = self._bar_types.get(sym)
            if bar_type:
                self.subscribe_bars(bar_type)
                self.log.info(f"[FLUSH] {sym}: ✓ 已订阅实时 M1 (Source: EXTERNAL)")
                self._schedule_poll(sym)   # 60s 轮询兜底，防 IBKR 实时流断路
            else:
                self.log.error(f"[FLUSH] {sym}: ✗ 无法订阅实时")

        self.msgbus.publish(
            "bars.history.flushed",
            BarsHistoryFlushedEvent(sym, self._today_et_date or ""),
        )

    def _schedule_poll(self, sym: str) -> None:
        t = threading.Timer(60.0, self._poll_history, args=(sym,))
        t.daemon = True
        t.start()

    def _poll_history(self, sym: str) -> None:
        """主动拉取最近 45 分钟历史数据，作为实时流的兜底。"""
        if self.config.backtest_mode:
            return
        bar_type = self._bar_types.get(sym)
        if not bar_type:
            return
        start_utc = self.clock.utc_now() - timedelta(minutes=45)
        self.log.info(f"[POLL] {sym}: 拉取最近 45min M1 (Start={start_utc})...")
        self.request_bars(bar_type, start=start_utc)
        self._schedule_poll(sym)

    def _inc_update_redis(self, sym: str, bar_dict: dict) -> None:
        """增量更新 Redis（poll 回放去重 → 算指标 → 写 bars:1m + publish + registry）。"""
        if not self._redis:
            return
        et = int(bar_dict["time"])
        if et % 86400 >= 16 * 3600:
            return
        if not is_chart_et(et):
            return
        try:
            key_m1 = f"bars:1m:{sym}"
            last_json = self._redis.lindex(key_m1, -1)
            if last_json:
                last_bar = json.loads(last_json)
                if bar_dict["time"] <= last_bar["time"]:
                    return

            o, h, lo, c = (float(bar_dict["open"]), float(bar_dict["high"]),
                           float(bar_dict["low"]), float(bar_dict["close"]))
            v = int(bar_dict.get("volume") or 0)

            if sym not in self._st_m1:
                self._st_m1[sym] = STState(self.config.st_period, self.config.st_mult)
                self._ema_m1[sym] = EMAState(self.config.ema_period)
                self._atr_m1[sym] = AverageTrueRange(self.config.atr_period, MovingAverageType.WILDER)

            st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
            ema21 = self._ema_m1[sym].update(c)
            self._atr_m1[sym].update_raw(h, lo, c)
            atr_val = self._atr_m1[sym].value if self._atr_m1[sym].initialized else None

            bucket_closed = (et % 300 == 0)
            pub = self._refresh_m1_st_publish(
                sym, st_val=st_val, st_dir=st_dir, st_up=st_up, st_lo=st_lo,
                et=et, close=c, o=o, h=h, lo=lo, bucket_closed=bucket_closed,
            )
            self._publish_m1_snapshot_to_registry(sym, pub, atr_val)

            enriched = {
                **bar_dict, "symbol": sym,
                "ema21": ema21,
                "st_value": pub["st_value"], "st_dir": pub["st_dir"],
                "st_upper": pub["st_upper"], "st_lower": pub["st_lower"],
            }
            if is_rth(et):
                self._apply_session_vwap(sym, enriched)
            elif is_premarket_chart_et(et):
                enriched["premarket"] = True

            data_json = json.dumps(enriched)
            self._redis.rpush(key_m1, data_json)
            self._redis.ltrim(key_m1, -MAX_BARS, -1)
            self._redis.publish(f"kline:1m:{sym}", data_json)
            self.log.info(f"[INC-UPDATE] {sym}: M1 增量更新 (Time={et} C={c} vwap={enriched.get('vwap')})")
        except Exception as e:
            self.log.error(f"[INC-UPDATE] {sym}: 失败 - {e}")

    # ── 实时 M1 K 线收盘 ────────────────────────────────────────────
    def on_bar(self, bar: Bar) -> None:
        self._bar_count += 1
        sym = bar.bar_type.instrument_id.symbol.value
        o, h, lo, c = float(bar.open), float(bar.high), float(bar.low), float(bar.close)
        v = int(bar.volume)
        et = et_fake_utc(bar.ts_event)

        # 过滤非今日实时 bar（防 IBKR 追倒推送历史）
        if self._today_et_date:
            ts_utc = bar.ts_event // 1_000_000_000
            bar_et_dt = datetime.fromtimestamp(ts_utc, tz=ZoneInfo("UTC")).astimezone(
                ZoneInfo("America/New_York")
            )
            if bar_et_dt.date().isoformat() != self._today_et_date:
                self.log.debug(f"[BAR] {sym}: 跳过非今日 bar {bar_et_dt.strftime('%Y-%m-%d %H:%M')} ET")
                return

        # RTH / 盘前图表过滤：04:00–08:30 仅预热；08:30–09:30 写 Redis；≥16:00 忽略
        is_rth_bar = is_rth(et)
        is_pm_chart = is_premarket_chart_et(et)
        is_premarket = is_premarket_et(et)

        if not is_rth_bar and not is_premarket:
            self.log.debug(f"[BAR] {sym}: 跳过盘后 bar et_hour={datetime.fromtimestamp(et, tz=timezone.utc).hour:02d}")
            return

        # 历史未到就收到实时 bar 的兜底初始化
        if sym not in self._st_m1:
            self.log.warning(f"[BAR] {sym}: ⚠ 实时 bar 到达时历史尚未预热，临时初始化 M1 状态机")
            self._st_m1[sym] = STState(self.config.st_period, self.config.st_mult)
            self._ema_m1[sym] = EMAState(self.config.ema_period)
            self._atr_m1[sym] = AverageTrueRange(self.config.atr_period, MovingAverageType.WILDER)

        st_val, st_dir, st_up, st_lo = self._st_m1[sym].update(o, h, lo, c)
        ema21 = self._ema_m1[sym].update(c)
        self._atr_m1[sym].update_raw(h, lo, c)
        atr_val = self._atr_m1[sym].value if self._atr_m1[sym].initialized else None

        bar_internal = {
            "time": et, "open": round(o, 4), "high": round(h, 4),
            "low": round(lo, 4), "close": round(c, 4), "volume": v,
            "ema21": ema21,
            "st_value": st_val, "st_dir": st_dir, "st_upper": st_up, "st_lower": st_lo,
        }

        # 更早盘前（04:00–08:30）：只预热指标，不写 Redis
        if is_premarket and not is_pm_chart:
            pre_dict = dict(bar_internal)
            self._premarket_last_m1[sym] = pre_dict
            # 盘前锚定同步到 registry（供 M5 实时组装 premarket:ref）
            if self._registry:
                self._registry.set(sym, "m1", "premarket_last", pre_dict)
            self._cur_bar[sym] = None
            self.log.debug(f"[BAR] {sym}: 盘前预热（<08:30，不写 Redis）  C={c:.2f}  ST={st_val:.2f}")
            return

        bucket_closed = (et % 300 == 0)
        pub = self._refresh_m1_st_publish(
            sym, st_val=st_val, st_dir=st_dir, st_up=st_up, st_lo=st_lo,
            et=et, close=c, o=o, h=h, lo=lo, bucket_closed=bucket_closed,
        )
        self._publish_m1_snapshot_to_registry(sym, pub, atr_val)

        bar_dict = {
            "symbol": sym,
            **bar_internal,
            "st_value": pub["st_value"], "st_dir": pub["st_dir"],
            "st_upper": pub["st_upper"], "st_lower": pub["st_lower"],
        }
        if is_rth_bar:
            self._apply_session_vwap(sym, bar_dict)
        else:
            bar_dict["premarket"] = True
            self._premarket_last_m1[sym] = dict(bar_dict)
            if self._registry:
                self._registry.set(sym, "m1", "premarket_last", bar_dict)

        tag = "盘前" if is_pm_chart else "RTH"
        if ema21 is not None:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym} [{tag}]  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  ST_UP={st_up:.2f}  ST_LO={st_lo:.2f}  EMA21={ema21:.2f}"
            )
        else:
            self.log.info(
                f"[BAR #{self._bar_count}] {sym} [{tag}]  "
                f"O={o:.2f} H={h:.2f} L={lo:.2f} C={c:.2f} V={v}  "
                f"ST={st_val:.2f}({'↑' if st_dir==1 else '↓'})  EMA21=预热中"
            )

        self._cur_bar[sym] = None

        if not self._redis:
            self.log.warning(f"[BAR] {sym}: Redis 不可用，跳过写入")
            return

        key = f"bars:1m:{sym}"
        ch = f"kline:1m:{sym}"
        try:
            dedup_rpush_bar(self._redis, key, bar_dict)
            self._redis.publish(ch, json.dumps(bar_dict))
            self.log.debug(f"[BAR] {sym}: ✓ Redis RPUSH bars:1m + PUBLISH {ch}")
        except Exception as e:
            self.log.error(f"[BAR] {sym}: ✗ Redis 写入失败: {e}")

        if is_rth_bar:
            bar_dict_with_id = {**bar_dict, "instrument_id": str(bar.bar_type.instrument_id)}
            self.msgbus.publish("bar.collected", BarCollectedEvent(sym, bar_dict_with_id))

    # ── QuoteTick 实时跳动（仅实盘）──────────────────────────────────
    def on_quote_tick(self, tick: QuoteTick) -> None:
        """实时 Bid/Ask Tick → 维护当前未完成 K 线 → PUBLISH bars:1m:tick。"""
        sym = tick.instrument_id.symbol.value
        et_tick = et_fake_utc(tick.ts_event)
        if not (is_rth(et_tick) or is_premarket_chart_et(et_tick)):
            return
        mid = (float(tick.bid_price) + float(tick.ask_price)) / 2

        cur = self._cur_bar.get(sym)
        if cur is None:
            et = et_fake_utc(tick.ts_event)
            et_min = et - (et % 60)   # 对齐到当前分钟起始（与 on_bar 时间戳一致）
            cur = {"symbol": sym, "time": et_min, "open": mid, "high": mid, "low": mid, "close": mid}
            self._cur_bar[sym] = cur
            self.log.debug(f"[TICK] {sym}: 新 tick bar 初始化  time={et_min}  mid={mid:.2f}")
        else:
            cur["high"] = max(cur["high"], mid)
            cur["low"] = min(cur["low"], mid)
            cur["close"] = mid

        if not self._redis:
            return
        try:
            self._redis.publish(f"bars:1m:tick:{sym}", json.dumps(cur))
        except Exception as e:
            self.log.warning(f"[TICK] {sym}: PUBLISH 失败: {e}")
