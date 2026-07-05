"""
SignalDetector — 独立 Actor（Strategy 壳）

订阅 MessageBus：
  - bar.collected           (M1) → 超级信号 st_super（M1 ST 翻转 + M5 ST 同向）
  - bar.collected.m5        (M5) → 更新 M5 ST 状态
  - bars.history.flushed    → 历史 K 线 flush 后回放当天 st_super

写入 Redis：
  - signals:touch:{SYMBOL}
  - signals:touch:index
  - signals:markers:{SYMBOL}   图表标记（当日回放 + 实时追加）
  - proposal:pending:*         实时触线自动建建议（ALPHA_AUTO_PROPOSAL=1）
  - PUBLISH signal:touch / signal:touch:backfill
"""
from __future__ import annotations

import json
import os
import time
from collections import defaultdict

import redis as _redis
from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from approval.alpha_scan import auto_proposal_from_touch
from events import BarCollectedEvent, BarCollectedM5Event, BarsHistoryFlushedEvent
from signals.st_super import (
    StSuperSymbolState,
    detect_st_super_flip,
    replay_st_super_touches,
    update_st5_from_m5_bar,
)
from signals.touch_backfill import MARKERS_KEY, write_markers_list
from signals.touch_detector import TouchEvent, dedup_key

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
TOUCH_INDEX = "signals:touch:index"
AUTO_PROPOSAL = os.environ.get("ALPHA_AUTO_PROPOSAL", "1").strip().lower() in ("1", "true", "yes")


def _symbols_from_instrument_ids(instrument_ids: tuple[str, ...]) -> set[str]:
    out: set[str] = set()
    for iid in instrument_ids:
        sym = str(iid).split(".")[0].strip().upper()
        if sym:
            out.add(sym)
    return out


class SignalDetectorConfig(StrategyConfig, frozen=True):
    """M1 超级信号检测 Actor 配置。"""

    instrument_ids: tuple[str, ...] = ()


class SignalDetector(Strategy):
    """M1 ST 翻转 + M5 ST 同向 → st_super 触线写入 Redis，并可选自动建 pending 建议。"""

    def __init__(self, config: SignalDetectorConfig) -> None:
        super().__init__(config)
        self._redis: _redis.Redis | None = None
        self._dedup: dict[str, set[str]] = defaultdict(set)
        self._st_super: dict[str, StSuperSymbolState] = {}
        self._trade_symbols = _symbols_from_instrument_ids(config.instrument_ids)

    def on_start(self) -> None:
        try:
            self._redis = _redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            self._redis.ping()
            self.log.info("[SignalDetector] Redis 已连接")
        except Exception as e:
            self.log.error(f"[SignalDetector] Redis 连接失败: {e}")
            self._redis = None

        self.msgbus.subscribe(topic="bar.collected", handler=self._on_m1_bar)
        self.msgbus.subscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
        self.msgbus.subscribe(topic="bars.history.flushed", handler=self._on_history_flushed)
        self.log.info(
            "[SignalDetector] 已启动 | st_super + "
            f"{'自动建建议' if AUTO_PROPOSAL else '仅写 touch'}"
        )

    def on_stop(self) -> None:
        try:
            self.msgbus.unsubscribe(topic="bar.collected", handler=self._on_m1_bar)
            self.msgbus.unsubscribe(topic="bar.collected.m5", handler=self._on_m5_bar)
            self.msgbus.unsubscribe(topic="bars.history.flushed", handler=self._on_history_flushed)
        except Exception:
            pass
        if self._redis:
            self._redis.close()
        self.log.info("[SignalDetector] 已停止")

    def _on_history_flushed(self, event: BarsHistoryFlushedEvent) -> None:
        if not self._redis:
            return
        sym = event.symbol
        try:
            import json as _json

            def _parse(raw_list: list[str]) -> list[dict]:
                out = []
                for raw in raw_list:
                    try:
                        out.append(_json.loads(raw))
                    except (_json.JSONDecodeError, TypeError):
                        continue
                return out

            m5_raw = self._redis.lrange(f"bars:5m:{sym}", 0, -1)
            m1_raw = self._redis.lrange(f"bars:1m:{sym}", 0, -1)
            m5_bars, m1_bars = _parse(m5_raw), _parse(m1_raw)

            st_state, events_ss = replay_st_super_touches(sym, m5_bars, m1_bars)
            self._st_super[sym] = st_state
            seen = {dedup_key(ev) for ev in events_ss}
            all_events = sorted(events_ss, key=lambda e: e.touch_time)
            self._dedup[sym] = seen
            n = write_markers_list(self._redis, sym, all_events)
            pipe = self._redis.pipeline()
            pipe.delete(f"signals:touch:{sym}")
            for ev in all_events[-20:]:
                payload = ev.to_dict()
                payload["emitted_at"] = int(time.time())
                pipe.rpush(f"signals:touch:{sym}", _json.dumps(payload, ensure_ascii=False))
                idx_member = f"{ev.symbol}:{ev.signal_type}:{ev.touch_time}"
                pipe.zadd(TOUCH_INDEX, {idx_member: ev.touch_time})
            pipe.publish(
                "signal:touch:backfill",
                _json.dumps({
                    "symbol": sym,
                    "count": n,
                    "st_super": len(events_ss),
                    "session_date": event.session_date,
                }),
            )
            pipe.execute()
            self.log.info(
                f"[SignalDetector] {sym}: 历史回放 {n} 条 st_super "
                f"st5_dir={st_state.st5_dir}"
            )
        except Exception as e:
            self.log.error(f"[SignalDetector] {sym}: 历史触线回放失败: {e}")

    def _on_m5_bar(self, event: BarCollectedM5Event) -> None:
        sym = event.symbol
        bar = event.bar
        if sym not in self._st_super:
            self._st_super[sym] = StSuperSymbolState.create()
        update_st5_from_m5_bar(bar, self._st_super[sym])
        self.log.debug(
            f"[SignalDetector] {sym} M5 ST5 dir={self._st_super[sym].st5_dir} "
            f"time={bar.get('time')}"
        )

    def _on_m1_bar(self, event: BarCollectedEvent) -> None:
        sym = event.symbol
        m1 = event.bar
        if sym not in self._st_super:
            self._st_super[sym] = StSuperSymbolState.create()
        super_ev = detect_st_super_flip(sym, m1, self._st_super[sym])
        if super_ev:
            self._emit_touch(super_ev, publish_live=True)

    def _append_marker(self, touch: TouchEvent, payload: dict) -> None:
        if not self._redis:
            return
        key = MARKERS_KEY.format(symbol=touch.symbol)
        self._redis.rpush(key, json.dumps(payload, ensure_ascii=False))

    def _emit_touch(self, touch: TouchEvent, *, publish_live: bool = True) -> None:
        key = dedup_key(touch)
        seen = self._dedup[touch.symbol]
        if key in seen:
            return
        seen.add(key)

        payload = touch.to_dict()
        payload["emitted_at"] = int(time.time())
        if not self._redis:
            return
        try:
            pipe = self._redis.pipeline()
            pipe.rpush(f"signals:touch:{touch.symbol}", json.dumps(payload, ensure_ascii=False))
            pipe.ltrim(f"signals:touch:{touch.symbol}", -20, -1)
            idx_member = f"{touch.symbol}:{touch.signal_type}:{touch.touch_time}"
            pipe.zadd(TOUCH_INDEX, {idx_member: touch.touch_time})
            if publish_live:
                pipe.publish("signal:touch", json.dumps(payload, ensure_ascii=False))
            pipe.execute()
            self._append_marker(touch, payload)
            try:
                from measurement.signal_store import record_touch
                record_touch(payload)
            except Exception as e:
                self.log.debug(f"[SignalDetector] signal_store: {e}")
            self.log.info(
                f"[SignalDetector] {touch.symbol} {touch.signal_type} "
                f"{touch.side} @ {touch.touch_time} level={touch.trigger_level:.2f}"
            )
            if publish_live:
                self._maybe_auto_proposal(touch)
        except Exception as e:
            self.log.error(f"[SignalDetector] {touch.symbol}: 写 Redis 失败: {e}")

    def _maybe_auto_proposal(self, touch: TouchEvent) -> None:
        if not AUTO_PROPOSAL or not self._redis:
            return
        sym = touch.symbol.upper()
        if self._trade_symbols and sym not in self._trade_symbols:
            self.log.debug(f"[SignalDetector] {sym}: 不在 instrument_ids，跳过建建议")
            return
        try:
            created, out = auto_proposal_from_touch(self._redis, touch)
            if created:
                self.log.info(
                    f"[SignalDetector] + pending {sym} {touch.side} "
                    f"stop≈{touch.trigger_level:.2f} id={out}"
                )
            else:
                self.log.info(f"[SignalDetector] {sym}: 未建建议 ({out})")
        except Exception as e:
            self.log.error(f"[SignalDetector] {sym}: 自动建建议失败: {e}")
