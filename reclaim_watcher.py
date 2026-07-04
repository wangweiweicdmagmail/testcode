"""
ReclaimWatcher — 条件执行：approved_wait → ready_to_execute

订阅 bar.collected (M1)，对已批准建议检测 reclaim（收盘站回/跌破触发线），
满足后写入 ready_to_execute，供 AutoRunner 下单。
"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Optional

import redis as _redis
from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from approval.proposal_store import (
    list_approved_wait,
    mark_reclaim_failed,
    mark_ready_to_execute,
)
from events import BarCollectedEvent

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
RECLAIM_WAIT_MAX_MIN = int(os.environ.get("RECLAIM_WAIT_MAX_MINUTES", 30))
RECLAIM_MAX_M1_BARS = int(os.environ.get("RECLAIM_MAX_M1_BARS", 3))
MIN_RR_HALF = float(os.environ.get("RECLAIM_MIN_RR_HALF", "1.0"))


class ReclaimWatcherConfig(StrategyConfig, frozen=True):
    """Reclaim 条件执行监视器配置。"""
    pass


def _is_reclaimed(side: str, close: float, trigger: float) -> bool:
    if side.upper() == "LONG":
        return close >= trigger
    return close <= trigger


def _rr_half_at_entry(*, side: str, entry: float, stop: float, tp_half: float) -> Optional[float]:
    try:
        if side.upper() == "LONG":
            risk = entry - stop
            reward = tp_half - entry
        else:
            risk = stop - entry
            reward = entry - tp_half
        if risk <= 0:
            return None
        return round(reward / risk, 2) if reward > 0 else None
    except (TypeError, ValueError):
        return None


class ReclaimWatcher(Strategy):
    """M1 收盘检测 reclaim，推进 approved_wait → ready_to_execute。"""

    def __init__(self, config: ReclaimWatcherConfig) -> None:
        super().__init__(config)
        self._redis: Optional[_redis.Redis] = None

    def on_start(self) -> None:
        try:
            self._redis = _redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT,
                decode_responses=True, socket_timeout=3,
            )
            self._redis.ping()
            self.log.info("[ReclaimWatcher] Redis 已连接")
        except Exception as e:
            self.log.error(f"[ReclaimWatcher] Redis 连接失败: {e}")
            self._redis = None

        self.msgbus.subscribe(topic="bar.collected", handler=self._on_m1_bar)
        self.log.info(
            f"[ReclaimWatcher] 已启动 | max_wait={RECLAIM_WAIT_MAX_MIN}min "
            f"| max_bars={RECLAIM_MAX_M1_BARS} | min_rr={MIN_RR_HALF}"
        )

    def on_stop(self) -> None:
        try:
            self.msgbus.unsubscribe(topic="bar.collected", handler=self._on_m1_bar)
        except Exception:
            pass
        if self._redis:
            self._redis.close()

    def _on_m1_bar(self, event: BarCollectedEvent) -> None:
        if not self._redis:
            return
        sym = event.symbol
        bar = event.bar
        try:
            close = float(bar["close"])
            bar_time = int(bar.get("time") or 0)
        except (KeyError, TypeError, ValueError):
            return
        if not bar_time:
            return

        now = int(time.time())
        for proposal in list_approved_wait(self._redis, symbol=sym):
            self._evaluate(proposal, close, bar_time, now)

    def _evaluate(
        self,
        proposal: dict[str, Any],
        close: float,
        bar_time: int,
        now: int,
    ) -> None:
        if not self._redis:
            return
        pid = str(proposal.get("proposal_id", ""))
        side = str(proposal.get("side", "")).upper()
        try:
            trigger = float(proposal.get("trigger_level"))
            stop = float(proposal.get("stop_price"))
            tp_half = float(proposal.get("tp_half_price") or proposal.get("tp_price"))
        except (TypeError, ValueError):
            mark_reclaim_failed(self._redis, proposal, reason="invalid_prices")
            return

        approved_at = int(proposal.get("approved_at") or proposal.get("decided_at") or 0)

        exp = int(proposal.get("expires_at") or 0)
        if exp and now > exp:
            mark_reclaim_failed(self._redis, proposal, reason="proposal_expired")
            self.log.info(f"[ReclaimWatcher] 过期 id={pid} {proposal.get('symbol')}")
            return

        if approved_at and now - approved_at > RECLAIM_WAIT_MAX_MIN * 60:
            mark_reclaim_failed(self._redis, proposal, reason="reclaim_timeout")
            self.log.info(f"[ReclaimWatcher] 超时 id={pid} {proposal.get('symbol')}")
            return

        if _is_reclaimed(side, close, trigger):
            rr = _rr_half_at_entry(side=side, entry=close, stop=stop, tp_half=tp_half)
            if rr is not None and rr < MIN_RR_HALF:
                mark_reclaim_failed(
                    self._redis, proposal,
                    reason="rr_half_below_min",
                    meta={"rr_half_at_reclaim": rr, "min_rr": MIN_RR_HALF},
                )
                self.log.info(
                    f"[ReclaimWatcher] R:R 不足 id={pid} rr={rr} < {MIN_RR_HALF}"
                )
                return

            mark_ready_to_execute(
                self._redis,
                proposal,
                reclaim_bar_time=bar_time,
                reclaim_close=close,
                rr_half_at_reclaim=rr,
            )
            self.log.info(
                f"[ReclaimWatcher] ✓ reclaim {proposal.get('symbol')} {side} "
                f"close={close:.2f} trigger={trigger:.2f} id={pid} rr={rr}"
            )
            return

        bars_waited = int(proposal.get("reclaim_bars_waited") or 0) + 1
        self._patch(proposal, reclaim_bars_waited=bars_waited)
        if bars_waited > RECLAIM_MAX_M1_BARS:
            mark_reclaim_failed(self._redis, proposal, reason="reclaim_bars_exceeded")
            self.log.info(f"[ReclaimWatcher] 超 M1 根数 id={pid} {proposal.get('symbol')}")

    def _patch(self, proposal: dict[str, Any], **fields: Any) -> None:
        if not self._redis:
            return
        pid = str(proposal["proposal_id"])
        key = f"proposal:approved:{pid}"
        pipe = self._redis.pipeline()
        for k, v in fields.items():
            pipe.hset(key, k, json.dumps(v, ensure_ascii=False))
            proposal[k] = v
        pipe.execute()
