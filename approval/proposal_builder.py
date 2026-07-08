"""从触线事件构建并写入 pending proposal（MCP / 脚本共用）。"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from typing import Any, Optional

import redis as _redis

from approval.proposal_store import PENDING_INDEX, PROPOSAL_REDIS_RETENTION_SECONDS
from signals.indicators import session_vwap
from signals.st_super import (
    EXECUTION_MODE_ST_SUPER,
    SIGNAL_ST_SUPER,
    pricing_from_st_super,
)
from signals.touch_detector import TouchEvent, m5_st_dir

EXECUTION_MODE = "conditional_reclaim"
POSITION_PLAN = "half_tp_then_trail"
PROPOSAL_CHANNEL = "proposal:update"
DEFAULT_TTL = int(__import__("os").environ.get("ALPHA_PROPOSAL_TTL_SECONDS", 30 * 60))

log = logging.getLogger(__name__)

def proposal_id(symbol: str, signal_type: str, side: str, touch_time: int) -> str:
    raw = f"{symbol}|{signal_type}|{side}|{touch_time}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _input_hash(event: TouchEvent) -> str:
    raw = json.dumps(event.to_dict(), sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _read_m1_bars(r: _redis.Redis, symbol: str, *, limit: int = 30) -> list[dict[str, Any]]:
    raw_list = r.lrange(f"bars:1m:{symbol}", -limit, -1)
    out: list[dict[str, Any]] = []
    for raw in raw_list:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return _attach_vwap(out)


def _attach_vwap(bars: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not bars:
        return bars
    enriched: list[dict[str, Any]] = []
    session_bars: list[dict[str, Any]] = []
    last_date = ""
    for b in bars:
        bar = dict(b)
        from signals.indicators import bar_et_date
        d = bar_et_date(bar) or ""
        if d != last_date:
            session_bars = []
            last_date = d
        session_bars.append(bar)
        if bar.get("vwap") is None:
            v = session_vwap(session_bars)
            if v is not None:
                bar["vwap"] = v
        enriched.append(bar)
    return enriched


def touch_from_dict(d: dict[str, Any]) -> TouchEvent:
    return TouchEvent(
        symbol=str(d["symbol"]).upper(),
        signal_type=str(d["signal_type"]),
        side=str(d["side"]).upper(),
        trigger_level=float(d["trigger_level"]),
        touch_time=int(d["touch_time"]),
        m1_bar_time=int(d.get("m1_bar_time") or d["touch_time"]),
        m5_context_bar_time=d.get("m5_context_bar_time"),
        session_date=str(d.get("session_date") or ""),
        m1_high=float(d["m1_high"]),
        m1_low=float(d["m1_low"]),
        m1_close=float(d["m1_close"]),
        reclaim=bool(d.get("reclaim")),
        rule_confidence=float(d.get("rule_confidence", 0.5)),
        rule_thesis=str(d.get("rule_thesis") or ""),
    )


def build_st_super_proposal_payload(
    r: _redis.Redis,
    event: TouchEvent,
    *,
    thesis: str,
    confidence: float,
    ttl_seconds: int = DEFAULT_TTL,
) -> Optional[dict[str, Any]]:
    px = pricing_from_st_super(event)
    if not px:
        return None
    now_ts = int(time.time())
    side = event.side.upper()
    pid = proposal_id(event.symbol, event.signal_type, side, event.touch_time)
    m5_st_dir_val, m5_st_value, m5_bar_time = 0, None, None
    try:
        raw_active = r.get(f"indicators:active:{event.symbol}")
        if raw_active:
            active = json.loads(raw_active)
            m5_st_dir_val = m5_st_dir(active)
            m5_st_value = (active.get("supertrend") or {}).get("value")
            m5_bar_time = active.get("m5_bar_time")
    except Exception:
        pass
    return {
        "proposal_id": pid,
        "symbol": event.symbol,
        "side": side,
        "signal_type": SIGNAL_ST_SUPER,
        "thesis": thesis.strip() or event.rule_thesis,
        "confidence": round(max(0.0, min(1.0, confidence)), 3),
        "entry_price": px["entry_price"],
        "stop_price": px["stop_price"],
        "tp_price": px["tp_price"],
        "tp_half_price": px["tp_half_price"],
        "trigger_level": px["stop_price"],
        "bar_time": event.m1_bar_time,
        "touch_time": event.touch_time,
        "ttl_seconds": ttl_seconds,
        "expires_at": now_ts + ttl_seconds,
        "created_at": now_ts,
        "input_hash": _input_hash(event),
        "status": "pending",
        "execution_mode": EXECUTION_MODE_ST_SUPER,
        "execution_phase": "pending",
        "reclaim_rule": "审批通过后立即 ready（超级信号已在翻转 K 对齐）",
        "reclaim_label": "超级信号：审批通过即可执行",
        "touch_reclaimed_at_submit": True,
        "pullback_extreme": px["pullback_extreme"],
        "prior_swing": px["prior_swing"],
        "rr_half_est": px["rr_half_est"],
        "risk_est": px["risk_est"],
        "reward_half_est": px["reward_half_est"],
        "position_plan": POSITION_PLAN,
        "source": "st_super",
        "m5_st_dir": m5_st_dir_val,
        "m5_st_value": m5_st_value,
        "m5_context_bar_time": m5_bar_time,
        "st_super_stop_1m": px["stop_price"],
    }


def build_proposal_payload(
    r: _redis.Redis,
    event: TouchEvent,
    *,
    thesis: str,
    confidence: float,
    ttl_seconds: int = DEFAULT_TTL,
) -> Optional[dict[str, Any]]:
    if event.signal_type != SIGNAL_ST_SUPER:
        return None
    return build_st_super_proposal_payload(
        r, event, thesis=thesis, confidence=confidence, ttl_seconds=ttl_seconds,
    )


def store_pending(r: _redis.Redis, payload: dict[str, Any]) -> tuple[bool, str]:
    """写入 pending；返回 (created, message)。"""
    pid = str(payload["proposal_id"])
    dedup_key = f"proposal:dedup:{pid}"
    if not r.set(dedup_key, "1", nx=True, ex=2 * 24 * 3600):
        log.info("[Proposal] 跳过重复 pending id=%s symbol=%s", pid, payload.get("symbol"))
        return False, f"duplicate proposal_id={pid}"

    key = f"proposal:pending:{pid}"
    pipe = r.pipeline()
    pipe.hset(key, mapping={k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()})
    # Redis 保留 8h（默认），与 expires_at 交易有效期（默认 30min）分开
    pipe.expire(key, PROPOSAL_REDIS_RETENTION_SECONDS)
    pipe.zadd(PENDING_INDEX, {pid: int(payload["created_at"])})
    notify = {**payload, "event": "created"}
    pipe.publish(PROPOSAL_CHANNEL, json.dumps(notify, ensure_ascii=False))
    pipe.execute()
    log.info(
        "[Proposal] ✓ pending %s %s %s entry=%s stop=%s",
        payload.get("symbol"), payload.get("side"), pid,
        payload.get("entry_price"), payload.get("stop_price"),
    )
    return True, pid
