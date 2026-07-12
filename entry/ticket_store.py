"""
Entry Ticket 持久化 — resting/armed 进场单状态（照搬 approval/proposal_store 范式）。

键：
  entry:ticket:{id}     hash — 单张票全字段（每字段 JSON 编码，与 proposal_store 一致）
  entry:pending:index   zset — score=created_ts，活跃票索引（ARMED / TRIGGERED）
  entry:claim:{id}      SET nx ex — 触发抢占锁，防 M1 重复触发

状态机：ARMED →（触发）TRIGGERED →（成交）FILLED
                       ↘ CANCELED / EXPIRED
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

import redis as _redis

TICKET_PREFIX = "entry:ticket"
PENDING_INDEX = "entry:pending:index"
CLAIM_PREFIX = "entry:claim:"
CLAIM_TTL_SECONDS = 600
TICKET_RETENTION_SECONDS = 8 * 3600

ACTIVE_STATES = ("ARMED", "RESTING", "TRIGGERED")


def _key(ticket_id: str) -> str:
    return f"{TICKET_PREFIX}:{ticket_id}"


def _claim_key(ticket_id: str) -> str:
    return f"{CLAIM_PREFIX}{ticket_id}"


def parse_hash(raw: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in raw.items():
        try:
            out[k] = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            out[k] = v
    return out


def new_ticket_id() -> str:
    return f"ent-{int(time.time())}-{uuid.uuid4().hex[:8]}"


def create_ticket(
    r: _redis.Redis,
    *,
    req_dict: dict[str, Any],
    state: str = "ARMED",
    intent_meta: Optional[dict[str, Any]] = None,
    expire_ts: Optional[int] = None,
    ticket_id: Optional[str] = None,
) -> dict[str, Any]:
    tid = ticket_id or new_ticket_id()
    now = int(time.time())
    ticket = {
        "ticket_id": tid,
        "symbol": str(req_dict.get("symbol", "")).upper(),
        "method": req_dict.get("method"),
        "side": req_dict.get("side"),
        "state": state,
        "params": req_dict,
        "intent_meta": intent_meta or {},
        "created_ts": now,
        "expire_ts": int(expire_ts or 0),
        "triggered_ts": 0,
        "trigger_close": 0,
        "entry_coid": "",
        "qty": 0,
        "operator": req_dict.get("operator", "console"),
        "reason": "",
    }
    pipe = r.pipeline()
    pipe.hset(_key(tid), mapping={k: json.dumps(v, ensure_ascii=False) for k, v in ticket.items()})
    pipe.expire(_key(tid), TICKET_RETENTION_SECONDS)
    pipe.zadd(PENDING_INDEX, {tid: now})
    pipe.execute()
    return ticket


def get_ticket(r: _redis.Redis, ticket_id: str) -> Optional[dict[str, Any]]:
    raw = r.hgetall(_key(ticket_id))
    if not raw:
        return None
    return parse_hash(raw)


def list_pending(
    r: _redis.Redis, *, symbol: Optional[str] = None, limit: int = 100,
) -> list[dict[str, Any]]:
    """活跃票（ARMED/TRIGGERED）；顺手清理索引中已终结/缺失的条目。"""
    out: list[dict[str, Any]] = []
    for tid in r.zrevrange(PENDING_INDEX, 0, max(0, limit - 1)):
        t = get_ticket(r, tid)
        if not t:
            r.zrem(PENDING_INDEX, tid)
            continue
        if t.get("state") not in ACTIVE_STATES:
            r.zrem(PENDING_INDEX, tid)
            continue
        if symbol and str(t.get("symbol", "")).upper() != symbol.upper():
            continue
        out.append(t)
    return out


def list_armed_for_symbol(r: _redis.Redis, symbol: str) -> list[dict[str, Any]]:
    """AutoRunner 每根 M1 评估用：返回该标的所有 ARMED 票。"""
    return [t for t in list_pending(r, symbol=symbol, limit=200) if t.get("state") == "ARMED"]


def _update(
    r: _redis.Redis, ticket_id: str, *, fields: dict[str, Any],
    terminal: bool, publish_event: str,
) -> dict[str, Any]:
    t = get_ticket(r, ticket_id)
    if not t:
        return {}
    t.update(fields)
    pipe = r.pipeline()
    pipe.hset(_key(ticket_id), mapping={k: json.dumps(v, ensure_ascii=False) for k, v in t.items()})
    if terminal:
        pipe.zrem(PENDING_INDEX, ticket_id)
        pipe.delete(_claim_key(ticket_id))
    pipe.publish("entry:update", json.dumps({
        "event": publish_event, "ticket_id": ticket_id, "symbol": t.get("symbol"),
        "state": t.get("state"),
        **{k: v for k, v in fields.items() if k != "params"},
    }, ensure_ascii=False))
    pipe.execute()
    return t


def mark_triggered(r, ticket_id, *, trigger_close: float, bar_time: int) -> dict[str, Any]:
    return _update(r, ticket_id,
                   fields={"state": "TRIGGERED", "triggered_ts": int(time.time()),
                           "trigger_close": round(float(trigger_close), 4),
                           "trigger_bar_time": int(bar_time)},
                   terminal=False, publish_event="triggered")


def mark_filled(r, ticket_id, *, entry_coid: str, qty: int) -> dict[str, Any]:
    return _update(r, ticket_id,
                   fields={"state": "FILLED", "entry_coid": str(entry_coid),
                           "qty": int(qty), "filled_ts": int(time.time())},
                   terminal=True, publish_event="filled")


def mark_canceled(r, ticket_id, *, reason: str = "") -> dict[str, Any]:
    return _update(r, ticket_id,
                   fields={"state": "CANCELED", "reason": str(reason),
                           "canceled_ts": int(time.time())},
                   terminal=True, publish_event="canceled")


def mark_expired(r, ticket_id) -> dict[str, Any]:
    return _update(r, ticket_id,
                   fields={"state": "EXPIRED", "expired_ts": int(time.time())},
                   terminal=True, publish_event="expired")


def mark_observed(r, ticket_id) -> dict[str, Any]:
    """观察模式 dry-run（未实际下单）— 终态。"""
    return _update(r, ticket_id,
                   fields={"state": "OBSERVED", "observed_ts": int(time.time())},
                   terminal=True, publish_event="observed")


def update(r, ticket_id, *, fields, event: str = "updated", terminal: bool = False) -> dict[str, Any]:
    """通用字段更新（回填 entry_coid / 改触发价 / 改 params）。"""
    return _update(r, ticket_id, fields=fields, terminal=terminal, publish_event=event)


def claim(r: _redis.Redis, ticket_id: str) -> bool:
    """原子抢占触发权，防 M1 回调重复触发同一张票。"""
    return bool(r.set(_claim_key(ticket_id), "1", nx=True, ex=CLAIM_TTL_SECONDS))


def release(r: _redis.Redis, ticket_id: str) -> None:
    r.delete(_claim_key(ticket_id))


def expire_due(r: _redis.Redis, *, now_ts: Optional[int] = None) -> list[str]:
    """扫描活跃票，把已过期（expire_ts>0 且 < now）的标记 EXPIRED，返回被过期的 id 列表。"""
    now = int(now_ts if now_ts is not None else time.time())
    expired_ids: list[str] = []
    for t in list_pending(r, limit=500):
        exp = int(t.get("expire_ts") or 0)
        if exp and now > exp:
            mark_expired(r, t["ticket_id"])
            expired_ids.append(t["ticket_id"])
    return expired_ids
