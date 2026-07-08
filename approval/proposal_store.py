"""
Redis 交易建议（Proposal）读写 — Agent / 前端 / 引擎共用。
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Optional

import redis as _redis

log = logging.getLogger(__name__)

# Redis key 保留时长（与交易 expires_at 无关，仅供页面/审批留存）
PROPOSAL_REDIS_RETENTION_SECONDS = int(
    os.environ.get("PROPOSAL_REDIS_RETENTION_SECONDS", 8 * 3600)
)

PENDING_INDEX = "proposal:pending:index"
APPROVED_INDEX = "proposal:approved:index"
REJECTED_INDEX = "proposal:rejected:index"
EXECUTED_INDEX = "proposal:executed:index"
EXEC_CLAIM_PREFIX = "proposal:exec_claim:"
EXEC_CLAIM_TTL_SECONDS = int(os.environ.get("PROPOSAL_EXEC_CLAIM_TTL_SECONDS", 600))


def _key(status: str, proposal_id: str) -> str:
    return f"proposal:{status}:{proposal_id}"


def parse_hash(raw: dict[str, str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in raw.items():
        try:
            out[k] = json.loads(v)
        except (json.JSONDecodeError, TypeError):
            out[k] = v
    return out


def list_ids(r: _redis.Redis, status: str, limit: int = 50) -> list[str]:
    index = {
        "pending": PENDING_INDEX,
        "approved": APPROVED_INDEX,
        "rejected": REJECTED_INDEX,
        "executed": EXECUTED_INDEX,
    }.get(status)
    if not index:
        return []
    return r.zrevrange(index, 0, max(0, limit - 1))


def get_proposal(r: _redis.Redis, status: str, proposal_id: str) -> Optional[dict[str, Any]]:
    raw = r.hgetall(_key(status, proposal_id))
    if not raw:
        return None
    return parse_hash(raw)


def list_proposals(
    r: _redis.Redis,
    status: str,
    *,
    symbol: Optional[str] = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    index = {
        "pending": PENDING_INDEX,
        "approved": APPROVED_INDEX,
        "rejected": REJECTED_INDEX,
        "executed": EXECUTED_INDEX,
    }.get(status)
    for pid in list_ids(r, status, limit=limit):
        p = get_proposal(r, status, pid)
        if not p:
            if index:
                r.zrem(index, pid)
            continue
        if symbol and str(p.get("symbol", "")).upper() != symbol.upper():
            continue
        out.append(p)
    return out


def list_approved_wait(
    r: _redis.Redis,
    *,
    symbol: Optional[str] = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    now = int(time.time())
    for pid in list_ids(r, "approved", limit=limit):
        p = get_proposal(r, "approved", pid)
        if not p:
            continue
        if str(p.get("execution_phase") or "") != "approved_wait":
            continue
        if symbol and str(p.get("symbol", "")).upper() != symbol.upper():
            continue
        exp = int(p.get("expires_at") or 0)
        if exp and now > exp:
            continue
        out.append(p)
    return out


def _save_approved(r: _redis.Redis, payload: dict[str, Any], *, event: str, extra: Optional[dict[str, Any]] = None) -> None:
    pid = str(payload["proposal_id"])
    pipe = r.pipeline()
    pipe.hset(_key("approved", pid), mapping={
        k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()
    })
    pipe.expire(_key("approved", pid), PROPOSAL_REDIS_RETENTION_SECONDS)
    pub: dict[str, Any] = {
        "event": event,
        "proposal_id": pid,
        "symbol": payload.get("symbol"),
        "execution_phase": payload.get("execution_phase"),
    }
    if extra:
        pub.update(extra)
    pipe.publish("proposal:update", json.dumps(pub, ensure_ascii=False))
    pipe.execute()


def reject_pending(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    reason: str,
    operator: str = "alpha_purge",
) -> None:
    """pending → rejected（MCP 扫描前清理 / 脚本用）。"""
    pid = str(proposal["proposal_id"])
    now = int(time.time())
    payload = {
        **proposal,
        "status": "rejected",
        "decision": "rejected",
        "approver": operator,
        "comment": reason,
        "decided_at": now,
        "purge_reason": reason,
    }
    pipe = r.pipeline()
    pipe.hset(_key("rejected", pid), mapping={
        k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()
    })
    pipe.expire(_key("rejected", pid), PROPOSAL_REDIS_RETENTION_SECONDS)
    pipe.zadd(REJECTED_INDEX, {pid: now})
    pipe.delete(_key("pending", pid))
    pipe.zrem(PENDING_INDEX, pid)
    pipe.publish("proposal:update", json.dumps({
        "event": "rejected",
        "proposal_id": pid,
        "symbol": payload.get("symbol"),
        "reason": reason,
        "operator": operator,
    }, ensure_ascii=False))
    pipe.execute()


def cancel_approved_proposal(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    comment: str = "",
    operator: str = "operator",
) -> None:
    """approved → rejected（用户撤销批准，reclaim 前可取消）。"""
    pid = str(proposal["proposal_id"])
    phase = str(proposal.get("execution_phase") or "")
    if proposal.get("executed_at"):
        raise ValueError("已执行，无法取消")
    if phase not in ("approved_wait", "ready_to_execute"):
        raise ValueError(f"当前阶段不可取消: {phase or 'unknown'}")

    now = int(time.time())
    payload = {
        **proposal,
        "status": "rejected",
        "decision": "rejected",
        "execution_phase": "cancelled",
        "approver": operator,
        "comment": comment or "用户取消批准",
        "cancelled_at": now,
        "decided_at": proposal.get("decided_at") or now,
    }
    pipe = r.pipeline()
    pipe.hset(_key("rejected", pid), mapping={
        k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()
    })
    pipe.expire(_key("rejected", pid), PROPOSAL_REDIS_RETENTION_SECONDS)
    pipe.zadd(REJECTED_INDEX, {pid: now})
    pipe.delete(_key("approved", pid))
    pipe.zrem(APPROVED_INDEX, pid)
    pipe.delete(f"{EXEC_CLAIM_PREFIX}{pid}")
    pipe.publish("proposal:update", json.dumps({
        "event": "cancelled",
        "proposal_id": pid,
        "symbol": payload.get("symbol"),
        "side": payload.get("side"),
        "operator": operator,
    }, ensure_ascii=False))
    pipe.execute()


def mark_ready_to_execute(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    reclaim_bar_time: int,
    reclaim_close: float,
    rr_half_at_reclaim: Optional[float] = None,
) -> None:
    payload = dict(proposal)
    payload["execution_phase"] = "ready_to_execute"
    payload["reclaim_bar_time"] = reclaim_bar_time
    payload["reclaim_close"] = round(reclaim_close, 4)
    payload["entry_price"] = round(reclaim_close, 2)
    payload["ready_at"] = int(time.time())
    if rr_half_at_reclaim is not None:
        payload["rr_half_at_reclaim"] = rr_half_at_reclaim
    _save_approved(r, payload, event="reclaim_ready")


def mark_reclaim_failed(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    reason: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    payload = dict(proposal)
    payload["execution_phase"] = "failed"
    payload["reclaim_fail_reason"] = reason
    payload["failed_at"] = int(time.time())
    if meta:
        payload["reclaim_fail_meta"] = meta
    _save_approved(r, payload, event="reclaim_failed")


def try_claim_execution(r: _redis.Redis, proposal_id: str) -> bool:
    """原子抢占执行权，避免 M1 回调重复下单。"""
    key = f"{EXEC_CLAIM_PREFIX}{proposal_id}"
    return bool(r.set(key, "1", nx=True, ex=EXEC_CLAIM_TTL_SECONDS))


def release_execution_claim(r: _redis.Redis, proposal_id: str) -> None:
    r.delete(f"{EXEC_CLAIM_PREFIX}{proposal_id}")


def pop_approved_for_symbol(
    r: _redis.Redis,
    symbol: str,
    *,
    decision: str = "approved_live",
) -> list[dict[str, Any]]:
    """取某标的未过期、可立即执行的已批准建议（跳过 approved_wait 条件单）。"""
    now = int(time.time())
    matched: list[dict[str, Any]] = []
    for pid in list_ids(r, "approved", limit=100):
        p = get_proposal(r, "approved", pid)
        if not p:
            continue
        if str(p.get("symbol", "")).upper() != symbol.upper():
            continue
        if p.get("decision") != decision:
            continue
        if p.get("executed_at"):
            continue
        phase = str(p.get("execution_phase") or "")
        if phase in ("approved_wait", "pending", "executing"):
            continue
        if str(p.get("execution_mode") or "") == "conditional_reclaim" and phase != "ready_to_execute":
            continue
        exp = int(p.get("expires_at") or 0)
        if exp and now > exp:
            continue
        matched.append(p)
    return matched


def mark_executing(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    """实盘报单已提交、待成交 — 仍保留在 approved。"""
    payload = dict(proposal)
    payload["execution_phase"] = "executing"
    payload["executing_at"] = int(time.time())
    if meta:
        payload["exec_meta"] = meta
    _save_approved(r, payload, event="executing")
    log.info(
        "[Proposal] executing %s %s qty=%s",
        payload.get("symbol"), payload.get("proposal_id"), (meta or {}).get("qty"),
    )


def mark_submit_failed(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    reason: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    """入场失败/被拒 — 释放 claim，恢复 ready_to_execute 供下一根 M1 重试。"""
    pid = str(proposal["proposal_id"])
    payload = dict(proposal)
    payload["execution_phase"] = "ready_to_execute"
    payload["last_submit_fail_reason"] = reason
    payload["last_submit_fail_at"] = int(time.time())
    if meta:
        payload["last_submit_fail_meta"] = meta
    for k in ("executing_at",):
        payload.pop(k, None)
    release_execution_claim(r, pid)
    _save_approved(r, payload, event="submit_failed", extra={"reason": reason})
    log.warning(
        "[Proposal] submit_failed %s %s reason=%s",
        payload.get("symbol"), pid, reason,
    )


def mark_executed(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    result: str,
    meta: Optional[dict[str, Any]] = None,
) -> None:
    pid = str(proposal["proposal_id"])
    payload = dict(proposal)
    payload["status"] = "executed"
    payload["executed_at"] = int(time.time())
    payload["exec_result"] = result
    if meta:
        payload["exec_meta"] = meta

    pipe = r.pipeline()
    pipe.hset(_key("executed", pid), mapping={
        k: json.dumps(v, ensure_ascii=False) for k, v in payload.items()
    })
    pipe.expire(_key("executed", pid), PROPOSAL_REDIS_RETENTION_SECONDS)
    pipe.zadd(EXECUTED_INDEX, {pid: payload["executed_at"]})
    pipe.delete(_key("approved", pid))
    pipe.delete(f"{EXEC_CLAIM_PREFIX}{pid}")
    pipe.zrem(APPROVED_INDEX, pid)
    pipe.publish("proposal:update", json.dumps({
        "event": "executed",
        "proposal_id": pid,
        "symbol": payload.get("symbol"),
        "result": result,
    }, ensure_ascii=False))
    pipe.execute()
    log.info(
        "[Proposal] ✓ executed %s %s result=%s",
        payload.get("symbol"), pid, result,
    )
