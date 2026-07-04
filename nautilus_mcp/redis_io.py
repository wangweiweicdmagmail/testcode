"""MCP 层 Redis 只读/写封装。"""
from __future__ import annotations

import json
import os
import time
from typing import Any, Optional

import redis

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
# 心跳 ts 与当前时间差超过此值视为离线（引擎每 5s 发一次）
ENGINE_HEARTBEAT_MAX_AGE_S = int(os.environ.get("ENGINE_HEARTBEAT_MAX_AGE_S", 30))
TOUCH_INDEX = "signals:touch:index"
DEFAULT_SYMBOLS = tuple(
    s.strip().upper()
    for s in os.environ.get("ALPHA_SYMBOLS", "NVDA,TSLA,AAPL").split(",")
    if s.strip()
)

_redis_client: Optional[redis.Redis] = None


def get_redis() -> redis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_timeout=3,
        )
        _redis_client.ping()
    return _redis_client


def get_indicators_active(r: redis.Redis, symbol: str) -> Optional[dict[str, Any]]:
    raw = r.get(f"indicators:active:{symbol.upper()}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def get_bars(r: redis.Redis, timeframe: str, symbol: str, *, limit: int = 30) -> list[dict[str, Any]]:
    key = f"bars:{timeframe}:{symbol.upper()}"
    raw_list = r.lrange(key, -limit, -1)
    out: list[dict[str, Any]] = []
    for raw in raw_list:
        try:
            out.append(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return out


def list_recent_touches(
    r: redis.Redis,
    *,
    symbol: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """从 signals:touch:index + 各 symbol list 读取最近触线。"""
    members = r.zrevrange(TOUCH_INDEX, 0, max(0, limit * 3 - 1))
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for member in members:
        parts = member.split(":", 2)
        if len(parts) < 3:
            continue
        sym, sig, t_str = parts[0], parts[1], parts[2]
        if symbol and sym.upper() != symbol.upper():
            continue
        dedup = f"{sym}:{sig}:{t_str}"
        if dedup in seen:
            continue
        seen.add(dedup)
        raw_list = r.lrange(f"signals:touch:{sym}", -10, -1)
        for raw in reversed(raw_list):
            try:
                d = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if str(d.get("signal_type")) == sig and str(d.get("touch_time")) == t_str:
                out.append(d)
                break
        if len(out) >= limit:
            break
    return out


def list_pending_proposals(
    r: redis.Redis,
    *,
    symbol: Optional[str] = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    from approval.proposal_store import get_proposal, list_ids

    out: list[dict[str, Any]] = []
    for pid in list_ids(r, "pending", limit=limit):
        p = get_proposal(r, "pending", pid)
        if not p:
            continue
        if symbol and str(p.get("symbol", "")).upper() != symbol.upper():
            continue
        out.append(p)
    return out


def parse_engine_heartbeat(raw: Optional[str]) -> tuple[Any, Optional[int]]:
    """解析 engine:heartbeat JSON，返回 (payload, age_s)。"""
    if not raw:
        return None, None
    try:
        hb = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return raw, None
    age_s: Optional[int] = None
    if isinstance(hb, dict) and hb.get("ts"):
        age_s = max(0, int(time.time()) - int(hb["ts"]))
    return hb, age_s


def stack_health(r: redis.Redis) -> dict[str, Any]:
    from approval.proposal_store import PENDING_INDEX

    pending = r.zcard(PENDING_INDEX)
    hb, hb_age_s = parse_engine_heartbeat(r.get("engine:heartbeat"))
    return {
        "redis_ok": True,
        "pending_proposals": pending,
        "engine_heartbeat": hb,
        "engine_heartbeat_age_s": hb_age_s,
        "engine_heartbeat_max_age_s": ENGINE_HEARTBEAT_MAX_AGE_S,
        "engine_online": hb_age_s is not None and hb_age_s <= ENGINE_HEARTBEAT_MAX_AGE_S,
        "symbols": list(DEFAULT_SYMBOLS),
    }
