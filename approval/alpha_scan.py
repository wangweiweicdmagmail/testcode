"""Alpha 增量扫描：顺势 / R:R / pending 去重 / 触线时效（MCP 与脚本共用）。"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any, Optional

import redis as _redis

from approval.proposal_builder import (
    build_proposal_payload,
    proposal_id,
    store_pending,
    touch_from_dict,
)
from approval.proposal_store import get_proposal, list_ids
from nautilus_mcp.redis_io import DEFAULT_SYMBOLS, list_recent_touches
from signals.st_super import SIGNAL_ST_SUPER
from signals.touch_detector import TouchEvent, m5_st_dir

log = logging.getLogger(__name__)

MIN_RR_HALF = float(
    os.environ.get("ALPHA_MIN_RR_HALF", os.environ.get("RECLAIM_MIN_RR_HALF", "1.0"))
)
MAX_TOUCH_AGE = int(os.environ.get("ALPHA_TOUCH_MAX_AGE_SECONDS", "300"))
MAX_PER_SYMBOL = int(os.environ.get("ALPHA_MAX_PER_SYMBOL", "1"))
MAX_PER_SCAN = int(os.environ.get("ALPHA_MAX_PER_SCAN", "3"))
# 仅超级信号；回踩类已禁用（反转已包含回踩语义）
PRIMARY_SIGNAL = SIGNAL_ST_SUPER


def _signal_allowed(signal_type: str) -> bool:
    return signal_type == PRIMARY_SIGNAL


@dataclass
class SkipRecord:
    symbol: str
    signal_type: str
    side: str
    touch_time: int
    reason: str


@dataclass
class ScanResult:
    created: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[SkipRecord] = field(default_factory=list)

    @property
    def no_op(self) -> bool:
        return len(self.created) == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "no_op": self.no_op,
            "created_count": len(self.created),
            "created": self.created,
            "skipped_count": len(self.skipped),
            "skipped": [
                {
                    "symbol": s.symbol,
                    "signal_type": s.signal_type,
                    "side": s.side,
                    "touch_time": s.touch_time,
                    "reason": s.reason,
                }
                for s in self.skipped
            ],
        }


def _latest_m1_bar_time(r: _redis.Redis, symbol: str) -> Optional[int]:
    raw = r.lindex(f"bars:1m:{symbol.upper()}", -1)
    if not raw:
        return None
    try:
        return int(json.loads(raw).get("time") or 0) or None
    except (json.JSONDecodeError, TypeError, ValueError):
        return None


def _read_active(r: _redis.Redis, symbol: str) -> Optional[dict[str, Any]]:
    raw = r.get(f"indicators:active:{symbol.upper()}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def side_matches_m5_trend(side: str, m5_dir: int) -> bool:
    s = side.upper()
    if m5_dir not in (1, -1):
        return False
    if s == "LONG":
        return m5_dir == 1
    if s == "SHORT":
        return m5_dir == -1
    return False


def approved_sides_by_symbol(r: _redis.Redis, *, limit: int = 100) -> dict[str, set[str]]:
    """未执行、未过期的已批准建议（approved_wait / ready_to_execute）按标的+方向索引。"""
    import time as _time

    out: dict[str, set[str]] = {}
    now = int(_time.time())
    for pid in list_ids(r, "approved", limit=limit):
        p = get_proposal(r, "approved", pid)
        if not p or p.get("executed_at"):
            continue
        phase = str(p.get("execution_phase") or "")
        if phase not in ("approved_wait", "ready_to_execute"):
            continue
        exp = int(p.get("expires_at") or 0)
        if exp and now > exp:
            continue
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        if sym and side:
            out.setdefault(sym, set()).add(side)
    return out


def has_pending_same_side(
    r: _redis.Redis,
    symbol: str,
    side: str,
    *,
    pending_sides: Optional[dict[str, set[str]]] = None,
) -> bool:
    sym = symbol.upper()
    side_u = side.upper()
    if pending_sides is not None:
        return side_u in pending_sides.get(sym, set())
    for pid in list_ids(r, "pending", limit=80):
        p = get_proposal(r, "pending", pid)
        if not p:
            continue
        if str(p.get("symbol", "")).upper() == sym and str(p.get("side", "")).upper() == side_u:
            return True
    return False


def has_active_proposal_same_side(
    r: _redis.Redis,
    symbol: str,
    side: str,
    *,
    pending_sides: Optional[dict[str, set[str]]] = None,
    approved_sides: Optional[dict[str, set[str]]] = None,
) -> bool:
    sym, side_u = symbol.upper(), side.upper()
    if pending_sides is not None:
        if side_u in pending_sides.get(sym, set()):
            return True
    elif has_pending_same_side(r, symbol, side):
        return True
    if approved_sides is not None:
        return side_u in approved_sides.get(sym, set())
    return side_u in approved_sides_by_symbol(r).get(sym, set())


def pending_sides_by_symbol(r: _redis.Redis, *, limit: int = 80) -> dict[str, set[str]]:
    """一次扫描 pending 索引，供本轮 evaluate 复用。"""
    out: dict[str, set[str]] = {}
    for pid in list_ids(r, "pending", limit=limit):
        p = get_proposal(r, "pending", pid)
        if not p:
            continue
        sym = str(p.get("symbol", "")).upper()
        side = str(p.get("side", "")).upper()
        if sym and side:
            out.setdefault(sym, set()).add(side)
    return out


def touch_too_old(
    r: _redis.Redis,
    symbol: str,
    touch_time: int,
    *,
    max_age: int,
    latest_m1_time: Optional[int] = None,
) -> bool:
    latest = latest_m1_time if latest_m1_time is not None else _latest_m1_bar_time(r, symbol)
    if not latest or not touch_time:
        return True
    return touch_time < latest - max_age


def evaluate_touch(
    r: _redis.Redis,
    event: TouchEvent,
    *,
    max_age: int = MAX_TOUCH_AGE,
    min_rr: float = MIN_RR_HALF,
    pending_sides: Optional[dict[str, set[str]]] = None,
    approved_sides: Optional[dict[str, set[str]]] = None,
    latest_m1_times: Optional[dict[str, int]] = None,
    trust_signal_m5: bool = False,
) -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """返回 (payload, skip_reason)。通过则 payload 非空。"""
    if not _signal_allowed(event.signal_type):
        return None, "pullback_disabled"

    latest = (latest_m1_times or {}).get(event.symbol.upper())
    if touch_too_old(
        r, event.symbol, event.touch_time, max_age=max_age, latest_m1_time=latest,
    ):
        return None, "touch_too_old"

    if not trust_signal_m5:
        active = _read_active(r, event.symbol)
        m5_dir = m5_st_dir(active)
        if not side_matches_m5_trend(event.side, m5_dir):
            return None, "st_super_m5_mismatch"

    if has_active_proposal_same_side(
        r, event.symbol, event.side,
        pending_sides=pending_sides, approved_sides=approved_sides,
    ):
        return None, "active_proposal_same_side"

    pid = proposal_id(event.symbol, event.signal_type, event.side, event.touch_time)
    if r.exists(f"proposal:dedup:{pid}"):
        return None, "duplicate_proposal"

    payload = build_proposal_payload(
        r,
        event,
        thesis=event.rule_thesis,
        confidence=event.rule_confidence,
    )
    if not payload:
        return None, "pricing_failed"

    rr = payload.get("rr_half_est")
    if rr is None or float(rr) < min_rr:
        return None, "rr_below_min"

    return payload, None


def collect_candidate_touches(
    r: _redis.Redis,
    symbols: tuple[str, ...] | list[str],
    *,
    max_age: int = MAX_TOUCH_AGE,
    limit: int = 40,
) -> list[TouchEvent]:
    """最近触线中，在时效窗口内且属于扫描标的的候选。"""
    sym_set = {s.upper() for s in symbols}
    latest_m1_times: dict[str, int] = {}
    for sym in sym_set:
        t = _latest_m1_bar_time(r, sym)
        if t:
            latest_m1_times[sym] = t
    out: list[TouchEvent] = []
    seen: set[str] = set()
    for raw in list_recent_touches(r, limit=limit):
        sym = str(raw.get("symbol", "")).upper()
        if sym not in sym_set:
            continue
        try:
            ev = touch_from_dict(raw)
        except (KeyError, TypeError, ValueError):
            continue
        if not _signal_allowed(ev.signal_type):
            continue
        if touch_too_old(
            r, sym, ev.touch_time, max_age=max_age, latest_m1_time=latest_m1_times.get(sym),
        ):
            continue
        key = f"{ev.symbol}:{ev.signal_type}:{ev.side}:{ev.touch_time}"
        if key in seen:
            continue
        seen.add(key)
        out.append(ev)
    return out


def run_incremental_scan(
    r: _redis.Redis,
    symbols: Optional[tuple[str, ...] | list[str]] = None,
    *,
    incremental: bool = True,
    max_age: Optional[int] = None,
    max_per_symbol: int = MAX_PER_SYMBOL,
    max_per_scan: int = MAX_PER_SCAN,
) -> ScanResult:
    """
    增量 Alpha 扫描：过滤后每标的最多 max_per_symbol 条，全市场最多 max_per_scan 条。
    incremental=False 时放宽触线时效（仅用于手动调试）。
    """
    syms = tuple(symbols or DEFAULT_SYMBOLS)
    age = max_age if max_age is not None else (MAX_TOUCH_AGE if incremental else 24 * 3600)
    result = ScanResult()
    pending_sides = pending_sides_by_symbol(r)
    approved_sides = approved_sides_by_symbol(r)
    latest_m1_times = {
        sym: t for sym in {s.upper() for s in syms}
        if (t := _latest_m1_bar_time(r, sym))
    }

    candidates = collect_candidate_touches(r, syms, max_age=age)
    # 置信度高的优先
    ranked: list[tuple[TouchEvent, Optional[dict[str, Any]], Optional[str]]] = []
    for ev in candidates:
        payload, reason = evaluate_touch(
            r, ev, max_age=age,
            pending_sides=pending_sides,
            approved_sides=approved_sides,
            latest_m1_times=latest_m1_times,
        )
        if payload:
            ranked.append((ev, payload, None))
        else:
            result.skipped.append(
                SkipRecord(ev.symbol, ev.signal_type, ev.side, ev.touch_time, reason or "skip")
            )

    ranked.sort(key=lambda x: float(x[1].get("confidence", 0)), reverse=True)

    per_sym_count: dict[str, int] = {}
    for ev, payload, _ in ranked:
        if len(result.created) >= max_per_scan:
            result.skipped.append(
                SkipRecord(ev.symbol, ev.signal_type, ev.side, ev.touch_time, "scan_cap")
            )
            continue
        n = per_sym_count.get(ev.symbol, 0)
        if n >= max_per_symbol:
            result.skipped.append(
                SkipRecord(ev.symbol, ev.signal_type, ev.side, ev.touch_time, "symbol_cap")
            )
            continue
        sym_u, side_u = ev.symbol.upper(), ev.side.upper()
        if side_u in pending_sides.get(sym_u, set()) or side_u in approved_sides.get(sym_u, set()):
            result.skipped.append(
                SkipRecord(ev.symbol, ev.signal_type, ev.side, ev.touch_time, "active_proposal_same_side")
            )
            continue
        created, msg = store_pending(r, payload)
        if created:
            per_sym_count[ev.symbol] = n + 1
            result.created.append(payload)
            pending_sides.setdefault(ev.symbol.upper(), set()).add(ev.side.upper())
        else:
            result.skipped.append(
                SkipRecord(ev.symbol, ev.signal_type, ev.side, ev.touch_time, msg)
            )

    log.info(
        "[AlphaScan] 完成 symbols=%s candidates=%d created=%d skipped=%d incremental=%s",
        ",".join(syms),
        len(candidates),
        len(result.created),
        len(result.skipped),
        incremental,
    )
    if result.created:
        for p in result.created:
            log.info(
                "[AlphaScan] ✓ pending %s %s id=%s stop=%s",
                p.get("symbol"), p.get("side"), p.get("proposal_id"), p.get("stop_price"),
            )
    elif result.skipped and not incremental:
        log.info(
            "[AlphaScan] 全部跳过，首条原因: %s",
            result.skipped[0].reason if result.skipped else "—",
        )

    return result


def auto_proposal_from_touch(
    r: _redis.Redis,
    event: TouchEvent,
) -> tuple[bool, str]:
    """
    引擎内实时 st_super 触线 → 直接写入 pending 建议（不依赖 MCP / 外部扫描）。
    返回 (created, proposal_id_or_skip_reason)。
    """
    payload, reason = evaluate_touch(r, event, trust_signal_m5=True)
    if not payload:
        return False, reason or "rejected"
    created, msg = store_pending(r, payload)
    if not created:
        return False, msg
    return True, msg


def try_create_proposal_from_touch(
    r: _redis.Redis,
    event: TouchEvent,
    *,
    thesis: str = "",
    confidence: Optional[float] = None,
) -> tuple[bool, dict[str, Any]]:
    """单条触线建单（MCP create_proposal 用）。"""
    if not _signal_allowed(event.signal_type):
        return False, {
            "ok": False,
            "error": "pullback_disabled",
            "hint": "仅支持 st_super 超级信号，回踩类已禁用",
        }
    ev = event
    if thesis or confidence is not None:
        ev = TouchEvent(
            symbol=event.symbol,
            signal_type=event.signal_type,
            side=event.side,
            trigger_level=event.trigger_level,
            touch_time=event.touch_time,
            m1_bar_time=event.m1_bar_time,
            m5_context_bar_time=event.m5_context_bar_time,
            session_date=event.session_date,
            m1_high=event.m1_high,
            m1_low=event.m1_low,
            m1_close=event.m1_close,
            reclaim=event.reclaim,
            rule_confidence=confidence if confidence is not None else event.rule_confidence,
            rule_thesis=thesis or event.rule_thesis,
        )
    payload, reason = evaluate_touch(r, ev)
    if not payload:
        return False, {"ok": False, "error": reason or "rejected"}
    created, msg = store_pending(r, payload)
    if not created:
        return False, {"ok": False, "error": msg}
    return True, {"ok": True, "proposal_id": msg, "proposal": payload}
