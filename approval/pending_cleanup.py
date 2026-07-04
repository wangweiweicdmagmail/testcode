"""扫描前清理过期 / 逆势 / 过时触线的 pending 建议。"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import redis as _redis

from approval.alpha_scan import (
    MAX_TOUCH_AGE,
    _latest_m1_bar_time,
    side_matches_m5_trend,
)
from approval.proposal_store import get_proposal, list_ids, reject_pending
from signals.touch_detector import m5_st_dir

STALE_TOUCH_AGE = int(os.environ.get("ALPHA_PURGE_TOUCH_AGE_SECONDS", str(MAX_TOUCH_AGE)))


@dataclass
class PurgeRecord:
    proposal_id: str
    symbol: str
    side: str
    reason: str


@dataclass
class PurgeResult:
    purged: list[PurgeRecord] = field(default_factory=list)
    kept: list[str] = field(default_factory=list)
    dry_run: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "dry_run": self.dry_run,
            "purged_count": len(self.purged),
            "kept_count": len(self.kept),
            "purged": [
                {
                    "proposal_id": p.proposal_id,
                    "symbol": p.symbol,
                    "side": p.side,
                    "reason": p.reason,
                }
                for p in self.purged
            ],
            "kept": self.kept,
        }


def _read_active(r: _redis.Redis, symbol: str) -> Optional[dict[str, Any]]:
    import json

    raw = r.get(f"indicators:active:{symbol.upper()}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


def _should_purge(
    r: _redis.Redis,
    proposal: dict[str, Any],
    *,
    now: int,
    reject_expired: bool,
    reject_stale_touch: bool,
    reject_counter_trend: bool,
    max_touch_age: int,
    latest_m1_times: dict[str, int],
) -> Optional[str]:
    sym = str(proposal.get("symbol", "")).upper()
    side = str(proposal.get("side", "")).upper()

    exp = int(proposal.get("expires_at") or 0)
    if reject_expired and exp and now > exp:
        return "expired"

    touch_time = int(proposal.get("touch_time") or proposal.get("bar_time") or 0)
    if reject_stale_touch and touch_time:
        latest = latest_m1_times.get(sym) or _latest_m1_bar_time(r, sym)
        if latest and touch_time < latest - max_touch_age:
            return "touch_stale"

    if reject_counter_trend and sym and side:
        active = _read_active(r, sym)
        m5_dir = m5_st_dir(active)
        if m5_dir in (1, -1) and not side_matches_m5_trend(side, m5_dir):
            return "counter_trend"

    return None


def purge_pending_proposals(
    r: _redis.Redis,
    *,
    symbols: Optional[tuple[str, ...] | list[str]] = None,
    limit: int = 100,
    dry_run: bool = False,
    reject_expired: bool = True,
    reject_stale_touch: bool = True,
    reject_counter_trend: bool = True,
    max_touch_age: Optional[int] = None,
    operator: str = "alpha_purge",
) -> PurgeResult:
    """
    将应清理的 pending 移至 rejected（或 dry_run 仅列出）。

    默认规则：expires_at 已过 / 触线超出时效 / 与当前 M5 ST 逆势。
    """
    age = max_touch_age if max_touch_age is not None else STALE_TOUCH_AGE

    sym_filter = {s.upper() for s in symbols} if symbols else None
    now = int(time.time())
    result = PurgeResult(dry_run=dry_run)

    pending_ids = list_ids(r, "pending", limit=limit)
    sym_for_m1: set[str] = set()
    for pid in pending_ids:
        p0 = get_proposal(r, "pending", pid)
        if not p0:
            continue
        s0 = str(p0.get("symbol", "")).upper()
        if s0 and (not sym_filter or s0 in sym_filter):
            sym_for_m1.add(s0)
    latest_m1_times = {
        sym: t for sym in sym_for_m1 if (t := _latest_m1_bar_time(r, sym))
    }

    for pid in pending_ids:
        p = get_proposal(r, "pending", pid)
        if not p:
            continue
        sym = str(p.get("symbol", "")).upper()
        if sym_filter and sym not in sym_filter:
            continue
        reason = _should_purge(
            r,
            p,
            now=now,
            reject_expired=reject_expired,
            reject_stale_touch=reject_stale_touch,
            reject_counter_trend=reject_counter_trend,
            max_touch_age=age,
            latest_m1_times=latest_m1_times,
        )
        if reason:
            result.purged.append(
                PurgeRecord(pid, sym, str(p.get("side", "")), reason)
            )
            if not dry_run:
                reject_pending(r, p, reason=reason, operator=operator)
        else:
            result.kept.append(pid)

    return result
