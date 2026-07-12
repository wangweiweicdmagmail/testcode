"""从 Redis M5 K 线重放 SuperTrend，与 bars / indicators:active 对账。"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import redis as _redis

from nautilus_mcp.redis_io import get_bars, get_indicators_active

from indicators.supertrend import STState

ST_PERIOD = 10
ST_MULT_M5 = 3.0


# SuperTrend 实现已合并至 indicators.supertrend.STState（设计原则 #2）。
# 历史的 _WilderATR（手写 Wilder 平滑）与 STReplay 删除，统一用引擎
# AverageTrueRange(WILDER)，消除原则 #2 的明确违背项。


@dataclass
class STMismatch:
    time: int
    close: float
    redis_dir: int
    replay_dir: int
    redis_st: float
    replay_st: float
    prev_redis_st: Optional[float]
    note: str = ""


@dataclass
class STAuditResult:
    symbol: str
    m5_bar_count: int
    dir_mismatches: list[STMismatch] = field(default_factory=list)
    active: Optional[dict[str, Any]] = None
    last_bar: Optional[dict[str, Any]] = None
    replay_last: Optional[dict[str, Any]] = None
    active_matches_replay: Optional[bool] = None
    warmup_warning: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "symbol": self.symbol,
            "m5_bar_count": self.m5_bar_count,
            "warmup_warning": self.warmup_warning,
            "active": self.active,
            "last_bar": {
                "time": self.last_bar.get("time"),
                "close": self.last_bar.get("close"),
                "st_value": self.last_bar.get("st_value"),
                "st_dir": self.last_bar.get("st_dir"),
            } if self.last_bar else None,
            "replay_last": self.replay_last,
            "active_matches_replay": self.active_matches_replay,
            "dir_mismatch_count": len(self.dir_mismatches),
            "dir_mismatches": [
                {
                    "time": m.time,
                    "close": m.close,
                    "redis_dir": m.redis_dir,
                    "replay_dir": m.replay_dir,
                    "redis_st": m.redis_st,
                    "replay_st": m.replay_st,
                    "prev_redis_st": m.prev_redis_st,
                    "note": m.note,
                }
                for m in self.dir_mismatches
            ],
        }


def audit_m5_st(
    r: _redis.Redis,
    symbol: str,
    *,
    limit: int = 120,
) -> STAuditResult:
    sym = symbol.upper()
    bars = get_bars(r, "5m", sym, limit=limit)
    active = get_indicators_active(r, sym)
    st = STState(ST_PERIOD, ST_MULT_M5)
    mismatches: list[STMismatch] = []
    replay_last: Optional[dict[str, Any]] = None

    for i, b in enumerate(bars):
        sv, sd, _, _ = st.update(
            float(b["open"]), float(b["high"]), float(b["low"]), float(b["close"]),
        )
        replay_last = {"time": b["time"], "st_value": sv, "st_dir": sd, "close": b["close"]}
        rd = b.get("st_dir")
        rv = b.get("st_value")
        if rd is None:
            continue
        if int(rd) != int(sd):
            prev = bars[i - 1] if i else None
            note = ""
            if prev and int(prev.get("st_dir") or 0) == 1 and int(rd) == -1:
                prev_st = float(prev.get("st_value") or 0)
                if float(b["close"]) >= prev_st:
                    note = "疑似旧版 ST bug：收盘未跌破上一根 ST 却翻空"
            mismatches.append(
                STMismatch(
                    time=int(b["time"]),
                    close=float(b["close"]),
                    redis_dir=int(rd),
                    replay_dir=int(sd),
                    redis_st=float(rv or 0),
                    replay_st=float(sv),
                    prev_redis_st=float(prev["st_value"]) if prev else None,
                    note=note,
                )
            )

    warmup = ""
    if len(bars) < 30:
        warmup = (
            f"Redis 仅 {len(bars)} 根 M5，重放 ATR 预热不足；"
            "方向不一致更可能是引擎未重启或 bars 为旧代码写入。"
        )

    active_ok: Optional[bool] = None
    if active and replay_last:
        ast = active.get("supertrend") or {}
        active_ok = (
            int(ast.get("dir") or 0) == int(replay_last["st_dir"])
            and abs(float(ast.get("value") or 0) - float(replay_last["st_value"])) < 0.05
        )

    return STAuditResult(
        symbol=sym,
        m5_bar_count=len(bars),
        dir_mismatches=mismatches,
        active=active,
        last_bar=bars[-1] if bars else None,
        replay_last=replay_last,
        active_matches_replay=active_ok,
        warmup_warning=warmup,
    )
