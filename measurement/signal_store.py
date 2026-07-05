"""
信号日志 — SQLite 持久化 + outcome / SPY benchmark 标签。

范围见 docs/SCOPE_FIXED.md §1.2、§2、§3。
- touch → record_touch；auto → record_auto（outcome 回填仅 touch）
- Redis 仍负责实时；SQLite 负责跨日测量
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

_DEFAULT_DB = Path(__file__).resolve().parents[1] / ".run" / "signals.db"


def db_path() -> Path:
    raw = os.environ.get("SIGNAL_DB_PATH", "").strip()
    return Path(raw) if raw else _DEFAULT_DB


def _connect() -> sqlite3.Connection:
    path = db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS signal_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                signal_type TEXT,
                side TEXT,
                source TEXT NOT NULL,
                action TEXT,
                signal_price REAL,
                touch_time INTEGER,
                session_date TEXT,
                features TEXT,
                proposal_id TEXT,
                created_at INTEGER NOT NULL,
                outcome_1d_pct REAL,
                outcome_7d_pct REAL,
                benchmark_7d_pct REAL,
                outcome_checked_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_signal_logs_created ON signal_logs(created_at);
            CREATE INDEX IF NOT EXISTS idx_signal_logs_symbol ON signal_logs(symbol);
            CREATE INDEX IF NOT EXISTS idx_signal_logs_outcome ON signal_logs(outcome_7d_pct);
        """)


def record_touch(payload: dict[str, Any]) -> int:
    """signal:touch 落库。"""
    init_db()
    ts = int(payload.get("emitted_at") or payload.get("touch_time") or time.time())
    price = payload.get("m1_close") or payload.get("trigger_level")
    features = {k: payload.get(k) for k in (
        "trigger_level", "reclaim", "rule_confidence", "rule_thesis", "m1_high", "m1_low",
    ) if payload.get(k) is not None}
    with _connect() as conn:
        cur = conn.execute(
            """INSERT INTO signal_logs
               (symbol, signal_type, side, source, action, signal_price, touch_time,
                session_date, features, created_at)
               VALUES (?, ?, ?, 'touch', 'touch', ?, ?, ?, ?, ?)""",
            (
                payload.get("symbol", "").upper(),
                payload.get("signal_type"),
                payload.get("side"),
                float(price) if price is not None else None,
                int(payload.get("touch_time") or ts),
                payload.get("session_date"),
                json.dumps(features, ensure_ascii=False) if features else None,
                ts,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def record_auto(payload: dict[str, Any]) -> int:
    """auto:signal 落库。"""
    init_db()
    ts = int(payload.get("ts") or time.time())
    price = payload.get("entry")
    features = {k: payload.get(k) for k in ("reason", "mode", "qty", "stop", "tp", "seq")
                 if payload.get(k) is not None}
    with _connect() as conn:
        cur = conn.execute(
            """INSERT INTO signal_logs
               (symbol, signal_type, side, source, action, signal_price, touch_time,
                session_date, features, proposal_id, created_at)
               VALUES (?, ?, ?, 'auto', ?, ?, NULL, NULL, ?, ?, ?)""",
            (
                (payload.get("symbol") or "").upper(),
                payload.get("signal_type"),
                payload.get("side"),
                payload.get("action"),
                float(price) if price is not None else None,
                json.dumps(features, ensure_ascii=False) if features else None,
                payload.get("proposal_id"),
                ts,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def pending_outcomes(*, min_age_days: int = 7, limit: int = 500) -> list[dict[str, Any]]:
    init_db()
    # 用 created_at（真实 wall clock）判断信号「够不够老」
    cutoff = int(time.time()) - min_age_days * 86400
    outcome_col = "outcome_7d_pct" if min_age_days >= 7 else "outcome_1d_pct"
    with _connect() as conn:
        rows = conn.execute(
            f"""SELECT * FROM signal_logs
               WHERE {outcome_col} IS NULL AND signal_price IS NOT NULL
               AND created_at <= ?
               ORDER BY created_at ASC LIMIT ?""",
            (cutoff, limit),
        ).fetchall()
    return [dict(r) for r in rows]


def update_outcome(
    row_id: int,
    *,
    outcome_7d_pct: Optional[float] = None,
    benchmark_7d_pct: Optional[float] = None,
    outcome_1d_pct: Optional[float] = None,
) -> None:
    init_db()
    now = int(time.time())
    with _connect() as conn:
        conn.execute(
            """UPDATE signal_logs SET
               outcome_7d_pct = COALESCE(?, outcome_7d_pct),
               benchmark_7d_pct = COALESCE(?, benchmark_7d_pct),
               outcome_1d_pct = COALESCE(?, outcome_1d_pct),
               outcome_checked_at = ?
               WHERE id = ?""",
            (outcome_7d_pct, benchmark_7d_pct, outcome_1d_pct, now, row_id),
        )
        conn.commit()


def stats_summary() -> dict[str, Any]:
    init_db()
    with _connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM signal_logs").fetchone()[0]
        labeled = conn.execute(
            "SELECT COUNT(*) FROM signal_logs WHERE outcome_7d_pct IS NOT NULL"
        ).fetchone()[0]
        avg_alpha = conn.execute(
            """SELECT AVG(outcome_7d_pct - COALESCE(benchmark_7d_pct, 0))
               FROM signal_logs WHERE outcome_7d_pct IS NOT NULL"""
        ).fetchone()[0]
    return {
        "total": total,
        "labeled_7d": labeled,
        "avg_alpha_vs_spy_7d": round(float(avg_alpha), 4) if avg_alpha is not None else None,
        "db_path": str(db_path()),
    }


init_db()
