"""signal_store SQLite 单元测试。"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import measurement.signal_store as ss


def test_record_and_outcome():
    with tempfile.TemporaryDirectory() as td:
        db = Path(td) / "t.db"
        os.environ["SIGNAL_DB_PATH"] = str(db)
        ss.init_db()

        rid = ss.record_touch({
            "symbol": "NVDA",
            "signal_type": "st_super",
            "side": "LONG",
            "m1_close": 120.5,
            "touch_time": 1_700_000_000,
            "emitted_at": 1_700_000_001,
            "session_date": "2026-07-01",
        })
        assert rid > 0

        rid2 = ss.record_auto({
            "symbol": "NVDA",
            "action": "rejected",
            "mode": "paper",
            "reason": "risk_gate",
            "entry": 121.0,
            "ts": 1_700_000_100,
        })
        assert rid2 > rid

        ss.update_outcome(rid, outcome_7d_pct=2.5, benchmark_7d_pct=1.1)
        summary = ss.stats_summary()
        assert summary["total"] >= 2
        assert summary["labeled_7d"] >= 1

        pending = ss.pending_outcomes(min_age_days=0, limit=10)
        assert any(p["id"] == rid2 for p in pending)

        os.environ.pop("SIGNAL_DB_PATH", None)
        ss.init_db()


if __name__ == "__main__":
    test_record_and_outcome()
    print("test_signal_store: OK")
