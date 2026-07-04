#!/usr/bin/env python3
"""手动扫一轮 Alpha（增量过滤，与 MCP run_alpha_scan 同逻辑）。"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import redis  # noqa: E402

import alpha_agent as aa  # noqa: E402
from approval.alpha_scan import run_incremental_scan  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental alpha scan → pending proposals")
    parser.add_argument("--symbols", default="", help="NVDA,TSLA override ALPHA_SYMBOLS")
    parser.add_argument(
        "--full",
        action="store_true",
        help="回放 24h 内触线（调试用，非 cron）",
    )
    args = parser.parse_args()

    if args.symbols.strip():
        os.environ["ALPHA_SYMBOLS"] = args.symbols.strip()

    r = redis.Redis(
        host=aa.REDIS_HOST, port=aa.REDIS_PORT, db=aa.REDIS_DB,
        decode_responses=True, socket_timeout=3,
    )
    r.ping()

    result = run_incremental_scan(
        r, aa.ALPHA_SYMBOLS, incremental=not args.full,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        raise SystemExit(1)
