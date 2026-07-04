#!/usr/bin/env python3
"""列出 Redis 中的交易建议。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import redis  # noqa: E402
from approval.proposal_store import list_proposals  # noqa: E402
import alpha_agent as aa  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--status", default="pending",
                        choices=["pending", "approved", "rejected", "executed"])
    parser.add_argument("--symbol", default="")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    r = redis.Redis(
        host=aa.REDIS_HOST, port=aa.REDIS_PORT, db=aa.REDIS_DB,
        decode_responses=True, socket_timeout=3,
    )
    sym = args.symbol.strip().upper() or None
    rows = list_proposals(r, args.status, symbol=sym, limit=args.limit)
    print(json.dumps({"status": args.status, "count": len(rows), "proposals": rows},
                     ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
