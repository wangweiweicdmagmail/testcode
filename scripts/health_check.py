#!/usr/bin/env python3
"""Redis + 前端 API 健康检查。"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import redis  # noqa: E402
import alpha_agent as aa  # noqa: E402

API = os.environ.get("NAUTILUS_API_BASE", "http://localhost:3000").rstrip("/")


def main() -> int:
    result: dict = {"ok": True, "checks": {}}
    try:
        r = redis.Redis(host=aa.REDIS_HOST, port=aa.REDIS_PORT, db=aa.REDIS_DB,
                        decode_responses=True, socket_timeout=3)
        r.ping()
        result["checks"]["redis"] = {"ok": True, "pending": r.zcard(aa.PENDING_INDEX_KEY)}
    except Exception as e:
        result["ok"] = False
        result["checks"]["redis"] = {"ok": False, "error": str(e)}

    try:
        with urllib.request.urlopen(f"{API}/api/stack-health", timeout=5) as resp:
            result["checks"]["frontend"] = {"ok": True, **json.loads(resp.read().decode())}
    except Exception as e:
        result["ok"] = False
        result["checks"]["frontend"] = {"ok": False, "error": str(e)}

    result["checked_at"] = int(time.time())
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
