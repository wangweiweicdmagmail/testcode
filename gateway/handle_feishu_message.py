#!/usr/bin/env python3
"""由 server.js 异步拉起：读取飞书 webhook JSON，调用 Cursor Agent 并回复。"""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from gateway.feishu_agent import process_async_job  # noqa: E402


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: handle_feishu_message.py <json-file>", file=sys.stderr)
        return 2
    path = Path(sys.argv[1])
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"read json failed: {e}", file=sys.stderr)
        return 1

    event = body.get("event") or {}
    job = {"kind": "im_message", "event": event}
    try:
        result = process_async_job(job)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result.get("ok", True) else 1
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        return 1
    finally:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
