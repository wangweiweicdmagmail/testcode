#!/usr/bin/env python3
"""手动将指定 pending 建议推送到飞书。"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import redis  # noqa: E402

from feishu.cards import build_proposal_card  # noqa: E402
from feishu.client import send_interactive_card  # noqa: E402
from feishu import config  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Push proposal card to Feishu")
    parser.add_argument("--id", required=True, help="proposal_id")
    args = parser.parse_args()

    if not config.enabled():
        print(json.dumps({"ok": False, "error": "飞书未配置"}, ensure_ascii=False))
        return 1

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    raw = r.hgetall(f"proposal:pending:{args.id}")
    if not raw:
        print(json.dumps({"ok": False, "error": "pending 建议不存在"}, ensure_ascii=False))
        return 1

    proposal = {}
    for k, v in raw.items():
        try:
            proposal[k] = json.loads(v)
        except json.JSONDecodeError:
            proposal[k] = v

    card = build_proposal_card(proposal)
    result = send_interactive_card(card)
    print(json.dumps({"ok": True, "feishu": result}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
