#!/usr/bin/env python3
"""
订阅 Redis proposal:update，新 pending 建议推送飞书交互卡片。

用法:
  python feishu/notifier.py
"""
from __future__ import annotations

import json
import sys
import time

import redis

from feishu import config
from feishu.cards import build_proposal_card
from feishu.client import send_interactive_card

REDIS_HOST = __import__("os").environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(__import__("os").environ.get("REDIS_PORT", 6379))
REDIS_DB = int(__import__("os").environ.get("REDIS_DB", 0))
CHANNEL = "proposal:update"


def _should_notify(payload: dict) -> bool:
    ev = payload.get("event")
    if ev and ev not in ("created",):
        return False
    return payload.get("status") == "pending" and bool(payload.get("proposal_id"))


def push_proposal(proposal: dict) -> None:
    card = build_proposal_card(proposal)
    send_interactive_card(card)
    print(
        f"[FeishuNotifier] sent card {proposal.get('symbol')} "
        f"id={proposal.get('proposal_id')}",
        flush=True,
    )


def run() -> None:
    if not config.enabled():
        print(
            "[FeishuNotifier] 未配置 FEISHU_APP_ID/SECRET/RECEIVE_ID，退出",
            flush=True,
        )
        raise SystemExit(1)

    r = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
        decode_responses=True, socket_timeout=5,
    )
    r.ping()
    pubsub = r.pubsub()
    pubsub.subscribe(CHANNEL)
    print(f"[FeishuNotifier] listening {CHANNEL}", flush=True)

    for msg in pubsub.listen():
        if msg.get("type") != "message":
            continue
        try:
            payload = json.loads(msg["data"])
        except (json.JSONDecodeError, TypeError):
            continue
        if not _should_notify(payload):
            continue
        try:
            push_proposal(payload)
        except Exception as e:
            print(f"[FeishuNotifier] push error: {e}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        sys.exit(0)
