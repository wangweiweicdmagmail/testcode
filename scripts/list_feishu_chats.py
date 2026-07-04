#!/usr/bin/env python3
"""列出机器人可见的飞书群 chat_id（配置 FEISHU_RECEIVE_ID 用）。"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from feishu.client import tenant_access_token  # noqa: E402


def main() -> int:
    token = tenant_access_token()
    url = "https://open.feishu.cn/open-apis/im/v1/chats?page_size=20"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    if data.get("code") != 0:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return 1
    items = data.get("data", {}).get("items", [])
    out = []
    for c in items:
        out.append({
            "chat_id": c.get("chat_id"),
            "name": c.get("name"),
            "description": c.get("description"),
        })
    print(json.dumps({"ok": True, "chats": out}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
        raise SystemExit(1)
