#!/usr/bin/env python3
"""审批交易建议（调 frontend API）。"""
from __future__ import annotations

import argparse
import json
import os
import urllib.request


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True)
    parser.add_argument("--decision", required=True,
                        choices=["approved_live", "approved_observe", "rejected"])
    parser.add_argument("--approver", default="cli")
    parser.add_argument("--comment", default="")
    args = parser.parse_args()

    base = os.environ.get("NAUTILUS_API_BASE", "http://localhost:3000").rstrip("/")
    body = json.dumps({
        "decision": args.decision,
        "approver": args.approver,
        "comment": args.comment,
    }).encode("utf-8")
    req = urllib.request.Request(
        f"{base}/api/proposals/{args.id}/decision",
        data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    token = os.environ.get("NAUTILUS_API_SECRET", "").strip()
    if token:
        req.add_header("X-Nautilus-Token", token)
    with urllib.request.urlopen(req, timeout=10) as resp:
        print(resp.read().decode())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
