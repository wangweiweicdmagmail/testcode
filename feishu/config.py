"""从环境变量 / 项目 .env 读取飞书配置。"""
from __future__ import annotations

import os
from pathlib import Path


def _load_dotenv() -> None:
    root = Path(__file__).resolve().parents[1]
    env_path = root / ".env"
    if not env_path.is_file():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip("'\"")
            if key and key not in os.environ:
                os.environ[key] = val
    except OSError:
        pass


_load_dotenv()

APP_ID = os.environ.get("FEISHU_APP_ID", "").strip()
APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "").strip()
RECEIVE_ID = os.environ.get("FEISHU_RECEIVE_ID", "").strip()
RECEIVE_ID_TYPE = os.environ.get("FEISHU_RECEIVE_ID_TYPE", "chat_id").strip()
VERIFICATION_TOKEN = os.environ.get("FEISHU_VERIFICATION_TOKEN", "").strip()
ENCRYPT_KEY = os.environ.get("FEISHU_ENCRYPT_KEY", "").strip()
API_BASE = os.environ.get("NAUTILUS_API_BASE", "http://localhost:3000").rstrip("/")


def enabled() -> bool:
    return bool(APP_ID and APP_SECRET and RECEIVE_ID)
