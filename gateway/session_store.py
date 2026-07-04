"""飞书用户 / 会话 → Cursor Agent resume chat id。"""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Optional

_LOCK = threading.Lock()
_ROOT = Path(__file__).resolve().parents[1]
_STORE_PATH = Path(
    __import__("os").environ.get(
        "FEISHU_CURSOR_SESSION_FILE",
        str(_ROOT / ".run" / "feishu_cursor_sessions.json"),
    )
)


def _load() -> dict[str, str]:
    if not _STORE_PATH.is_file():
        return {}
    try:
        data = json.loads(_STORE_PATH.read_text(encoding="utf-8"))
        return {str(k): str(v) for k, v in data.items() if k and v}
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return {}


def _save(data: dict[str, str]) -> None:
    _STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _STORE_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def session_key(*, open_id: str, chat_id: str) -> str:
    return f"{chat_id}:{open_id}" if open_id else chat_id


def get_resume_id(key: str) -> Optional[str]:
    with _LOCK:
        return _load().get(key)


def set_resume_id(key: str, chat_id: str) -> None:
    if not key or not chat_id:
        return
    with _LOCK:
        data = _load()
        data[key] = chat_id
        _save(data)
