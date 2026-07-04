"""解析飞书 IM 事件，调用 Cursor Agent，回发文本。"""
from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from feishu import config  # noqa: E402
from feishu.client import reply_text_message, send_text_message  # noqa: E402
from gateway.cursor_agent import run_agent  # noqa: E402
from gateway.session_store import get_resume_id, session_key, set_resume_id  # noqa: E402

_MAX_REPLY_CHARS = 3500
_BOT_OPEN_ID = os.environ.get("FEISHU_BOT_OPEN_ID", "").strip()


def _enabled() -> bool:
    flag = os.environ.get("FEISHU_AGENT_ENABLED", "true").strip().lower()
    if flag in ("0", "false", "no", "off"):
        return False
    return bool(config.APP_ID and config.APP_SECRET)


def _parse_message_text(content: str, message_type: str) -> str:
    if message_type != "text":
        return ""
    try:
        obj = json.loads(content or "{}")
    except json.JSONDecodeError:
        return (content or "").strip()
    text = obj.get("text", "")
    if not isinstance(text, str):
        return ""
    # 去掉 @机器人 前缀
    text = re.sub(r"@_user_\d+\s*", "", text)
    text = re.sub(r"@\S+\s*", "", text, count=1)
    return text.strip()


def _truncate(text: str) -> str:
    text = (text or "").strip()
    if len(text) <= _MAX_REPLY_CHARS:
        return text
    return text[: _MAX_REPLY_CHARS - 20] + "\n\n…（已截断）"


def _system_prefix() -> str:
    return (
        os.environ.get(
            "CURSOR_AGENT_SYSTEM_PREFIX",
            "你是用户的策略交易助手。用简洁中文回答。",
        ).strip()
        or ""
    )


def _build_prompt(user_text: str, *, chat_type: str) -> str:
    prefix = _system_prefix()
    scope = "群聊" if chat_type == "group" else "私聊"
    body = f"[飞书{scope}]\n{user_text}"
    return f"{prefix}\n\n{body}" if prefix else body


def handle_im_message_event(event: dict[str, Any]) -> dict[str, Any]:
    """处理 im.message.receive_v1，返回处理结果摘要。"""
    if not _enabled():
        return {"ok": False, "skipped": "feishu agent disabled or not configured"}

    message = event.get("message") or {}
    sender = event.get("sender") or {}
    sender_id = sender.get("sender_id") or {}

    message_id = message.get("message_id") or ""
    chat_id = message.get("chat_id") or ""
    chat_type = message.get("chat_type") or "p2p"
    message_type = message.get("message_type") or "text"
    content = message.get("content") or ""

    open_id = sender_id.get("open_id") or ""
    if _BOT_OPEN_ID and open_id and open_id == _BOT_OPEN_ID:
        return {"ok": True, "skipped": "bot message"}

    user_text = _parse_message_text(content, message_type)
    if not user_text:
        if message_id:
            reply_text_message(
                message_id,
                "暂仅支持文字消息；请在群里 @机器人 后发送文本。",
            )
        return {"ok": True, "skipped": "non-text or empty"}

    allowed = os.environ.get("FEISHU_AGENT_ALLOWED_OPEN_IDS", "").strip()
    if allowed:
        allowed_set = {x.strip() for x in allowed.split(",") if x.strip()}
        if open_id and open_id not in allowed_set:
            if message_id:
                reply_text_message(message_id, "未授权使用该机器人。")
            return {"ok": False, "error": "sender not allowed"}

    key = session_key(open_id=open_id, chat_id=chat_id)
    resume_id = get_resume_id(key)
    prompt = _build_prompt(user_text, chat_type=chat_type)

    try:
        result = run_agent(prompt, resume_id=resume_id)
    except Exception as e:
        err = str(e).strip() or "Cursor Agent 执行失败"
        if message_id:
            reply_text_message(message_id, f"⚠️ Agent 错误：{err}")
        elif chat_id:
            send_text_message(_truncate(f"⚠️ Agent 错误：{err}"), receive_id=chat_id)
        return {"ok": False, "error": err}

    reply = _truncate(result.get("text") or "")
    new_resume = result.get("resume_id")
    if isinstance(new_resume, str) and new_resume.strip():
        set_resume_id(key, new_resume.strip())

    if message_id:
        reply_text_message(message_id, reply)
    elif chat_id:
        send_text_message(reply, receive_id=chat_id)
    else:
        return {"ok": False, "error": "missing message_id and chat_id"}

    return {
        "ok": True,
        "chat_id": chat_id,
        "open_id": open_id,
        "resume_id": new_resume,
        "reply_chars": len(reply),
    }


def handle_webhook_body(body: dict[str, Any]) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    """
  返回 (immediate_response, async_job)。
  immediate_response 非空时直接 HTTP 响应；async_job 非空时后台处理。
  """
    if body.get("challenge"):
        return {"challenge": body["challenge"]}, None

    event_type = (body.get("header") or {}).get("event_type") or body.get("type")
    if event_type == "url_verification":
        challenge = (body.get("event") or {}).get("challenge") or body.get("challenge")
        return {"challenge": challenge}, None

    if event_type == "im.message.receive_v1":
        event = body.get("event") or {}
        return {}, {"kind": "im_message", "event": event}

    return None, None


def process_async_job(job: dict[str, Any]) -> dict[str, Any]:
    if job.get("kind") == "im_message":
        return handle_im_message_event(job.get("event") or {})
    return {"ok": False, "error": f"unknown job {job.get('kind')}"}
