"""飞书 Open API：tenant token + 发交互卡片。"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Optional

from feishu import config

_TOKEN: Optional[str] = None
_TOKEN_EXPIRES_AT = 0.0


def _request(
    method: str,
    url: str,
    *,
    headers: Optional[dict[str, str]] = None,
    body: Optional[dict[str, Any]] = None,
    timeout: int = 15,
) -> dict[str, Any]:
    data = None
    hdrs = {"Content-Type": "application/json; charset=utf-8"}
    if headers:
        hdrs.update(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Feishu HTTP {e.code}: {raw}") from e


def tenant_access_token() -> str:
    global _TOKEN, _TOKEN_EXPIRES_AT
    now = time.time()
    if _TOKEN and now < _TOKEN_EXPIRES_AT - 60:
        return _TOKEN
    if not config.APP_ID or not config.APP_SECRET:
        raise RuntimeError("FEISHU_APP_ID / FEISHU_APP_SECRET 未配置")
    data = _request(
        "POST",
        "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
        body={"app_id": config.APP_ID, "app_secret": config.APP_SECRET},
    )
    if data.get("code") != 0:
        raise RuntimeError(f"获取 tenant_access_token 失败: {data}")
    _TOKEN = str(data["tenant_access_token"])
    _TOKEN_EXPIRES_AT = now + int(data.get("expire", 7200))
    return _TOKEN


def send_text_message(
    text: str,
    *,
    receive_id: Optional[str] = None,
    receive_id_type: Optional[str] = None,
) -> dict[str, Any]:
    rid = receive_id or config.RECEIVE_ID
    if not rid:
        raise RuntimeError("FEISHU_RECEIVE_ID 未配置")
    token = tenant_access_token()
    q = urllib.parse.urlencode(
        {"receive_id_type": receive_id_type or config.RECEIVE_ID_TYPE}
    )
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?{q}"
    body = {
        "receive_id": rid,
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    data = _request(
        "POST",
        url,
        headers={"Authorization": f"Bearer {token}"},
        body=body,
    )
    if data.get("code") != 0:
        raise RuntimeError(f"发送飞书文本失败: {data}")
    return data


def reply_text_message(message_id: str, text: str) -> dict[str, Any]:
    if not message_id:
        raise RuntimeError("message_id 不能为空")
    token = tenant_access_token()
    url = (
        "https://open.feishu.cn/open-apis/im/v1/messages/"
        f"{urllib.parse.quote(message_id, safe='')}/reply"
    )
    body = {
        "msg_type": "text",
        "content": json.dumps({"text": text}, ensure_ascii=False),
    }
    data = _request(
        "POST",
        url,
        headers={"Authorization": f"Bearer {token}"},
        body=body,
    )
    if data.get("code") != 0:
        raise RuntimeError(f"回复飞书消息失败: {data}")
    return data


def send_interactive_card(card: dict[str, Any], *, receive_id: Optional[str] = None) -> dict[str, Any]:
    rid = receive_id or config.RECEIVE_ID
    if not rid:
        raise RuntimeError("FEISHU_RECEIVE_ID 未配置")
    token = tenant_access_token()
    q = urllib.parse.urlencode({"receive_id_type": config.RECEIVE_ID_TYPE})
    url = f"https://open.feishu.cn/open-apis/im/v1/messages?{q}"
    body = {
        "receive_id": rid,
        "msg_type": "interactive",
        "content": json.dumps(card, ensure_ascii=False),
    }
    data = _request(
        "POST",
        url,
        headers={"Authorization": f"Bearer {token}"},
        body=body,
    )
    if data.get("code") != 0:
        raise RuntimeError(f"发送飞书卡片失败: {data}")
    return data
