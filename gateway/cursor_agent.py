"""调用 Cursor Agent CLI（cursor-agent）并返回文本回复。"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

_ROOT = Path(__file__).resolve().parents[1]


def _load_dotenv() -> None:
    env_path = _ROOT / ".env"
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


def find_cursor_agent() -> str:
    explicit = os.environ.get("CURSOR_AGENT_PATH", "").strip()
    if explicit:
        return explicit
    for candidate in (
        Path.home() / ".local/bin/cursor-agent",
        Path("/Applications/Cursor.app/Contents/Resources/app/bin/cursor-agent"),
    ):
        if candidate.is_file():
            return str(candidate)
    for name in ("cursor-agent", "agent"):
        found = shutil.which(name)
        if found:
            return found
    raise RuntimeError(
        "未找到 cursor-agent。请安装 Cursor CLI："
        "curl https://cursor.com/install -fsS | bash，"
        "或设置 CURSOR_AGENT_PATH"
    )


def _project_cwd() -> Path:
    raw = os.environ.get("CURSOR_AGENT_CWD") or os.environ.get("NAUTILUS_ROOT", "")
    if raw.strip():
        return Path(raw).expanduser().resolve()
    return _ROOT


def _timeout_sec() -> int:
    try:
        return max(30, int(os.environ.get("CURSOR_AGENT_TIMEOUT_SEC", "300")))
    except ValueError:
        return 300


def _extract_text_from_stream_json(stdout: str) -> str:
    parts: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(obj, dict):
            continue
        typ = obj.get("type")
        if typ == "text":
            delta = obj.get("text") or obj.get("content") or ""
            if isinstance(delta, str):
                parts.append(delta)
        elif typ == "assistant" and isinstance(obj.get("message"), dict):
            content = obj["message"].get("content")
            if isinstance(content, str):
                parts.append(content)
        elif typ == "result" and isinstance(obj.get("result"), str):
            parts.append(obj["result"])
    return "".join(parts).strip()


def _parse_stdout(stdout: str, output_format: str) -> str:
    text = stdout.strip()
    if not text:
        return ""
    if output_format == "stream-json":
        parsed = _extract_text_from_stream_json(text)
        if parsed:
            return parsed
    if output_format == "json":
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                for key in ("result", "text", "content", "message"):
                    val = obj.get(key)
                    if isinstance(val, str) and val.strip():
                        return val.strip()
        except json.JSONDecodeError:
            pass
    return text


def _extract_resume_id(stdout: str, output_format: str) -> Optional[str]:
    if output_format not in ("json", "stream-json"):
        return None
    blob = stdout
    if output_format == "stream-json":
        blob = "\n".join(stdout.splitlines())
    for pattern in (
        r'"chatId"\s*:\s*"([^"]+)"',
        r'"chat_id"\s*:\s*"([^"]+)"',
        r'"sessionId"\s*:\s*"([^"]+)"',
    ):
        m = re.search(pattern, blob)
        if m:
            return m.group(1)
    return None


def run_agent(
    prompt: str,
    *,
    resume_id: Optional[str] = None,
) -> dict[str, Any]:
    """执行一次 Cursor Agent 对话，返回 {text, resume_id, cmd}."""
    prompt = (prompt or "").strip()
    if not prompt:
        raise ValueError("prompt 不能为空")

    binary = find_cursor_agent()
    output_format = os.environ.get("CURSOR_AGENT_OUTPUT_FORMAT", "text").strip() or "text"
    mode = os.environ.get("CURSOR_AGENT_MODE", "ask").strip()
    model = os.environ.get("CURSOR_AGENT_MODEL", "").strip()
    force = os.environ.get("CURSOR_AGENT_FORCE", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )

    cmd: list[str] = [
        binary,
        "-p",
        prompt,
        "--print",
        "--output-format",
        output_format,
    ]
    if mode:
        cmd.extend(["--mode", mode])
    if model:
        cmd.extend(["--model", model])
    if resume_id:
        cmd.extend(["--resume", resume_id])
    if force:
        cmd.append("--force")

    env = os.environ.copy()
    if not env.get("PATH", "").startswith(str(Path.home() / ".local/bin")):
        env["PATH"] = f"{Path.home() / '.local/bin'}:{env.get('PATH', '')}"

    proc = subprocess.run(
        cmd,
        cwd=str(_project_cwd()),
        env=env,
        capture_output=True,
        text=True,
        timeout=_timeout_sec(),
    )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    if proc.returncode != 0:
        detail = stderr or stdout or f"exit {proc.returncode}"
        if "Authentication required" in detail or "agent login" in detail:
            raise RuntimeError(
                "Cursor Agent 未登录。请在本机执行 `cursor-agent login`，"
                "或在 .env 设置 CURSOR_API_KEY"
            )
        raise RuntimeError(detail)

    text = _parse_stdout(stdout, output_format)
    if not text:
        text = stderr or "（Agent 无文本输出）"

    new_resume = _extract_resume_id(stdout, output_format) or resume_id
    return {
        "text": text,
        "resume_id": new_resume,
        "cmd": cmd,
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Run Cursor Agent once")
    parser.add_argument("prompt", help="User prompt")
    parser.add_argument("--resume", default="", help="Cursor chat id to resume")
    args = parser.parse_args()
    try:
        result = run_agent(args.prompt, resume_id=args.resume or None)
        print(result["text"])
        if result.get("resume_id"):
            print(f"\n[resume_id={result['resume_id']}]", file=sys.stderr)
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
