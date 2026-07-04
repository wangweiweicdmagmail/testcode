"""从项目 .env 加载环境变量（不覆盖已有）。"""
from __future__ import annotations

import os
from pathlib import Path


def load_dotenv(path: Path | None = None) -> Path:
    root = Path(__file__).resolve().parents[1]
    env_path = path or (root / ".env")
    if not env_path.is_file():
        return env_path
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key, val = key.strip(), val.strip().strip("'\"")
        if key and key not in os.environ:
            os.environ[key] = val
    return env_path
