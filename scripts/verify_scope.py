#!/usr/bin/env python3
"""
SCOPE_FIXED 验收 — 对照 docs/SCOPE_FIXED.md 检查文件 / 模块 / 路由声明。

不启动 IBKR / Nautilus / 前端；仅静态验收。CI 与本地发布前运行：

  python scripts/verify_scope.py
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = [
    "docs/SCOPE_FIXED.md",
    "measurement/signal_store.py",
    "scripts/check_signal_outcomes.py",
    "portfolio/auto_settings.py",
    "portfolio/order_policy.py",
    "execution/auto_pm.py",
    "auto_runner.py",
    "frontend/journal.js",
    "frontend/public/audit.html",
    "frontend/public/settings.html",
    "frontend/public/docs.html",
    ".github/workflows/ci.yml",
    "pytest.ini",
]

REQUIRED_SERVER_ROUTES = [
    'app.get("/api/journal/day"',
    'app.get("/api/journal/timeline"',
    'app.get("/api/config/settings"',
    "journal.recordSignalTouch",
    "journal.recordAutoSignal",
    "journal.recordOrderUpdate",
    "journal.recordProposalUpdate",
    "journal.recordPositionUpdate",
]

REQUIRED_PY_SYMBOLS = {
    "measurement/signal_store.py": ["record_touch", "record_auto", "pending_outcomes", "update_outcome"],
    "portfolio/auto_settings.py": ["uses_auto_pm", "is_auto_managed"],
    "portfolio/order_policy.py": ["decide_entry_order", "decide_close_order", "data_state_from_env", "is_delayed_market_data"],
    "portfolio/ib_orders.py": ["build_marketable_order"],
    "execution/auto_pm.py": ["reconcile_startup", "_journal_map_coid", "_finalize_symbol_close"],
}

REQUIRED_HOOKS = [
    ("signal_detector.py", "record_touch"),
    ("auto_runner.py", "reconcile_startup"),
    ("auto_runner.py", "_drain_close_request"),
    ("auto_runner.py", "record_auto"),
    ("execution/auto_pm.py", "build_marketable_order"),
    ("execution/auto_pm.py", "ocaType"),
    ("order_actor.py", "build_marketable_order"),
    ("frontend/server.js", "usesAutoPm"),
    ("frontend/server.js", "routeAutoPmClose"),
    ("frontend/server.js", "auto:close:"),
]


def _fail(msg: str, errors: list[str]) -> None:
    errors.append(msg)


def check_scope_doc(errors: list[str]) -> None:
    text = (ROOT / "docs/SCOPE_FIXED.md").read_text(encoding="utf-8")
    for needle in (
        "上层 → 引擎参数边界",
        "auto:close:{sym}",
        "POST /api/position",
    ):
        if needle not in text:
            _fail(f"SCOPE_FIXED.md 缺少: {needle}", errors)


def check_files(errors: list[str]) -> None:
    for rel in REQUIRED_FILES:
        if not (ROOT / rel).is_file():
            _fail(f"缺少文件: {rel}", errors)


def check_server_js(errors: list[str]) -> None:
    text = (ROOT / "frontend/server.js").read_text(encoding="utf-8")
    for needle in REQUIRED_SERVER_ROUTES:
        if needle not in text:
            _fail(f"server.js 缺少: {needle}", errors)


def check_py_symbols(errors: list[str]) -> None:
    for rel, names in REQUIRED_PY_SYMBOLS.items():
        path = ROOT / rel
        tree = ast.parse(path.read_text(encoding="utf-8"))
        defined = {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
        for name in names:
            if name not in defined:
                _fail(f"{rel} 缺少函数: {name}", errors)


def check_hooks(errors: list[str]) -> None:
    for rel, needle in REQUIRED_HOOKS:
        text = (ROOT / rel).read_text(encoding="utf-8")
        if needle not in text:
            _fail(f"{rel} 未接入: {needle}", errors)


def check_status_nav(errors: list[str]) -> None:
    text = (ROOT / "frontend/public/shared/status-bar.js").read_text(encoding="utf-8")
    for page in ("/audit.html", "/settings.html", "/docs.html"):
        if page not in text:
            _fail(f"status-bar 缺少导航: {page}", errors)


def check_env_example(errors: list[str]) -> None:
    text = (ROOT / "env.example").read_text(encoding="utf-8")
    for key in ("TRADING_ENV", "MARKET_DATA_DELAYED", "SIGNAL_DB_PATH"):
        if key not in text:
            _fail(f"env.example 缺少: {key}", errors)


def check_gitignore_run(errors: list[str]) -> None:
    gi = (ROOT / ".gitignore").read_text(encoding="utf-8")
    if ".run/" not in gi and ".run" not in gi:
        _fail(".gitignore 未忽略 .run/", errors)


def main() -> int:
    errors: list[str] = []
    check_files(errors)
    check_scope_doc(errors)
    check_server_js(errors)
    check_py_symbols(errors)
    check_hooks(errors)
    check_status_nav(errors)
    check_env_example(errors)
    check_gitignore_run(errors)

    if errors:
        print("SCOPE_FIXED 验收失败:\n")
        for e in errors:
            print(f"  ✗ {e}")
        print(f"\n共 {len(errors)} 项。见 docs/SCOPE_FIXED.md")
        return 1

    print("SCOPE_FIXED 验收通过（静态检查）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
