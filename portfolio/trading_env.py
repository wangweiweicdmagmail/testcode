"""TRADING_ENV 护栏：paper=不向 IBKR 提交自动单；live=允许实盘执行。"""
from __future__ import annotations

import os

_VALID = frozenset({"paper", "live"})


def trading_env() -> str:
    raw = os.environ.get("TRADING_ENV", "paper").strip().lower()
    return raw if raw in _VALID else "paper"


def live_orders_allowed() -> bool:
    return trading_env() == "live"


def allow_fixed_qty(fixed_qty: int) -> bool:
    """fixed_qty>0 会跳过以损定量；实盘需显式 ALLOW_FIXED_QTY=1。"""
    if fixed_qty <= 0:
        return True
    flag = os.environ.get("ALLOW_FIXED_QTY", "").strip().lower()
    return flag in ("1", "true", "yes", "on")


def production_safety_warnings() -> list[str]:
    """启动时打印的 production 告警（非空表示应处理后再上实盘）。"""
    out: list[str] = []
    if trading_env() != "live":
        return out
    if not os.environ.get("ORDER_GATEWAY_SECRET", "").strip():
        out.append("TRADING_ENV=live 但未设置 ORDER_GATEWAY_SECRET（8888 网关无 Token 保护）")
    bind = os.environ.get("NAUTILUS_BIND_HOST", "127.0.0.1").strip().lower()
    if bind not in ("127.0.0.1", "localhost"):
        if not os.environ.get("NAUTILUS_API_SECRET", "").strip():
            out.append("TRADING_ENV=live 但未设置 NAUTILUS_API_SECRET（前端审批/平仓 API 无鉴权）")
    bind_display = os.environ.get("NAUTILUS_BIND_HOST", "127.0.0.1").strip()
    if bind_display in ("0.0.0.0", "::", ""):
        out.append(f"NAUTILUS_BIND_HOST={bind_display or '(空)'} — 前端 API 暴露于所有网卡，生产建议 127.0.0.1")
    fixed = max(0, int(os.environ.get("AUTO_FIXED_QTY", "0") or "0"))
    if fixed > 0 and not allow_fixed_qty(fixed):
        out.append(
            f"AUTO_FIXED_QTY={fixed} 但未 ALLOW_FIXED_QTY=1 — 固定股数会绕过以损定量"
        )
    return out
