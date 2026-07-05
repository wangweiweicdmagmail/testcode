"""FA Group 启动校验 — 对照 IBKR requestFA(GROUPS) 官方流程。"""
from __future__ import annotations

import threading
import time
import xml.etree.ElementTree as ET


FA_GROUPS_TYPE = 2  # ibapi FaDataTypeEnum.GROUPS


def validate_fa_group(host: str, port: int, client_id: int, fa_group: str, timeout: float = 20.0) -> tuple[bool, str]:
    """
    临时 ibapi 连接，requestFA(GROUPS)，确认 fa_group 名称存在于 TWS 配置。

    Returns (ok, message).
    """
    if not fa_group or not fa_group.strip():
        return False, "fa_group 为空"

    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
    except ImportError:
        return True, "ibapi 未安装，跳过 FA 校验"

    class _FAWrapper(EWrapper):
        def __init__(self):
            super().__init__()
            self._xml = ""
            self._done = threading.Event()
            self._error = ""

        def receiveFA(self, faData, cxml: str):
            if int(faData) == FA_GROUPS_TYPE:
                self._xml = cxml or ""
            self._done.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            if reqId == -1:
                return
            self._error = f"code={errorCode} {errorString}"
            self._done.set()

    wrapper = _FAWrapper()
    client = EClient(wrapper)
    try:
        client.connect(host, port, client_id)
    except Exception as e:
        return False, f"ibapi 连接失败: {e}"

    t = threading.Thread(target=client.run, daemon=True)
    t.start()
    time.sleep(2)
    if not client.isConnected():
        try:
            client.disconnect()
        except Exception:
            pass
        return False, "ibapi 连接未就绪"

    try:
        client.requestFA(FA_GROUPS_TYPE)
        wrapper._done.wait(timeout=timeout)
    finally:
        try:
            client.disconnect()
        except Exception:
            pass

    if wrapper._error:
        return False, wrapper._error
    if not wrapper._xml.strip():
        return False, "requestFA(GROUPS) 无返回（检查 TWS API 与 FA 权限）"

    try:
        root = ET.fromstring(wrapper._xml)
    except ET.ParseError as e:
        return False, f"FA XML 解析失败: {e}"

    names: set[str] = set()
    for grp in root.iter("Group"):
        el = grp.find("name")
        if el is not None and el.text:
            names.add(el.text.strip())

    if fa_group in names:
        return True, f"FA Group '{fa_group}' 已确认"
    preview = ", ".join(sorted(names)[:8])
    suffix = "..." if len(names) > 8 else ""
    return False, f"FA Group '{fa_group}' 不存在；可用: {preview}{suffix}"
