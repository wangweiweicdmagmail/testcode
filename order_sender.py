"""
外部下单测试脚本 (order_sender.py)

向 OrderGatewayActor 的 HTTP 接口发送下单指令。

使用方法：
  python order_sender.py           # 普通市价单
  python order_sender.py --bracket # 括号单（市价+止损，定时移动止损）
"""
import json
import sys
import urllib.error
import urllib.request

GATEWAY_URL = "http://localhost:8888/order"


def send_order(payload: dict) -> dict:
    """向 OrderGatewayActor 发送一条下单指令"""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        GATEWAY_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            result = json.loads(resp.read())
            print(f"✅ 指令已接受: {result}")
            return result
    except urllib.error.URLError as e:
        print(f"❌ 连接失败: {e}")
        print("   请确认引擎已启动 (python main.py)")
        sys.exit(1)


def test_market_order():
    """测试普通市价单"""
    print("\n[1] 发送 MKT BUY 1股 QQQ.NASDAQ ...")
    send_order({
        "instrument_id": "QQQ.NASDAQ",
        "side": "BUY",
        "qty": 1,
        "order_type": "MARKET",
    })


def test_bracket_order():
    """
    测试括号单：
      - 入场：市价买入 1股 QQQ
      - 止损：初始触发价 601
      - 1分钟后：止损价 → 602
      - 再1分钟后：止损价 → 603
      - 括号联动：取消主单自动取消止损单
    """
    print("\n[2] 发送 BRACKET BUY 1股 QQQ.NASDAQ ...")
    print("    止损价: 601 → 602（60s后）→ 603（120s后）")
    send_order({
        "instrument_id": "QQQ.NASDAQ",
        "side":          "BUY",
        "qty":           1,
        "order_type":    "BRACKET",
        "stop_loss":     601,        # 初始止损触发价
        "sl_steps":      [602, 603], # 依次修改的止损价
        "sl_step_secs":  60,         # 每步间隔（秒）
    })
    print("\n止损单已注册，引擎将在 60s 后自动将止损价改为 602，")
    print("再过 60s 后改为 603。请在 TWS 中观察止损单变化。")
    print("若需要测试联动取消：在 TWS 里手动取消主单，止损单应自动消失。")


if __name__ == "__main__":
    print("=" * 55)
    print("  外部下单测试 → OrderGatewayActor")
    print("=" * 55)

    if "--bracket" in sys.argv:
        test_bracket_order()
    else:
        test_market_order()

    print("\n发送完毕，请查看引擎日志确认执行结果。")
