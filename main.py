"""
鹦鹉螺引擎 (NautilusTrader) IBKR HelloWorld 主程序

组件说明：
  BarLoggerStrategy    — 订阅 AAPL 1分钟K线，打印日志
  OrderGatewayActor    — 监听 HTTP 8888 端口，接收外部下单指令，
                         通过 MessageBus 发布 ExternalOrderCommand，
                         再经 RiskEngine → ExecEngine 提交到 IBKR

测试下单：
  引擎启动后，在另一个终端运行：
    python order_sender.py

前置条件：
  1. TWS 或 IB Gateway 已启动并开启 API
  2. 修改下方 ACCOUNT_ID 为你的真实账号
"""
import os

from nautilus_trader.adapters.interactive_brokers.common import IB
from nautilus_trader.adapters.interactive_brokers.config import IBMarketDataTypeEnum
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from nautilus_trader.adapters.interactive_brokers.config import InteractiveBrokersExecClientConfig
from nautilus_trader.adapters.interactive_brokers.config import (
    InteractiveBrokersInstrumentProviderConfig,
)
from nautilus_trader.adapters.interactive_brokers.config import SymbologyMethod
from nautilus_trader.adapters.interactive_brokers.factories import (
    InteractiveBrokersLiveDataClientFactory,
)
from nautilus_trader.adapters.interactive_brokers.factories import (
    InteractiveBrokersLiveExecClientFactory,
)
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import RoutingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId

from order_actor import OrderGatewayActor, OrderGatewayConfig
from strategy import BarLoggerStrategy, BarLoggerStrategyConfig

# ============================================================
# ⚠️  FA 支持增强（monkey-patch）
#
# 问题：NautilusTrader 的 IBOrderTags 不包含 faGroup/faMethod 字段，
#       execution.py 在解析 IBOrderTags JSON 时会丢弃未知字段，
#       导致 ib_order.faGroup 始终为空（IBKR 默认发到 ALL 账户）。
#
# 修复：在运行时用子类扩展 IBOrderTags，加入 FA 字段，
#       然后替换 execution.py 模块中的引用。
#       这样 _attach_order_tags 里的 setattr(ib_order, "faGroup", "dt_test")
#       就能正确写入 IBKR IBOrder 对象。
# ============================================================
from nautilus_trader.adapters.interactive_brokers.common import IBOrderTags
import nautilus_trader.adapters.interactive_brokers.execution as _ib_exec_mod


class _FAIBOrderTags(IBOrderTags, frozen=True):
    """扩展 IBOrderTags，加入 FA Group 分配字段"""
    faGroup: str = ""        # FA Group 名称，如 "dt_test"
    faMethod: str = ""       # 分配方法：EqualQuantity | AvailableEquity | NetLiq | PctChange
    faProfile: str = ""      # FA Profile 名称（与 faGroup 互斥）


# 替换 execution.py 模块中的 IBOrderTags 引用，使解析时包含 FA 字段
_ib_exec_mod.IBOrderTags = _FAIBOrderTags

# ============================================================
# ⚙️  用户配置区
# ============================================================
IBG_HOST = "127.0.0.1"
IBG_PORT = 7496       # 实盘 TWS=7496 | 实盘 Gateway=4001 | 模拟 TWS=7497
IBG_CLIENT_ID = 1

ACCOUNT_ID = os.environ.get("IB_ACCOUNT_ID", "F10251881")  # FA 主账号

# FA Group 配置
FA_GROUP = os.environ.get("IB_FA_GROUP", "dt_test")
FA_METHOD = "NetLiq"   # dt_test group 的默认分配方式

# K 线订阅合约（P6: 多标的）
BAR_INSTRUMENT_ID = "AAPL.NASDAQ"   # 主合约（兼容旧配置）
GATEWAY_INSTRUMENTS_EXTRA = [
    "QQQ.NASDAQ",
    "NVDA.NASDAQ",
    "TSLA.NASDAQ",
]

# 下单网关支持的全部合约（预加载）
GATEWAY_INSTRUMENTS = list({
    BAR_INSTRUMENT_ID,
    *GATEWAY_INSTRUMENTS_EXTRA,
})

MARKET_DATA_TYPE = IBMarketDataTypeEnum.REALTIME

# ============================================================
# 合约提供者配置
# ============================================================
instrument_provider_config = InteractiveBrokersInstrumentProviderConfig(
    symbology_method=SymbologyMethod.IB_SIMPLIFIED,
    load_ids=frozenset(GATEWAY_INSTRUMENTS),
)

# ============================================================
# 交易节点配置
# ============================================================
config_node = TradingNodeConfig(
    trader_id="HELLO-WORLD-001",
    logging=LoggingConfig(log_level="INFO", log_colors=True),
    data_clients={
        IB: InteractiveBrokersDataClientConfig(
            ibg_host=IBG_HOST,
            ibg_port=IBG_PORT,
            ibg_client_id=IBG_CLIENT_ID,
            handle_revised_bars=False,
            use_regular_trading_hours=False,  # False=包含盘前/盘后；True=仅 RTH 09:30-16:00
            market_data_type=MARKET_DATA_TYPE,
            instrument_provider=instrument_provider_config,
        ),
    },
    exec_clients={
        IB: InteractiveBrokersExecClientConfig(
            ibg_host=IBG_HOST,
            ibg_port=IBG_PORT,
            ibg_client_id=IBG_CLIENT_ID,
            account_id=ACCOUNT_ID,
            instrument_provider=instrument_provider_config,
            routing=RoutingConfig(default=True),
        ),
    },
    data_engine=LiveDataEngineConfig(
        time_bars_timestamp_on_close=False,
        validate_data_sequence=True,
    ),
    timeout_connection=90.0,
    timeout_reconciliation=5.0,
    timeout_portfolio=5.0,
    timeout_disconnection=5.0,
    timeout_post_stop=2.0,
)

# ============================================================
# 策略 1：K 线日志 Strategy
# ============================================================
bar_strategy = BarLoggerStrategy(
    config=BarLoggerStrategyConfig(
        instrument_id=InstrumentId.from_str(BAR_INSTRUMENT_ID),
        instrument_ids=tuple(GATEWAY_INSTRUMENTS),  # P6: 多标的
        bar_step=1,
        st_period=10,
        st_mult=2.0,
        ema_period=21,
        history_days=1,   # 拉取当日历史 K 线用于预热
    )
)

# ============================================================
# 策略 2：订单网关 Actor（HTTP 8888 → MessageBus → ExecEngine）
# ============================================================
gateway_actor = OrderGatewayActor(
    config=OrderGatewayConfig(
        http_host="localhost",
        http_port=8888,
        fa_group=FA_GROUP,
        fa_method=FA_METHOD,
    )
)

# ============================================================
# 启动交易节点
# ============================================================
node = TradingNode(config=config_node)
node.trader.add_strategy(bar_strategy)
node.trader.add_strategy(gateway_actor)   # Actor 以 Strategy 形式注册
node.add_data_client_factory(IB, InteractiveBrokersLiveDataClientFactory)
node.add_exec_client_factory(IB, InteractiveBrokersLiveExecClientFactory)
node.build()

if __name__ == "__main__":
    try:
        print("=" * 60)
        print("  鹦鹉螺引擎 IBKR HelloWorld + OrderGateway")
        print(f"  K线合约: {BAR_INSTRUMENT_ID} | 账户: {ACCOUNT_ID}")
        print(f"  下单网关: http://localhost:8888/order")
        print("  发送测试单: python order_sender.py")
        print("  按 Ctrl+C 停止...")
        print("=" * 60)
        node.run()
    finally:
        node.dispose()
