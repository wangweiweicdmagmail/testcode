import json
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy
from events import BarCollectedEvent, STTrailSettingsEvent

class ExitManagerConfig(StrategyConfig, frozen=True):
    """ExitManager 配置"""
    pass

class ExitManager(Strategy):
    """
    独立止盈管理器 Actor。
    负责监听指标变化并根据用户设置执行自动止盈逻辑。
    """
    def __init__(self, config: ExitManagerConfig) -> None:
        super().__init__(config)
        self._st_trail_active = {}  # symbol -> bool

    def on_start(self) -> None:
        # 订阅指标更新事件
        self.msgbus.subscribe(
            topic="bar.collected",
            handler=self._on_bar_collected
        )
        # 订阅设置变更事件
        self.msgbus.subscribe(
            topic="settings.st_trail",
            handler=self._on_settings_change
        )
        self.log.info("[ExitManager] 已启动，监听指标与设置变更")

    def _on_settings_change(self, event: STTrailSettingsEvent) -> None:
        self._st_trail_active[event.symbol] = event.active
        self.log.info(f"[ExitManager] {event.symbol} ST 跟踪止盈已{'开启' if event.active else '关闭'}")

    def _on_bar_collected(self, event: BarCollectedEvent) -> None:
        sym = event.symbol
        if not self._st_trail_active.get(sym, False):
            return

        bar = event.bar
        # 获取当前标的的持仓
        instrument_id = InstrumentId.from_str(bar["instrument_id"] if "instrument_id" in bar else f"{sym}.NASDAQ") # 兼容处理
        positions = self.cache.positions_open(instrument_id)
        
        if not positions:
            return

        # ST 核心退出逻辑
        c = bar["close"]
        st_val = bar["st_value"]
        st_dir = bar["st_dir"] # 1=多, -1=空

        for pos in positions:
            should_exit = False
            reason = ""
            
            if pos.is_long:
                if c < st_val:
                    should_exit = True
                    reason = f"收盘价({c}) 跌破 ST 线({st_val})"
                elif st_dir == -1:
                    should_exit = True
                    reason = "ST 指标转向 (BULL -> BEAR)"
            else: # Short position
                if c > st_val:
                    should_exit = True
                    reason = f"收盘价({c}) 突破 ST 线({st_val})"
                elif st_dir == 1:
                    should_exit = True
                    reason = "ST 指标转向 (BEAR -> BULL)"

            if should_exit:
                self.log.warning(f"[ExitManager] !!! {sym} 触发展平条件: {reason}")
                self._close_position(pos)

    def _close_position(self, pos) -> None:
        """执行全平市价单"""
        side = OrderSide.SELL if pos.is_long else OrderSide.BUY
        order = self.order_factory.market(
            instrument_id=pos.instrument_id,
            order_side=side,
            quantity=pos.quantity,
            time_in_force=TimeInForce.DAY
        )
        self.log.info(f"[ExitManager] → 提交平仓指令: {side} {pos.quantity} {pos.instrument_id}")
        self.submit_order(order)
