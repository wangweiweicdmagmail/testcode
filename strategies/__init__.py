"""周期策略层（设计原则 #1：每周期一个 NautilusTrader Strategy）。

- _common：M1/M5 策略共享的 ET 时区工具、Redis 写入辅助、常量
- m1_indicator.M1IndicatorStrategy：M1 周期指标计算策略
- m5_indicator.M5IndicatorStrategy：M5 周期指标计算策略（引擎原生订阅）
"""
