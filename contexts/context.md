# 项目核心上下文 (Context)

## 项目概述
本项目是一个基于 **NautilusTrader** 框架连接 **Interactive Brokers (IBKR)** 的实盘交易系统。它主要用于个人量化交易基础设施，提供从数据获取、指标计算、订单执行到前端可视化的完整链路。

## 架构特点
- **三层架构解耦**：
  - 数据源/执行层：基于 NautilusTrader 和 IBKR
  - 共享状态层：Redis
  - 展示层：Node.js WebSocket + 浏览器端 Dashboard
- **核心组件**：
  - `main.py`: 主程序，配置并启动 TradingNode
  - `strategy.py`: 核心策略模块，负责 1m K 线处理、SuperTrend/EMA 指标计算及 Redis 写入
  - `order_actor.py`: HTTP 下单网关，处理订单生命周期
  - `frontend/`: 实时可视化及交互面板

## 开发约定
- **语言**：所有解释、分析、建议使用**中文**。代码相关（变量名、函数名、文件路径）保留英文。
- **配置**：系统支持 `live` (实盘) 和 `backtest` (回测) 模式。
- **数据源**：一切数据源需来自 IBKR，无外部数据源依赖。
