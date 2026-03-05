# 鹦鹉螺引擎 IBKR — 项目上下文

## ⚠️ 时区（必读，禁止再犯）

| 时区 | UTC 偏移 | 备注 |
|------|---------|------|
| 上海 / 北京（CST） | UTC+8 | 系统显示的本地时间 |
| 纽约（ET 夏令时 EDT） | UTC-4 | 美股交易时段 3-11月 |
| 纽约（ET 冬令时 EST） | UTC-5 | 美股交易时段 11-3月 |

**换算公式（夏令时）：**
- 北京时间 → 纽约时间：`北京时间 - 12小时`
- 例：北京 23:20 = 纽约 11:20（完全在交易时段内）

**美股交易时段（纽约时间）：**
- 盘前：04:00 – 09:29
- 正市：09:30 – 16:00
- 盘后：16:00 – 20:00

**北京时间对应：**
- 夏令时正市：北京 21:30 – 次日 04:00
- 冬令时正市：北京 22:30 – 次日 05:00

> [!CAUTION]
> AI 助手看到的系统时间是北京时间。凡是涉及美股是否在交易的判断，必须先换算为纽约时间再做结论，禁止直接用北京时间判断。

---

## 项目概述

- **项目名称**：鹦鹉螺引擎（Nautilus Trader × IBKR）
- **目的**：连接 Interactive Brokers 进行美股实盘量化交易
- **架构**：三层（引擎 Python → Redis → 前端 Node.js）

## 核心组件

| 文件 | 职责 |
|------|------|
| `main.py` | 入口，配置并启动 TradingNode |
| `strategy.py` | 核心策略：K线聚合、指标计算（SuperTrend/EMA）、写 Redis |
| `order_actor.py` | HTTP 下单网关（端口 8888），订单生命周期管理 |
| `exit_manager.py` | 独立止盈管理器，监听 bar.collected 事件 |
| `events.py` | 自定义 MessageBus 事件类 |
| `frontend/server.js` | Node.js WebSocket 服务器，展示 K 线和指标 |

## 账户配置

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `IB_ACCOUNT_ID` | FA 主账号 | `F10251881` |
| `IB_FA_GROUP` | FA Group 名称 | `dt_test` |
| `IB_FA_METHOD` | 分配方法 | `NetLiq` |
| `IBG_PORT` | TWS/Gateway 端口 | `7496`（实盘 TWS） |
| `IBG_CLIENT_ID` | API 客户端 ID | `2` |

## 启动命令

```bash
# 实盘
cd /Users/weiweiwang/testcode/nautilus_ibkr_helloworld
python main.py

# 回测
python main.py --mode backtest

# 前端
cd frontend && node server.js
```

> [!IMPORTANT]
> **重启规则**：说"重启"默认表示**前端 + 引擎同时重启**。只有明确说"重启前端"时才只重启前端。
>
> 重启命令：
> ```bash
> # 重启引擎（前后端都重启时）
> pkill -f "python main.py"; pkill -f "node server.js"
> cd /Users/weiweiwang/testcode/nautilus_ibkr_helloworld && python main.py > /tmp/engine.log 2>&1 &
> cd frontend && node server.js > /tmp/frontend.log 2>&1 &
> ```


## HTTP 端点

### 引擎端（order_actor.py 端口 8888）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/account` | 账户余额（FA Group 聚合） |
| GET | `/positions` | 当前开放仓位 |
| GET | `/active-orders` | 活跃止损单 + 入场价（供前端恢复价格线） |
| GET | `/debug-orders` | 调试：打印 cache 中所有订单和持仓 |
| POST | `/order` | 下单（MARKET / LIMIT / BRACKET） |
| POST | `/settings` | 策略开关（st_trail 跟踪止盈） |
| POST | `/close` | ⏳ 待实现：反向市价平仓 + 取消止损单 |
| POST | `/modify-stop` | 修改活跃止损单触发价（body: {symbol, price}） |

### Node.js 端（server.js 端口 3000）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/data/:symbol` | 历史 K 线 + 仓位 + 昨日围栏 |
| GET | `/api/indicators` | 所有标的最新指标 |
| GET | `/api/account` | 账户余额（优先 Redis，fallback 引擎） |
| GET | `/api/positions` | 仓位（代理引擎） |
| GET | `/api/active-orders` | 活跃订单（代理引擎） |
| POST | `/api/order/:symbol` | 下单（代理引擎） |
| DELETE | `/api/position/:symbol` | 平仓 ⚠️ 目前仅删 Redis，引擎平仓待实现 |
| POST | `/api/modify-stop/:symbol` | 修改止损价（代理引擎 + 更新 Redis position.stop_loss） |
| POST | `/api/settings/:symbol` | 策略开关，同步到引擎 |

## 已实现功能汇总

- **盘前盘后数据过滤** ✅：引擎层（`strategy.py`）和前端 API（`server.js`）均过滤非正市 RTH 数据（仅保留 09:30–16:00 ET）
- **M1/M5 ST 参数分离** ✅：M1 ST（period=10, mult=3.5）/ M5 ST（period=10, mult=3.0）
- **trail_mode 重构（三套互斥止盈）** ✅：`settings:{sym}.trail_mode` 单字段控制（0=关, 1=M1-ST, 2=M5-ST, 3=EMA-M5）
- **M5 bar 收盘事件** ✅：`events.py` 新增 `BarCollectedM5Event`；`ExitManager` 订阅后处理 M5-ST 跟踪
- **动量窗口指标（mom_atr）** ✅：`_MomentumATRState` 状态机；公式 = `(M5_close₀ - M5_open₋₂) / M1_ATR_14`（把3根M5组成15分K线，实体除以M1 ATR归一化）；写入 `bars:5m:{sym}.mom_atr`；`/api/indicators` 暴露；`indicators.html` 第一列 + `index.html` 右侧面板置顶，含 4 档强度标签
- **平仓按钮** ✅：`DELETE /api/position/:symbol` → 反向市价单 + 取消止损单
- **持仓止损价修改** ✅：拖动止损线 → `POST /api/modify-stop/:symbol` → `modify_order()`
- **止损单 ID 持久化** ✅：`order:stop:{sym}` Redis 持久化，引擎重启后恢复
- **全标的指标排行** ✅：`indicators.html` 五列并排（动量窗口 / M1-ST / M5-ST / EMA偏离 / 日内高低突破）
- **日内高低突破信号（hl_score）** ✅：`server.js` 维护 `hlState`；每根 M5 bar 创新高 `+1` 累积、创新低 `-1` 累积、无突破归零；`/api/indicators` 返回 `hl_score`；`indicators.html` 第五列显示带颜色的正负值 + 语音播报（创新高/新低时自动朗读）
- **尾盘 15:45 ET 自动平仓** ✅：`exit_manager.py` `_check_eod_close()`；每根 M1 bar 检测 ET 时间 ≥ 15:45 时调用 HTTP POST `/close` 平掉所有持仓；每交易日仅触发一次（`_eod_closed_dates` set 防重复）
- **止盈跟踪改为下拉框** ✅：工具栏三个 toggle 合并为单个 `<select>`（0=关/1=M1-ST/2=M5-ST/3=EMA）；新增 `GET /api/settings/:symbol` 端点，页面加载时自动恢复已保存的 trail_mode 选中项
- **EMA21 偏差修复** ✅：修复 `_M5Bucket.flush_current()` 重复喂入 Bug
- **代码审核 Bug 修复（2026-03-05）** ✅：
  - `on_stop()` 代码错位 → 每根 M1 bar 随机关闭 HTTP 网关（最高优先级）
  - `_sync_position_to_redis` 覆写 `stop_loss=None` → 追踪止损价随机丢失
  - `_process_m5_bar` fallback 硬编码 `.NASDAQ` → TSM/SPY 等合约跟踪止损失效
  - `on_order_updated` 枚举用 `str().replace()` → Cython 返回整数序号，判断失效
  - `TERMINAL_STATUS` 统一到 `events.py`，消除 7 处重复定义
  - `__init__` 补充 `_heartbeat_running = False` 初始化，消除极端情况 `AttributeError`
  - 删除 `strategy.py` 重复的 `from events import BarCollectedEvent`
- **UI 稳定性修复（2026-03-05）** ✅：
  - 引擎心跳文字加 `min-width:4.5em`，防止在线/离线切换时工具栏抖动
  - 切换标的时不再重复播报"引擎已上线"（`sessionStorage` 跨页面保持在线状态）
- **开仓重复打点修复（2026-03-06）** ✅：
  - `FILLED` 事件和 `POSITION_OPENED` 事件均调用 `setEntryLine` + 加 marker，导致一次开仓出现两个箭头
  - 修复：`POSITION_OPENED` 改为只在 `activeOrderLines.entryLine` 不存在时才补画（外部/重启恢复场景），不再重复推 marker
- **pmessage 残留旧函数导致推送中断（2026-03-06）** ✅：
  - 重构 `nh_score → hl_score` 时未同步更新 `server.js` 的 `pmessage` 回调，每根 M5 bar 收盘时抛 `ReferenceError: updateNHState is not defined`，导致整个实时推送中断
  - 修复：改为调用 `updateHLState()`，广播频道改为 `hl:update`
- **isRTH() 冬令时 DST 判断错误（2026-03-06）** ✅：
  - `month >= 3 → -4h` 的简化规则在 DST 切换日（3月第二个周日）前的冬令时期间算出错误的 ET 时间（15:12 被算成 16:12），导致 `isRTH()` 返回 false，屏蔽所有 kline 实时推送，前端必须手动刷新
  - 修复：新增 `getETInfo()` / `getETOffsetSec()` 工具函数，使用 `Intl.DateTimeFormat('America/New_York')` 精确处理 DST 边界，替换 `isRTH()`、`etDayKey()`、`calcHLScore()`、`calcPrevDay()` 中所有简化偏移计算


## Redis 数据结构

| Key | 类型 | 内容 |
|-----|------|------|
| `bars:1m:{sym}` | List | 1分钟 K 线历史（仅正市 RTH） |
| `bars:5m:{sym}` | List | 5分钟 K 线历史（含 `mom_atr` 指标，仅正市） |
| `position:{sym}` | String | 仓位 JSON（含 `stop_loss`） |
| `settings:{sym}` | String | 策略开关 JSON：`{"trail_mode": 0/1/2/3}` |
| `order:stop:{sym}` | String | 活跃止损单 `{client_order_id, trigger_price}` — 重启恢复用 |
| `kline:1m:{sym}` | PubSub | 实时 1m K 线推送（仅 RTH） |
| `kline:5m:{sym}` | PubSub | 实时 5m K 线推送（含 `mom_atr`） |
| `order:update` | PubSub | 订单状态变更推送 |
| `account:funds` | String | 账户余额 JSON |
| `engine:heartbeat` | PubSub | 引擎心跳（每 5s） |

## 指标字段说明

| 字段 | 所在 bar | 说明 |
|------|---------|------|
| `st_value` | M1/M5 | SuperTrend 值（M1:10,3.5 / M5:10,3.0） |
| `st_dir` | M1/M5 | ST 方向：`1`=多头, `-1`=空头 |
| `ema21` | M1/M5 | EMA21 |
| `mom_atr` | M5 | 归一化15分钟动量 = `(M5_close₀ - M5_open₋₂) / M1_ATR_14`（3根M5组成15分K线实体，M1 ATR归一化；⩾3根M5 bar且M1 ATR预热后有值） |

## /api/indicators 返回字段

| 字段 | 说明 |
|------|------|
| `st_score_m1` | M1 ST 积分（连续多头+N，连续空头-N） |
| `st_score_m5` | M5 ST 积分 |
| `ema_score` | EMA 偏离积分 |
| `mom_atr` | 15分钟动量信号 |
| `hl_score` | 日内高低突破信号：+N=连续创新高，-N=连续创新低，0=无突破 |
