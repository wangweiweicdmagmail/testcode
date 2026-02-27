/**
 * server.js — Node.js WebSocket + HTTP 服务器
 * 从 Redis 读取 K 线和仓位数据，推送给前端
 */
const express = require("express");
const { WebSocketServer } = require("ws");
const Redis = require("ioredis");
const http = require("http");
const path = require("path");

const PORT = 3000;
const SYMBOL = process.env.SYMBOL || "QQQ";

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const redis = new Redis({
    host: "localhost", port: 6379, maxRetriesPerRequest: null,
    retryStrategy: (t) => Math.min(t * 500, 5000)
});
redis.on("error", (err) => console.error(`[Redis主连接] ${err.message}`));


// 静态文件服务
app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());


// REST API：获取所有数据（M3: 最多返回最近 500 根 K 线，避免大 JSON 打爆 Node）
const MAX_BARS = 500;
const ALL_SYMBOLS = ["QQQ", "AAPL", "NVDA", "TSLA"];

app.get("/api/data/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const [m1Raw, m5Raw, posRaw, prevDayRaw] = await Promise.all([
            redis.get(`bars:1m:${symbol}`),
            redis.get(`bars:5m:${symbol}`),
            redis.get(`position:${symbol}`),
            redis.get(`prev_day:${symbol}`),   // 引擎启动时从日K写入
        ]);

        if (!m1Raw) {
            return res.status(404).json({ error: `No data for ${symbol}. Run data_feeder.py first.` });
        }

        const m1All = JSON.parse(m1Raw);
        const m5All = m5Raw ? JSON.parse(m5Raw) : [];

        // 防御层：按时间戳去重（保留最后出现的），确保 LightweightCharts setData 时间严格递增
        function dedupBars(bars) {
            const map = new Map();
            bars.forEach(b => map.set(b.time, b));
            return Array.from(map.values()).sort((a, b) => a.time - b.time);
        }

        // 计算昨日 H/L/C（从 5m bars 中筛选昨日 ET 日期数据）
        // ET fake-UTC：bars.time 已是 ET fake-UTC 秒
        function calcPrevDay(m5Bars) {
            if (!m5Bars.length) return null;
            const now = Date.now() / 1000;
            const month = new Date().getUTCMonth() + 1;
            const etOff = (month >= 3 && month <= 11) ? -4 * 3600 : -5 * 3600;
            const etNow = now + etOff;
            // 今日 ET 凌晨 0:00（fake-UTC）
            const todayMidnight = etNow - (etNow % 86400);
            // 昨日 ET 凌晨 0:00
            const prevMidnight = todayMidnight - 86400;
            // 筛选昨日 09:30-16:00（ET fake-UTC 秒）
            const prevOpen = prevMidnight + 9 * 3600 + 30 * 60;
            const prevClose = prevMidnight + 16 * 3600;
            const prevBars = m5Bars.filter(b => b.time >= prevOpen && b.time < prevClose);
            if (!prevBars.length) return null;
            const prevHigh = Math.max(...prevBars.map(b => b.high));
            const prevLow = Math.min(...prevBars.map(b => b.low));
            const prevClosePrice = prevBars[prevBars.length - 1].close;
            return { high: prevHigh, low: prevLow, close: prevClosePrice };
        }

        res.json({
            symbol,
            m1_bars: dedupBars(m1All).slice(-MAX_BARS),
            m5_bars: dedupBars(m5All).slice(-MAX_BARS),
            position: posRaw ? JSON.parse(posRaw) : null,
            // 优先用引擎写入的日K数据，否则 fallback 到从5m bars计算
            prev_day: prevDayRaw ? JSON.parse(prevDayRaw) : calcPrevDay(m5All),
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /api/indicators — 所有标的的最新指标聚合（ST积分 / EMA积分）
function calcSTScore(bars) {
    // 从最后一根有效 ST 的 bar 往前，统计连续同向的 bar 数量
    const valid = bars.filter(b => b.st_dir !== null && b.st_dir !== undefined);
    if (!valid.length) return 0;
    const lastDir = valid[valid.length - 1].st_dir;
    let count = 0;
    for (let i = valid.length - 1; i >= 0; i--) {
        if (valid[i].st_dir === lastDir) count++;
        else break;
    }
    return lastDir === 1 ? count : -count;   // 做多正数，做空负数
}

function calcATR10(bars) {
    // 计算最近 10 期 ATR（使用最后 11 根 bar，得 10 个 TR）
    const recent = bars.slice(-11);
    if (recent.length < 2) return null;
    let sumTR = 0, n = 0;
    for (let i = 1; i < recent.length; i++) {
        const h = recent[i].high, l = recent[i].low, pc = recent[i - 1].close;
        const tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        sumTR += tr;
        n++;
    }
    return n > 0 ? sumTR / n : null;
}

// ─── 日内连续新高状态（内存，进程存活期间有效）─────────────────────────
// 结构：{ [symbol]: { dayKey: 'YYYYMMDD', dayHigh: number, count: number } }
const nhState = {};

// 从 Redis 中的 5m bars 重算当日连续新高计数（用于 /api/indicators 恢复状态）
function calcNewHigh(m5Bars) {
    if (!m5Bars || !m5Bars.length) return { count: 0, dayHigh: null };
    // 只保留今日的 bar（Unix timestamp 按 ET fake-UTC 判断）
    const now = Date.now() / 1000;
    // ET offset 简化: 夏令时3-11月 -4h，其他 -5h
    const month = new Date().getUTCMonth() + 1;
    const etOffsetSec = (month >= 3 && month <= 11) ? -4 * 3600 : -5 * 3600;
    // ET 今日零点（ET fake-UTC）
    const etNow = now + etOffsetSec;
    const etMidnight = etNow - (etNow % 86400);  // 当天0点 ET fake-UTC
    const todayBars = m5Bars.filter(b => b.time >= etMidnight);
    if (!todayBars.length) return { count: 0, dayHigh: null };

    let dayHigh = -Infinity;
    let count = 0;
    for (const bar of todayBars) {
        const c = bar.close;
        if (c > dayHigh) {
            dayHigh = c;
            count++;
        } else {
            count = 0;  // 跌破新高，清零
        }
    }
    return { count, dayHigh };
}

// 在 kline:5m: 推送时更新 nhState（实时路径）
function updateNHState(symbol, close) {
    const month = new Date().getUTCMonth() + 1;
    const etOffsetSec = (month >= 3 && month <= 11) ? -4 * 3600 : -5 * 3600;
    const etNow = Math.floor(Date.now() / 1000) + etOffsetSec;
    const dayKey = new Date(etNow * 1000).toISOString().slice(0, 10);  // YYYY-MM-DD

    if (!nhState[symbol] || nhState[symbol].dayKey !== dayKey) {
        // 新的一天，重置
        nhState[symbol] = { dayKey, dayHigh: close, count: 1 };
    } else {
        const s = nhState[symbol];
        if (close > s.dayHigh) {
            s.dayHigh = close;
            s.count++;
        } else {
            s.count = 0;
        }
    }
    return nhState[symbol].count;
}

app.get("/api/indicators", async (req, res) => {
    try {
        const results = await Promise.all(ALL_SYMBOLS.map(async sym => {
            const [m1Raw, m5Raw] = await Promise.all([
                redis.get(`bars:1m:${sym}`),
                redis.get(`bars:5m:${sym}`),
            ]);
            if (!m1Raw) return { symbol: sym, error: "no data" };

            const m1 = JSON.parse(m1Raw);
            const m5 = m5Raw ? JSON.parse(m5Raw) : [];
            const lastM1 = m1[m1.length - 1];
            const lastM5 = m5.length ? m5[m5.length - 1] : null;

            // M1 ST 积分
            const stScoreM1 = calcSTScore(m1);
            // M5 ST 积分（同逻辑，换 M5 bars）
            const stScoreM5 = m5.length ? calcSTScore(m5) : 0;
            // EMA 积分：(M5.close - M5.ema21) / ATR10(M5)
            let emaScore = null;
            if (lastM5 && lastM5.ema21 != null) {
                const atr10 = calcATR10(m5);
                if (atr10 && atr10 > 0) {
                    emaScore = parseFloat(((lastM5.close - lastM5.ema21) / atr10).toFixed(3));
                }
            }

            const nhResult = nhState[sym]
                ? nhState[sym]
                : calcNewHigh(m5);  // 首次从 bars 恢复
            if (!nhState[sym]) nhState[sym] = nhResult;  // 缓存

            return {
                symbol: sym,
                price: lastM1.close,
                st_score_m1: stScoreM1,     // M1 ST 积分（做多+，做空-）
                st_score_m5: stScoreM5,     // M5 ST 积分
                ema_score: emaScore,        // EMA 积分（ATR 倍数，正=价格在EMA上）
                nh_score: nhResult.count ?? 0,  // 日内连续新高计数
                st_dir_m1: lastM1.st_dir,
                st_dir_m5: lastM5 ? lastM5.st_dir : null,
                st_val_m1: lastM1.st_value,
                ema21_m5: lastM5 ? lastM5.ema21 : null,
            };
        }));

        // 排序：优先按 M1 ST 积分降序（做多最强在顶）
        results.sort((a, b) => (b.st_score_m1 ?? 0) - (a.st_score_m1 ?? 0));
        res.json(results);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// POST /api/position/:symbol — 开仓（写入 Redis）
app.post("/api/position/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const { entry_price, stop_loss, quantity, entry_time } = req.body;
    if (!entry_price || !stop_loss || !quantity) {
        return res.status(400).json({ error: "entry_price / stop_loss / quantity required" });
    }
    // P3: entry_time 使用 ET fake-UTC（与 K 线时间戳格式统一）
    // ET fake-UTC = 真实 UTC - UTC偏移量（EST=-5h, EDT=-4h）
    // 简化处理：取当前月份判断夏/冬令时（3-11月 EDT=-4h，其他 EST=-5h）
    const nowUtc = Math.floor(Date.now() / 1000);
    const month = new Date().getUTCMonth() + 1; // 1-12
    const etOffset = (month >= 3 && month <= 11) ? -4 * 3600 : -5 * 3600;
    const etFakeUtc = entry_time || (nowUtc + etOffset);

    const pos = {
        symbol,
        entry_price: parseFloat(entry_price),
        stop_loss: parseFloat(stop_loss),
        quantity: parseInt(quantity),
        entry_time: etFakeUtc,          // ET fake-UTC，与 K 线时间戳一致
        current_price: 0, pnl: 0, pnl_pct: 0,
    };
    await redis.set(`position:${symbol}`, JSON.stringify(pos));
    res.json({ ok: true, position: pos });
});

// DELETE /api/position/:symbol — 平仓（删除 Redis 仓位）
app.delete("/api/position/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    await redis.del(`position:${symbol}`);
    res.json({ ok: true });
});


// ── 引擎代理路由（转发到 order_actor :8888）─────────────────────────────

// symbol → NautilusTrader instrument_id 映射
const SYMBOL_MAP = {
    QQQ: 'QQQ.NASDAQ',
    AAPL: 'AAPL.NASDAQ',
    NVDA: 'NVDA.NASDAQ',
    TSLA: 'TSLA.NASDAQ',
};

/**
 * 通用 HTTP 代理：向 order_actor 发 GET/POST 请求
 * 引擎未启动时返回 fallback 值，不抛错
 */
function proxyToEngine(method, path, body, fallback) {
    return new Promise((resolve) => {
        const http = require('http');
        const postData = body ? JSON.stringify(body) : null;
        const opts = {
            host: '127.0.0.1', port: 8888, path, method,
            headers: { 'Content-Type': 'application/json' },
        };
        if (postData) opts.headers['Content-Length'] = Buffer.byteLength(postData);

        const req = http.request(opts, (r) => {
            let data = '';
            r.on('data', chunk => data += chunk);
            r.on('end', () => {
                try { resolve(JSON.parse(data)); }
                catch { resolve(fallback); }
            });
        });
        req.setTimeout(3000, () => { req.destroy(); resolve(fallback); }); // 3s 超时，引擎卡死时降级
        req.on('error', () => resolve(fallback));  // 引擎未启动时降级
        if (postData) req.write(postData);
        req.end();
    });
}

// GET /api/account — 真实账户余额（优先从 Redis account:funds 读取，引擎离线时 fallback 到 order_actor）
app.get('/api/account', async (req, res) => {
    try {
        const raw = await redis.get('account:funds');
        if (raw) {
            return res.json(JSON.parse(raw));
        }
    } catch (e) {
        console.warn('[account] Redis 读取失败，尝试大幅降级到 order_actor:', e.message);
    }
    // Redis 无数据时 fallback 到 order_actor
    const data = await proxyToEngine('GET', '/account', null,
        { account_id: '', balances: [], ts: 0, engine_offline: true });
    res.json(data);
});

// GET /api/positions — 真实 IBKR 仓位
app.get('/api/positions', async (req, res) => {
    const data = await proxyToEngine('GET', '/positions', null, []);
    res.json(data);
});

// POST /api/order/:symbol — 下单代理
app.post('/api/order/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const instrumentId = SYMBOL_MAP[symbol];
    if (!instrumentId) {
        return res.status(400).json({ error: `未知标的: ${symbol}，支持: ${Object.keys(SYMBOL_MAP).join(', ')}` });
    }
    const { side, qty, stop_loss, order_type = 'BRACKET' } = req.body;
    if (!side || !qty) {
        return res.status(400).json({ error: 'side 和 qty 必填' });
    }
    const payload = { instrument_id: instrumentId, side, qty: parseInt(qty), order_type };
    if (stop_loss != null) payload.stop_loss = parseFloat(stop_loss);

    console.log(`📤 下单代理 → ${instrumentId} ${side} x${qty} SL=${stop_loss} type=${order_type}`);
    const data = await proxyToEngine('POST', '/order', payload, { error: '引擎未启动', engine_offline: true });
    res.json(data);
});

// POST /api/settings/:symbol — ST 跟踪止盈等开关
app.post("/api/settings/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const existing = await redis.get(`settings:${symbol}`);
    const settings = existing ? JSON.parse(existing) : {};
    Object.assign(settings, req.body);
    await redis.set(`settings:${symbol}`, JSON.stringify(settings));
    console.log(`⚙️  设置更新 [${symbol}]:`, settings);

    // ★ 同步给 Python 引擎内存
    if (req.body.st_trail !== undefined) {
        await proxyToEngine('POST', '/settings', {
            symbol: symbol,
            active: req.body.st_trail
        }).catch(e => console.error("同步设置到引擎失败:", e.message));
    }

    res.json({ ok: true, settings });
});
// Redis 推送设置——防止连接丢失导致进程崩溃
const REDIS_OPTS = {
    host: "localhost",
    port: 6379,
    maxRetriesPerRequest: null,          // 不限制重试次数
    retryStrategy: (times) => Math.min(times * 500, 5000), // 最大 5s 重试间隔
    lazyConnect: false,
};

// Redis Pub/Sub：实时推送新 K 线给前端
// 频道与 strategy.py 一致：bars:1m:* / bars:5m:*
const redisSub = new Redis(REDIS_OPTS);

redisSub.on("error", (err) => {
    console.error(`[Redis订阅] 连接错误，将自动重试: ${err.message}`);
});

redisSub.on("ready", () => {
    console.log(`[✅ Redis订阅] 已连接，订阅 bars:1m:* / bars:5m:* / kline:1m:* / kline:5m:* / position:* / order:* / account:*`);
    redisSub.psubscribe("bars:1m:*", "bars:5m:*", "kline:1m:*", "kline:5m:*", "position:*", "order:*", "account:*").catch(console.error);
});

redisSub.on("pmessage", (_pattern, channel, message) => {
    try {
        const parsed = JSON.parse(message);

        // kline:5m: 收盘事件 → 更新日内连续新高状态并广播
        if (channel.startsWith('kline:5m:')) {
            const sym = channel.split(':')[2];
            if (sym && ALL_SYMBOLS.includes(sym)) {
                const count = updateNHState(sym, parsed.close);
                // 广播 nh:update 事件给前端（用于语音播报）
                const nhPayload = JSON.stringify({
                    channel: 'nh:update',
                    data: { symbol: sym, count, close: parsed.close },
                });
                wss.clients.forEach(c => c.readyState === 1 && c.send(nhPayload));
            }
        }

        const payload = JSON.stringify({ channel, data: parsed });
        wss.clients.forEach((client) => {
            if (client.readyState === 1) {
                client.send(payload);
            }
        });
    } catch (e) {
        console.error(`[Redis订阅] 消息解析失败: ${e.message}`);
    }
});

wss.on("connection", (ws) => {
    console.log("前端已连接 WebSocket");
    ws.on("close", () => console.log("前端 WebSocket 断开"));
    ws.on("error", (err) => console.error(`WebSocket 错误: ${err.message}`));
});

// 全局异常兼容层——防止未捕获异常导致进程退出
process.on("uncaughtException", (err) => {
    console.error(`[未捕获异常] ${err.message}`, err.stack);
});

process.on("unhandledRejection", (reason) => {
    console.error(`[未处理 Promise] ${reason}`);
});

server.listen(PORT, () => {
    console.log(`✅ 服务器启动: http://localhost:${PORT}`);
    console.log(`   监听频道: bars:1m:* / bars:5m:*`);
});
