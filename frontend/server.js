/**
 * server.js — Node.js WebSocket + HTTP 服务器
 * 从 Redis 读取 K 线和仓位数据，推送给前端
 */
const express = require("express");
const { WebSocketServer } = require("ws");
const Redis = require("ioredis");
const http = require("http");
const path = require("path");
const fs = require("fs");
const { spawn, execSync } = require("child_process");

// 先加载项目根 .env（TRADING_ENV 等以 .env 为准，避免 shell 残留 export 覆盖）
(function loadDotEnv() {
    const envPath = path.join(__dirname, "..", ".env");
    if (!fs.existsSync(envPath)) return;
    const ALWAYS_FROM_FILE = new Set(["TRADING_ENV", "AUTO_STRATEGY_MODE"]);
    try {
        for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
            const trimmed = line.trim();
            if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) continue;
            const eq = trimmed.indexOf("=");
            const key = trimmed.slice(0, eq).trim();
            let val = trimmed.slice(eq + 1).trim();
            if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
                val = val.slice(1, -1);
            }
            if (key && (ALWAYS_FROM_FILE.has(key) || process.env[key] === undefined)) {
                process.env[key] = val;
            }
        }
    } catch (_) { /* ignore */ }
})();

const journal = require("./journal");

/** 统一关键路径日志：tag 如 Approval / AutoPM / Config */
function logKey(tag, level, message, extra) {
    const ts = new Date().toISOString();
    const suffix = extra !== undefined
        ? ` ${typeof extra === "string" ? extra : JSON.stringify(extra)}`
        : "";
    const line = `[${ts}] [${tag}] ${message}${suffix}`;
    if (level === "error") console.error(line);
    else if (level === "warn") console.warn(line);
    else console.log(line);
}

const PORT = parseInt(process.env.NAUTILUS_PORT || "3000", 10);
const HOST = process.env.NAUTILUS_BIND_HOST || "127.0.0.1";
const NAUTILUS_API_SECRET = process.env.NAUTILUS_API_SECRET || "";
const ORDER_GATEWAY_SECRET = process.env.ORDER_GATEWAY_SECRET || "";
const TRADING_ENV = (process.env.TRADING_ENV || "paper").trim().toLowerCase();
const LIVE_TRADING_ALLOWED = TRADING_ENV === "live";

/** 仅绑定本机时：前端免输 Token，仍保留 ORDER_GATEWAY_SECRET 保护 8888 */
function isLocalBindOnly() {
    const h = (HOST || "").trim().toLowerCase();
    return h === "127.0.0.1" || h === "localhost";
}
const API_AUTH_ENFORCED = Boolean(NAUTILUS_API_SECRET) && !isLocalBindOnly();
const DEFAULT_ALPHA_SYMBOLS = (process.env.ALPHA_SYMBOLS || "NVDA,TSLA,AAPL")
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
const SYMBOL = process.env.SYMBOL || "QQQ";

const FEISHU_VERIFICATION_TOKEN = process.env.FEISHU_VERIFICATION_TOKEN || "";
const FEISHU_REQUIRE_TOKEN = !["0", "false", "no", "off"].includes(
    String(process.env.FEISHU_REQUIRE_TOKEN || (NAUTILUS_API_SECRET ? "1" : "0")).toLowerCase()
);
function getApiToken(req) {
    const h = req.headers["x-nautilus-token"] || req.headers["authorization"] || "";
    if (typeof h === "string" && h.startsWith("Bearer ")) return h.slice(7).trim();
    return String(h || "").trim();
}

function requireApiSecret(req, res, next) {
    if (!API_AUTH_ENFORCED) return next();
    if (getApiToken(req) === NAUTILUS_API_SECRET) return next();
    return res.status(401).json({
        error: "unauthorized",
        hint: "设置 Header X-Nautilus-Token 或 Authorization: Bearer <NAUTILUS_API_SECRET>",
    });
}

async function hasActiveProposalForSymbol(symbol) {
    const sym = String(symbol || "").toUpperCase();
    if (!sym) return false;
    const now = Math.floor(Date.now() / 1000);
    const indexes = { pending: "proposal:pending:index", approved: "proposal:approved:index" };
    for (const st of ["pending", "approved"]) {
        const ids = await redis.zrevrange(indexes[st], 0, 99);
        for (const id of ids) {
            const p = await parseProposalHash(`proposal:${st}:${id}`);
            if (!p || String(p.symbol || "").toUpperCase() !== sym) continue;
            if (st === "pending") return true;
            if (p.executed_at) continue;
            const phase = String(p.execution_phase || "");
            if (phase === "approved_wait" || phase === "ready_to_execute" || phase === "executing") {
                const exp = parseInt(p.expires_at || "0", 10);
                if (!exp || now <= exp) return true;
            }
        }
    }
    return false;
}

const FEISHU_AGENT_ENABLED = !["0", "false", "no", "off"].includes(
    String(process.env.FEISHU_AGENT_ENABLED || "true").toLowerCase()
);
const PROJECT_ROOT = path.join(__dirname, "..");
const PID_FILE = path.join(PROJECT_ROOT, ".frontend.pid");

// ─── 单例保护：端口 + PID 文件（对齐 main.py .engine.pid）────────────────────
function isProcessAlive(pid) {
    if (!pid || pid <= 0) return false;
    try {
        process.kill(pid, 0);
        return true;
    } catch {
        return false;
    }
}

function getPidsOnPort(port) {
    // 仅匹配 LISTEN 状态的进程：客户端到该端口的 ESTABLISHED 连接（如浏览器
    // 打开着页面）不算占用，否则重启时会把浏览器连接误判为旧服务器而拒绝启动。
    try {
        const out = execSync(`lsof -ti :${port} -sTCP:LISTEN`, { encoding: "utf8" }).trim();
        if (!out) return [];
        return [...new Set(
            out.split("\n")
                .map((s) => parseInt(s.trim(), 10))
                .filter((n) => Number.isFinite(n) && n > 0 && n !== process.pid)
        )];
    } catch {
        return [];
    }
}

function collectConflictPids() {
    const conflict = new Set();
    for (const pid of getPidsOnPort(PORT)) {
        conflict.add(pid);
    }
    if (fs.existsSync(PID_FILE)) {
        try {
            const oldPid = parseInt(fs.readFileSync(PID_FILE, "utf8").trim(), 10);
            if (isProcessAlive(oldPid)) {
                conflict.add(oldPid);
            }
        } catch {
            /* stale pid file */
        }
    }
    return [...conflict];
}

function printBanner(message) {
    const inner = ` ${message} `;
    const border = "*".repeat(inner.length + 2);
    console.log(`\n${border}`);
    console.log(`*${inner}*`);
    console.log(`${border}\n`);
}

function sleepMs(ms) {
    // 真正阻塞当前线程而不空转 CPU（用于启动期同步等待）
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function killProcess(pid) {
    if (!isProcessAlive(pid)) return;
    try {
        process.kill(pid, "SIGTERM");
    } catch {
        return;
    }
    const deadline = Date.now() + 5000;
    while (Date.now() < deadline) {
        if (!isProcessAlive(pid)) return;
        sleepMs(250);
    }
    try {
        process.kill(pid, "SIGKILL");
    } catch {
        /* already gone */
    }
}

function ensureFrontendSingleton() {
    // ── 1. 端口占用（启动前最先检查）────────────────────────────────────
    const portPids = getPidsOnPort(PORT);
    if (portPids.length > 0) {
        console.log(`\n⚠️  端口 ${PORT} 已被占用 (PID: ${portPids.join(", ")})`);
    }

    // ── 2. PID 文件（对齐 main.py .engine.pid）──────────────────────────
    if (fs.existsSync(PID_FILE)) {
        try {
            const oldPid = parseInt(fs.readFileSync(PID_FILE, "utf8").trim(), 10);
            if (isProcessAlive(oldPid) && !portPids.includes(oldPid)) {
                console.log(`\n⚠️  前端已在运行中 (PID=${oldPid})`);
            }
        } catch {
            /* ignore */
        }
    }

    const pids = collectConflictPids();
    if (pids.length === 0) {
        fs.writeFileSync(PID_FILE, String(process.pid));
        return;
    }

    console.log(`   自动终止旧进程并启动新前端...`);
    for (const pid of pids) {
        killProcess(pid);
    }
    try {
        fs.unlinkSync(PID_FILE);
    } catch {
        /* ignore */
    }
    const left = getPidsOnPort(PORT);
    if (left.length > 0) {
        console.error(`   端口 ${PORT} 仍被占用 (PID: ${left.join(", ")})，请手动: lsof -i :${PORT}\n`);
        process.exit(1);
    }
    console.log(`   已终止 PID=${pids.join(", ")}，正在启动新前端...\n`);

    fs.writeFileSync(PID_FILE, String(process.pid));
}

// 单例检查必须在 Redis/HTTP 初始化之前完成（直接清理旧进程，无交互）
ensureFrontendSingleton();

function cleanupPidFile() {
    try {
        if (!fs.existsSync(PID_FILE)) return;
        const saved = fs.readFileSync(PID_FILE, "utf8").trim();
        if (saved === String(process.pid)) {
            fs.unlinkSync(PID_FILE);
        }
    } catch {
        /* ignore */
    }
}

process.on("exit", cleanupPidFile);
process.on("SIGINT", () => process.exit(0));
process.on("SIGTERM", () => process.exit(0));

function spawnFeishuAgentJob(body) {
  const tmpDir = path.join(PROJECT_ROOT, ".run", "feishu_events");
  fs.mkdirSync(tmpDir, { recursive: true });
  const tmpFile = path.join(
    tmpDir,
    `evt_${Date.now()}_${Math.random().toString(36).slice(2, 8)}.json`
  );
  fs.writeFileSync(tmpFile, JSON.stringify(body), "utf8");
  const script = path.join(PROJECT_ROOT, "gateway", "handle_feishu_message.py");
  const child = spawn("python3", [script, tmpFile], {
    cwd: PROJECT_ROOT,
    env: process.env,
    detached: true,
    stdio: "ignore",
  });
  child.unref();
  console.log(`[FeishuAgent] queued ${path.basename(tmpFile)} pid=${child.pid}`);
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });
const redis = new Redis({
    host: "localhost", port: 6379, maxRetriesPerRequest: null,
    retryStrategy: (t) => Math.min(t * 500, 5000)
});
redis.on("error", (err) => console.error(`[Redis主连接] ${err.message}`));

// ─── ET 时区工具函数（自动处理夏令时 DST）─────────────────────────────────
// 用 Intl.DateTimeFormat 精确计算当前 ET 时间（克服 'month>=3' 简化判断导致冬令时算错）
// 美国 DST：3月第二个周日 → 11月第一个周日
const _etFormatter = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});

/** 返回当前 ET 时当天的秒偏移（对应 bar.time % 86400）和日期字符串 */
function getETInfo() {
    const now = new Date();
    const parts = _etFormatter.formatToParts(now);
    const get = (t) => parseInt(parts.find(p => p.type === t).value);
    const h = get('hour'), m = get('minute'), s = get('second');
    const yy = get('year'), mo = get('month'), dd = get('day');
    const secOfDay = h * 3600 + m * 60 + s;
    const dayKey = `${yy}-${String(mo).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
    return { secOfDay, dayKey };
}

/** ET fake-UTC 偏移秒数（用于 midnight 计算，保持与引擎 bar.time 的计算一致） */
function getETOffsetSec() {
    const { secOfDay } = getETInfo();
    const nowUtc = Math.floor(Date.now() / 1000);
    const utcSecOfDay = nowUtc % 86400;
    // offset = etSecOfDay - utcSecOfDay，并四舍五入到整小时
    const rawOff = secOfDay - utcSecOfDay;
    // 处理跨日边界（-12h ~ +12h 范围内）
    if (rawOff > 43200) return rawOff - 86400;
    if (rawOff < -43200) return rawOff + 86400;
    return rawOff;
}


// 静态文件服务
app.use(express.static(path.join(__dirname, "public"), { index: "console.html" }));
app.use(express.json());

const ENGINE_HEARTBEAT_MAX_AGE_S = parseInt(process.env.ENGINE_HEARTBEAT_MAX_AGE_S || "30", 10);

function parseEngineHeartbeat(raw) {
    if (!raw) return { heartbeat: null, age_s: null, online: false };
    let hb = raw;
    let age_s = null;
    try {
        hb = JSON.parse(raw);
        if (hb && hb.ts) {
            age_s = Math.max(0, Math.floor(Date.now() / 1000) - Number(hb.ts));
        }
    } catch (_) { /* keep raw */ }
    const online = age_s != null && age_s <= ENGINE_HEARTBEAT_MAX_AGE_S;
    return { heartbeat: hb, age_s, online };
}

// GET /api/config/public — 前端公开配置（不含密钥）
app.get("/api/config/public", (req, res) => {
    const warnings = [];
    if (LIVE_TRADING_ALLOWED) {
        if (!NAUTILUS_API_SECRET && !isLocalBindOnly()) {
            warnings.push("TRADING_ENV=live 但未配置 NAUTILUS_API_SECRET");
        }
        if (!ORDER_GATEWAY_SECRET) {
            warnings.push("TRADING_ENV=live 但未配置 ORDER_GATEWAY_SECRET");
        }
    }
    if (HOST === "0.0.0.0" || HOST === "::") {
        warnings.push(`NAUTILUS_BIND_HOST=${HOST} — API 暴露于所有网卡`);
    }
    res.json({
        api_auth_required: API_AUTH_ENFORCED,
        alpha_symbols: DEFAULT_ALPHA_SYMBOLS,
        symbols: Object.keys(SYMBOL_MAP),
        bind_host: HOST,
        trading_env: TRADING_ENV,
        live_trading_allowed: LIVE_TRADING_ALLOWED,
        production_warnings: warnings,
    });
});

// GET /api/config/settings — 引擎生效配置（Redis config:auto + 环境变量只读）
app.get("/api/config/settings", async (req, res) => {
    try {
        const raw = await redis.get("config:auto");
        let engine = {};
        if (raw) {
            try { engine = JSON.parse(raw); } catch (_) { /* ignore */ }
        }
        const rawRecon = await redis.get("reconcile:startup");
        let lastReconcile = null;
        if (rawRecon) {
            try { lastReconcile = JSON.parse(rawRecon); } catch (_) { /* ignore */ }
        }
        res.json({
            env: {
                trading_env: TRADING_ENV,
                live_trading_allowed: LIVE_TRADING_ALLOWED,
                market_data_delayed: (process.env.MARKET_DATA_DELAYED || "0").trim(),
                market_data_mode: (process.env.MARKET_DATA_MODE || "realtime").trim(),
                auto_fixed_qty: parseInt(process.env.AUTO_FIXED_QTY || "0", 10) || 0,
                allow_fixed_qty: ["1", "true", "yes"].includes(
                    (process.env.ALLOW_FIXED_QTY || "").trim().toLowerCase(),
                ),
                alpha_super_only: (process.env.ALPHA_SUPER_ONLY || "1").trim(),
            },
            engine,
            last_startup_reconcile: lastReconcile,
            note: "修改 TRADING_ENV / MARKET_DATA_DELAYED / MARKET_DATA_MODE 等需改 .env 并重启引擎；risk 参数来自 AutoRunner 启动写入",
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /api/engine-status — 引擎心跳（ts 与当前时间差判定在线）
app.get("/api/engine-status", async (req, res) => {
    try {
        const raw = await redis.get("engine:heartbeat");
        const { heartbeat, age_s, online } = parseEngineHeartbeat(raw);
        res.json({
            engine_online: online,
            engine_heartbeat: heartbeat,
            engine_heartbeat_age_s: age_s,
            engine_heartbeat_max_age_s: ENGINE_HEARTBEAT_MAX_AGE_S,
        });
    } catch (e) {
        res.status(500).json({ error: e.message, engine_online: false });
    }
});


// GET /api/signals/touches/:symbol — 图表回踩触线标记（VWAP / M5 ST / DEMA20）
app.get("/api/signals/touches/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const rawList = await redis.lrange(`signals:markers:${symbol}`, 0, -1);
        const touches = rawList.map((s) => {
            try { return JSON.parse(s); } catch (_) { return null; }
        }).filter(Boolean);
        touches.sort((a, b) => (a.touch_time || 0) - (b.touch_time || 0));
        res.json({ symbol, count: touches.length, touches });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// REST API：获取所有数据（M3: 最多返回最近 500 根 K 线，避免大 JSON 打爆 Node）
const MAX_BARS = 500;
const RTH_OPEN_SEC = 9 * 3600 + 30 * 60;   // 09:30 ET
const RTH_CLOSE_SEC = 16 * 3600;            // 16:00 ET
const PREMARKET_CHART_START_SEC = 8 * 3600 + 30 * 60; // 08:30 ET（开盘前 60 分钟）

function isPremarketChartSec(secOfDay) {
    return secOfDay >= PREMARKET_CHART_START_SEC && secOfDay < RTH_OPEN_SEC;
}

function isChartSecOfDay(secOfDay) {
    return isPremarketChartSec(secOfDay)
        || (secOfDay >= RTH_OPEN_SEC && secOfDay < RTH_CLOSE_SEC);
}

function filterChartBars(bars) {
    return bars.filter(b => isChartSecOfDay(b.time % 86400));
}
const ALL_SYMBOLS = ["NVDA", "AAPL", "GOOG", "AVGO", "SPY", "TSLA", "PLTR", "AMZN", "AMD", "META", "MSFT", "QQQ", "TSM", "MU", "NFLX"];

app.get("/api/data/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const [m1List, m5List, posRaw, prevDayRaw] = await Promise.all([
            redis.lrange(`bars:1m:${symbol}`, 0, -1),
            redis.lrange(`bars:5m:${symbol}`, 0, -1),
            redis.get(`position:${symbol}`),
            redis.get(`prev_day:${symbol}`),   // 引擎启动时从日K写入
        ]);

        if (!m1List || m1List.length === 0) {
            return res.status(404).json({ error: `No data for ${symbol}. Run engine first.` });
        }

        const m1All = m1List.map(s => JSON.parse(s));
        const m5All = m5List ? m5List.map(s => JSON.parse(s)) : [];

        // 防御层：按时间戳去重（保留最后出现的），确保 LightweightCharts setData 时间严格递增
        function dedupBars(bars) {
            const map = new Map();
            bars.forEach(b => map.set(b.time, b));
            return Array.from(map.values()).sort((a, b) => a.time - b.time);
        }

        // 图表时段：盘前 60 分钟 + 正市
        // 计算昨日 H/L/C（从 5m bars 中筛选昨日 ET 日期数据）
        // ET fake-UTC：bars.time 已是 ET fake-UTC 秒
        function calcPrevDay(m5Bars) {
            if (!m5Bars.length) return null;
            const etOff = getETOffsetSec();
            const etNow = Date.now() / 1000 + etOff;
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
            m1_bars: filterChartBars(dedupBars(m1All)).slice(-MAX_BARS),
            m5_bars: filterChartBars(dedupBars(m5All)).slice(-MAX_BARS),
            // 含盘前预热：引擎 04:00 起算指标，供前端 ATR 等（不限于图表 60 分窗口）
            m1_atr_bars: dedupBars(m1All).slice(-120),
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

// ─── 日内高低突破信号状态（内存，进程存活期间有效）─────────────────────
// 结构：{ [symbol]: { dayKey, dayHigh, dayLow, score } }
// score: 创新高 +1 累计、创新低 -1 累计、无突破归零
const hlState = {};

// ET fake-UTC 的当日 dayKey 工具函数
function etDayKey() {
    return getETInfo().dayKey;
}

// 从 Redis 5m bars 重建今日 hlState（服务重启后恢复）
// 只参考盘中 RTH（09:30-16:00 ET）数据，不包含盘前数据
function calcHLScore(m5Bars) {
    if (!m5Bars || !m5Bars.length) return { score: 0, dayHigh: null, dayLow: null };
    const etOff = getETOffsetSec();
    const etNow = Date.now() / 1000 + etOff;
    const etMidnight = etNow - (etNow % 86400);
    const RTH_OPEN = 9 * 3600 + 30 * 60;  // 34200s = 09:30 ET
    const RTH_CLOSE = 16 * 3600;            // 57600s = 16:00 ET
    // 只保留今日 RTH 期间的 M5 bar，不含盘前盘后
    const todayBars = m5Bars.filter(b => {
        const secOfDay = b.time % 86400;
        return b.time >= etMidnight && secOfDay >= RTH_OPEN && secOfDay < RTH_CLOSE;
    });
    if (!todayBars.length) return { score: 0, dayHigh: null, dayLow: null };

    let dayHigh = -Infinity, dayLow = Infinity, score = 0;
    for (const bar of todayBars) {
        const c = bar.close;
        if (c > dayHigh) {
            dayHigh = c;
            score++;          // 创新高，累加
        } else if (c < dayLow) {
            dayLow = c;
            score--;          // 创新低，累减
        } else {
            score = 0;        // 既没创新高也没创新低，归零
        }
        // 更新低点（第一根 bar 建立基准）
        if (dayLow === Infinity) dayLow = c;
    }
    return { score, dayHigh, dayLow: dayLow === Infinity ? null : dayLow };
}

// 实时路径：每根 M5 bar 收盘后更新 hlState（从实时 kline:5m 推送触发）
function updateHLState(symbol, close) {
    const dayKey = etDayKey();
    if (!hlState[symbol] || hlState[symbol].dayKey !== dayKey) {
        // 新的一天，重置
        hlState[symbol] = { dayKey, dayHigh: close, dayLow: close, score: 1 };
    } else {
        const s = hlState[symbol];
        if (close > s.dayHigh) {
            s.dayHigh = close;
            s.score++;
        } else if (close < s.dayLow) {
            s.dayLow = close;
            s.score--;
        } else {
            s.score = 0;
        }
    }
    return hlState[symbol].score;
}

// ─── EMA 分类：根据过去6根 M5 bar 收盘价相对 EMA21/EMA9 的位置判断行情状态 ─────
// 返回：'rocket_bull' | 'bull' | 'mixed' | 'bear' | 'rocket_bear' | 'insufficient'
//
// 注：过滤条件只要求 ema21 != null，不强制 ema9 != null。
// 当 ema9 为 null（引擎刚启动期间初始根），allAboveEma9 / allBelowEma9 因为
// `b.ema9 != null && ...` 的二次检查会自动判定为 false，安全降级到
// bull / bear / mixed，不会产生误报，无需额外处理。
function calcEMAClassify(m5Bars) {
    // 取最后 6 根有 ema21 的 bar（ema9 允许部分为 null，见上方注释）
    const recent = m5Bars.filter(b => b.ema21 != null).slice(-6);
    if (recent.length < 6) return 'insufficient';

    // allAboveEma9 / allBelowEma9：ema9 为 null 的 bar 不满足条件，自动降级
    const allAboveEma9  = recent.every(b => b.ema9 != null && b.close > b.ema9);
    const allBelowEma9  = recent.every(b => b.ema9 != null && b.close < b.ema9);
    const allAboveEma21 = recent.every(b => b.close > b.ema21);
    const allBelowEma21 = recent.every(b => b.close < b.ema21);

    if (allAboveEma9)  return 'rocket_bull';  // 极速↑：6根全在 EMA9 上方
    if (allBelowEma9)  return 'rocket_bear';  // 极速↓：6根全在 EMA9 下方
    if (allAboveEma21) return 'bull';          // 多头：6根全在 EMA21 上方
    if (allBelowEma21) return 'bear';          // 空头：6根全在 EMA21 下方
    return 'mixed';                            // 震荡：混合状态
}

app.get("/api/indicators", async (req, res) => {
    try {
        const results = await Promise.all(ALL_SYMBOLS.map(async sym => {
            const [m1List, m5List] = await Promise.all([
                redis.lrange(`bars:1m:${sym}`, 0, -1),
                redis.lrange(`bars:5m:${sym}`, 0, -1),
            ]);
            if (!m1List || m1List.length === 0) return { symbol: sym, error: "no data" };

            const m1 = m1List.map(s => JSON.parse(s));
            const m5 = m5List ? m5List.map(s => JSON.parse(s)) : [];
            const lastM1 = m1[m1.length - 1];
            const lastM5 = m5.length ? m5[m5.length - 1] : null;

            // M1 ST 积分
            const stScoreM1 = calcSTScore(m1);
            // M5 ST 积分
            const stScoreM5 = m5.length ? calcSTScore(m5) : 0;
            // EMA 积分
            let emaScore = null;
            if (lastM5 && lastM5.ema21 != null) {
                const atr10 = calcATR10(m5);
                if (atr10 && atr10 > 0) {
                    emaScore = parseFloat(((lastM5.close - lastM5.ema21) / atr10).toFixed(3));
                }
            }

            // 高低突破信号（从内存恢复 or 从 bars 重建）
            let hlResult;
            if (hlState[sym]) {
                hlResult = hlState[sym];
            } else {
                hlResult = { ...calcHLScore(m5), dayKey: etDayKey() };
                hlState[sym] = hlResult;
            }

            return {
                symbol: sym,
                price: lastM1.close,
                st_score_m1: stScoreM1,
                st_score_m5: stScoreM5,
                ema_score: emaScore,
                mom_atr: (() => {
                    for (let i = m5.length - 1; i >= 0; i--) {
                        if (m5[i].mom_atr != null) return m5[i].mom_atr;
                    }
                    return null;
                })(),
                hl_score: hlResult.score ?? 0,  // 日内高低突破信号：+N=连续新高 -N=连续新低 0=震荡
                ema_diff_int: (() => {
                    // 取最后一根有 ema_diff_int 字段的 M5 bar
                    for (let i = m5.length - 1; i >= 0; i--) {
                        if (m5[i].ema_diff_int != null) return m5[i].ema_diff_int;
                    }
                    return null;
                })(),  // M5 (EMA9-EMA21) 1小时均值积分 / M5_ATR14
                ema_classify: calcEMAClassify(m5),  // 过去6根M5位置分类
                last_m1_time: lastM1 ? lastM1.time : null,   // 最新M1 bar ET fake-UTC秒
                last_m5_time: lastM5 ? lastM5.time : null,   // 最新M5 bar ET fake-UTC秒
                st_dir_m1: lastM1.st_dir,
                st_dir_m5: lastM5 ? lastM5.st_dir : null,
                st_val_m1: lastM1.st_value,
                ema21_m5: lastM5 ? lastM5.ema21 : null,
            };

        }));

        // ── 后处理：计算相对 QQQ 的动量背离分 ──────────────────────────
        // div_mom = mom_atr(stock) - mom_atr(QQQ)
        // 正值：该标的比 QQQ 相对强（QQQ 杀跌但该股抗跌/上涨）
        // 负值：该标的比 QQQ 相对弱
        const qqq = results.find(r => r.symbol === 'QQQ' && r.mom_atr != null);
        const qqqMom = qqq ? qqq.mom_atr : null;
        results.forEach(r => {
            r.div_mom = (r.mom_atr != null && qqqMom != null)
                ? parseFloat((r.mom_atr - qqqMom).toFixed(4))
                : null;
        });

        // 排序：优先按 M1 ST 积分降序
        results.sort((a, b) => (b.st_score_m1 ?? 0) - (a.st_score_m1 ?? 0));
        res.json(results);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});


// ── AutoPM 路由（SCOPE_FIXED §7）──────────────────────────────────────────
async function usesAutoPm(symbol) {
    try {
        const raw = await redis.get(`settings:${symbol}`);
        if (!raw) return false;
        const s = JSON.parse(raw);
        return !!s.auto_strategy;
    } catch {
        return false;
    }
}

async function routeAutoPmClose(symbol, reason = 'ui_close') {
    const payload = JSON.stringify({ reason, ts: Math.floor(Date.now() / 1000) });
    await redis.set(`auto:close:${symbol}`, payload, 'EX', 300);
    await redis.publish('auto:close', JSON.stringify({ symbol, reason }));
    logKey("AutoPM", "info", `平仓请求已写入 Redis auto:close:${symbol}`, { reason });
}



// ── 控制台进场（Entry Console）路由 ────────────────────────────────────
// 四种进场方式统一经 auto:enter:{sym} → AutoRunner → AutoPM（与 auto:close 同构）。

// POST /api/enter/:symbol — 发起进场（manual_limit / ema / st_limit / conditional）
app.post("/api/enter/:symbol", requireApiSecret, async (req, res) => {
    const symbol = String(req.params.symbol || "").toUpperCase();
    const body = req.body || {};
    if (!symbol) return res.status(400).json({ error: "缺少 symbol" });
    const method = String(body.method || "").toLowerCase();
    if (!["market", "manual_limit", "ema", "st_limit", "conditional"].includes(method)) {
        return res.status(400).json({ error: `未知进场方法: ${method}` });
    }
    if (!["LONG", "SHORT", "BUY", "SELL"].includes(String(body.side || "").toUpperCase())) {
        return res.status(400).json({ error: "缺少或非法 side（LONG/SHORT）" });
    }
    const payload = JSON.stringify({
        method,
        side: String(body.side || "").toUpperCase(),
        limit_price: body.limit_price != null ? Number(body.limit_price) : null,
        ema_period: body.ema_period != null ? Number(body.ema_period) : 20,
        st_field: body.st_field || "value",
        stop_price: body.stop_price != null ? Number(body.stop_price) : null,
        tp_rr: body.tp_rr != null ? Number(body.tp_rr) : 2.0,
        trigger: body.trigger || null,
        expire_ts: body.expire_ts || null,
        bypass_window: !!body.bypass_window,
        operator: body.operator || "console",
        ts: Math.floor(Date.now() / 1000),
    });
    try {
        await redis.set(`auto:enter:${symbol}`, payload, "EX", 300);
        await redis.publish("auto:enter", JSON.stringify({ symbol, method }));
        // 立即触发引擎消费（市价单低延迟进场，不等下一根 M1；通知失败则 M1 兜底）
        proxyToEngine("POST", "/enter-now", { symbol }).catch(() => {});
        logKey("Entry", "info", `进场请求已写入 auto:enter:${symbol}（已通知立即执行）`, { method });
        res.json({
            ok: true, routed: "auto_pm_enter", symbol, method,
            note: "已立即触发 AutoRunner 消费；经 auto:signal / entry:update 跟踪状态",
        });
    } catch (e) {
        logKey("Entry", "error", `auto:enter 写入失败 ${symbol}: ${e.message}`);
        res.status(500).json({ error: e.message });
    }
});

// GET /api/pending-entries?symbol= — 挂单进场（RESTING 限价 + ARMED/TRIGGERED 条件票）
app.get("/api/pending-entries", async (req, res) => {
    const symbol = String(req.query.symbol || "").toUpperCase();
    try {
        const ids = await redis.zrevrange("entry:pending:index", 0, 99);
        const out = [];
        for (const id of ids) {
            const t = await parseTicketHash(`entry:ticket:${id}`);
            if (!t) { await redis.zrem("entry:pending:index", id); continue; }
            if (!["ARMED", "RESTING", "TRIGGERED"].includes(String(t.state || ""))) {
                await redis.zrem("entry:pending:index", id); continue;
            }
            if (symbol && String(t.symbol || "").toUpperCase() !== symbol) continue;
            out.push(t);
        }
        res.json(out);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /api/entry-context/:symbol — 进场表单预览上下文（价位 / 账户 / 风控 / 熔断）
app.get("/api/entry-context/:symbol", async (req, res) => {
    const symbol = String(req.params.symbol || "").toUpperCase();
    try {
        let levels = {};
        try { const r = await redis.get(`indicators:active:${symbol}`); if (r) levels = JSON.parse(r); } catch {}
        let lastClose = 0;
        try {
            const barRaw = await redis.lindex(`bars:1m:${symbol}`, -1);
            if (barRaw) lastClose = JSON.parse(barRaw).close || 0;
        } catch {}
        if (!lastClose) {
            try {
                const posRaw = await redis.get(`position:${symbol}`);
                if (posRaw) lastClose = JSON.parse(posRaw).last_price || JSON.parse(posRaw).entry_price || 0;
            } catch {}
        }
        let equity = 0;
        try {
            const fundsRaw = await redis.get("account:funds");
            if (fundsRaw) {
                const bal = (JSON.parse(fundsRaw).balances || []).find(b => b.currency === "USD");
                equity = (bal && bal.total) || 0;
            }
        } catch {}
        let cfg = {};
        try { const c = await redis.get("config:auto"); if (c) cfg = JSON.parse(c); } catch {}
        let halted = false;
        try {
            const today = new Date().toLocaleDateString("en-CA", { timeZone: "America/New_York" });
            halted = !!(await redis.get(`risk:halt:${today}`));
        } catch {}
        res.json({
            symbol, last_close: lastClose,
            dema20: levels.dema20 || null,
            supertrend: levels.supertrend || null,
            atr: (levels.atr != null && levels.atr !== false) ? levels.atr : null,
            equity,
            risk_pct: cfg.risk_pct != null ? cfg.risk_pct : 0.002,
            max_position_pct: cfg.max_position_pct != null ? cfg.max_position_pct : 0.10,
            min_qty: cfg.min_qty || 1,
            fixed_qty: cfg.fixed_qty || 0,
            atr_mult: cfg.atr_mult || 1.5,
            tp_rr: cfg.tp_rr || 2.0,
            halted,
            trading_env: cfg.trading_env || TRADING_ENV,
            live_orders_allowed: cfg.live_orders_allowed != null ? cfg.live_orders_allowed : LIVE_TRADING_ALLOWED,
        });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// POST /api/entry-cancel — 撤单（按 ticket_id 或 entry_coid 寻址）
app.post("/api/entry-cancel", requireApiSecret, async (req, res) => {
    const body = req.body || {};
    if (!body.ticket_id && !body.entry_coid) {
        return res.status(400).json({ error: "需提供 ticket_id 或 entry_coid" });
    }
    try {
        await redis.lpush("entry:cmd", JSON.stringify({
            action: "cancel",
            ticket_id: body.ticket_id || "",
            entry_coid: body.entry_coid || "",
            symbol: String(body.symbol || "").toUpperCase(),
            reason: body.reason || "ui_cancel",
        }));
        res.json({ ok: true, routed: "entry_cmd" });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// POST /api/entry-modify — 改价（RESTING 限价 / ARMED 条件触发价）
app.post("/api/entry-modify", requireApiSecret, async (req, res) => {
    const body = req.body || {};
    if (body.price == null) return res.status(400).json({ error: "缺少 price" });
    if (!body.ticket_id && !body.entry_coid) {
        return res.status(400).json({ error: "需提供 ticket_id 或 entry_coid" });
    }
    try {
        await redis.lpush("entry:cmd", JSON.stringify({
            action: "modify",
            ticket_id: body.ticket_id || "",
            entry_coid: body.entry_coid || "",
            symbol: String(body.symbol || "").toUpperCase(),
            price: Number(body.price),
        }));
        res.json({ ok: true, routed: "entry_cmd" });
    } catch (e) { res.status(500).json({ error: e.message }); }
});

async function parseTicketHash(key) {
    const raw = await redis.hgetall(key);
    if (!raw || !Object.keys(raw).length) return null;
    const obj = {};
    for (const [k, v] of Object.entries(raw)) {
        try { obj[k] = JSON.parse(v); } catch { obj[k] = v; }
    }
    return obj;
}


// ── 引擎代理路由（转发到 order_actor :8888）─────────────────────────────

// symbol → NautilusTrader instrument_id 映射
const SYMBOL_MAP = {
    NVDA: 'NVDA.NASDAQ',
    AAPL: 'AAPL.NASDAQ',
    GOOG: 'GOOG.NASDAQ',
    AVGO: 'AVGO.NASDAQ',
    SPY: 'SPY.ARCA',
    TSLA: 'TSLA.NASDAQ',
    PLTR: 'PLTR.NYSE',
    AMZN: 'AMZN.NASDAQ',
    AMD: 'AMD.NASDAQ',
    META: 'META.NASDAQ',
    MSFT: 'MSFT.NASDAQ',
    QQQ: 'QQQ.NASDAQ',
    TSM: 'TSM.NYSE',
    MU: 'MU.NASDAQ',
    NFLX: 'NFLX.NASDAQ',
};

/**
 * 通用 HTTP 代理：向 order_actor 发 GET/POST 请求
 * 引擎未启动时返回 fallback 值，不抛错
 */
function proxyToEngine(method, path, body, fallback) {
    return new Promise((resolve) => {
        const http = require('http');
        const postData = body ? JSON.stringify(body) : null;
        const headers = { 'Content-Type': 'application/json' };
        if (ORDER_GATEWAY_SECRET) headers['X-Order-Token'] = ORDER_GATEWAY_SECRET;
        const opts = {
            host: '127.0.0.1', port: 8888, path, method,
            headers,
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

// POST /api/position/:symbol — 禁止幽灵仓位（SCOPE_FIXED §7）
app.post("/api/position/:symbol", requireApiSecret, async (req, res) => {
    return res.status(403).json({
        error: '禁止手动写入 position Redis',
        hint: '仓位仅由引擎成交后写入；请走 Alpha 审批或 TWS',
    });
});

// DELETE /api/position/:symbol — 平仓（AutoPM 路由 或 order_actor /close）
app.delete("/api/position/:symbol", requireApiSecret, async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();

    if (await usesAutoPm(symbol)) {
        await routeAutoPmClose(symbol, 'ui_close');
        console.log(`✅ 平仓路由 AutoPM [${symbol}]`);
        return res.json({
            ok: true,
            routed: 'auto_pm',
            note: '已路由 AutoPM.close_all，下一根 M1 内提交；UI 随成交确认更新',
        });
    }

    const instrumentId = SYMBOL_MAP[symbol];
    let engineResult = null;
    if (instrumentId) {
        engineResult = await proxyToEngine('POST', '/close', { symbol }, { engine_offline: true });
    }

    if (engineResult?.engine_offline) {
        console.warn(`⚠️  平仓失败 [${symbol}]: 引擎离线`);
        return res.status(503).json({
            ok: false,
            error: '引擎离线，无法平仓',
            hint: '请启动引擎 (python main.py) 或在 TWS 手动平仓',
        });
    }

    if (engineResult?.error) {
        console.warn(`⚠️  平仓失败 [${symbol}]: ${engineResult.error}`);
        return res.status(400).json({
            ok: false,
            error: engineResult.error,
            hint: 'IBKR 可能无该标的持仓，请核对 TWS',
        });
    }

    if (engineResult && instrumentId) {
        console.log(`✅ 平仓已提交 [${symbol}]: ${JSON.stringify(engineResult)}`);
        return res.json({
            ok: true,
            routed: 'order_actor',
            engine: engineResult,
            note: '平仓单已提交，UI 将在成交确认后清除仓位线',
        });
    }

    return res.status(400).json({
        ok: false,
        error: '未知标的或未配置 instrument_id',
    });
});

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

// GET /api/positions-fa — FA 全账户持仓（reqPositions 直查 IBKR，覆盖子账户，补 cache 盲区）
// 控制台持仓显示用此端点：cache.positions_open() 在 FA 分配后会丢子账户仓位
app.get('/api/positions-fa', async (req, res) => {
    const data = await proxyToEngine('GET', '/positions-fa', null, []);
    res.json(data);
});

// GET /api/positions-redis — 轻量持仓快照（直接读 Redis position:*，不拉 K 线）
// 供 Status Bar 在引擎离线时快速统计四格持仓，避免对 /api/data/:sym 全量轮询
app.get('/api/positions-redis', async (req, res) => {
    try {
        const want = (req.query.symbols || '')
            .split(',').map(s => s.trim().toUpperCase()).filter(Boolean);
        const symbols = want.length ? want : Object.keys(SYMBOL_MAP);
        const keys = symbols.map(s => `position:${s}`);
        const raws = await redis.mget(keys);
        const out = {};
        symbols.forEach((sym, i) => {
            if (!raws[i]) return;
            try {
                const pos = JSON.parse(raws[i]);
                if (pos && Math.abs(pos.quantity || 0) > 0) out[sym] = pos;
            } catch (_) { /* skip malformed */ }
        });
        res.json(out);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /api/active-orders — 当前活跃订单（入场价 + 止损价），供前端恢复价格线
app.get('/api/active-orders', async (req, res) => {
    const data = await proxyToEngine('GET', '/active-orders', null, {});
    res.json(data);
});

// GET /api/auto-config — 生效的自动交易风控配置（供前端护栏：fixed_qty>0 弹警告）
app.get('/api/auto-config', async (req, res) => {
    try {
        const raw = await redis.get('config:auto');
        res.json(raw ? JSON.parse(raw) : {});
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// POST /api/close-all — 一键全平所有持仓（kill switch，代理引擎 /close-all）
// 注意：引擎侧会同时触发当日熔断（禁止再开仓），这是预期的紧急行为
app.post('/api/close-all', requireApiSecret, async (req, res) => {
    const result = await proxyToEngine('POST', '/close-all', {}, { engine_offline: true });
    if (result?.engine_offline) {
        return res.status(503).json({
            ok: false,
            error: '引擎离线，无法全平',
            hint: '请启动引擎 (python main.py) 或在 TWS 手动平仓',
        });
    }
    if (result?.error) {
        return res.status(400).json({ ok: false, error: result.error });
    }
    res.json({ ok: true, engine: result, note: '全平单已提交，熔断已激活；Redis 仓位随成交确认更新' });
});

// POST /api/order/:symbol — 禁止手动开仓（仅 Agent 审批后可由 AutoRunner 下单）
app.post('/api/order/:symbol', requireApiSecret, async (req, res) => {
    return res.status(403).json({
        error: '系统禁止手动开仓',
        hint: '请通过 Alpha 建议审批流程，批准实盘后由 Agent 自动执行',
    });
});

// POST /api/modify-stop/:symbol — 修改止损价（代理引擎 + 更新 Redis position）
app.post('/api/modify-stop/:symbol', requireApiSecret, async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const { price } = req.body;
    if (price == null) {
        return res.status(400).json({ error: 'price 必填' });
    }

    if (await usesAutoPm(symbol)) {
        return res.status(409).json({
            ok: false,
            error: 'AutoPM 接管标的禁止 HTTP 改止损',
            hint: '请关闭 Agent 执行，或仅用 trail_mode 管理手动仓位',
        });
    }

    // 1. 调引擎修改止损单触发价
    const result = await proxyToEngine('POST', '/modify-stop', { symbol, price: parseFloat(price) },
        { engine_offline: true });

    if (result.engine_offline || result.error) {
        return res.json({ ok: false, error: result.error || '引擎未启动' });
    }

    // 2. 同步更新 Redis position 中的 stop_loss 字段
    try {
        const raw = await redis.get(`position:${symbol}`);
        if (raw) {
            const pos = JSON.parse(raw);
            pos.stop_loss = parseFloat(price);
            await redis.set(`position:${symbol}`, JSON.stringify(pos));
        }
    } catch (e) {
        console.warn(`[modify-stop] Redis 更新失败: ${e.message}`);
    }

    console.log(`✅ 止损修改 [${symbol}]: ${JSON.stringify(result)}`);
    res.json({ ok: true, engine: result });
});

// POST /api/cancel-entry/:symbol — 取消挂单中的限价入场单
app.post('/api/cancel-entry/:symbol', requireApiSecret, async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const { client_order_id } = req.body;
    if (!client_order_id) {
        return res.status(400).json({ error: 'client_order_id 必填' });
    }
    const result = await proxyToEngine('POST', '/cancel-entry', { symbol, client_order_id },
        { engine_offline: true });
    if (result.engine_offline || result.error) {
        return res.json({ ok: false, error: result.error || '引擎未启动' });
    }
    console.log(`✅ 取消限价入场单 [${symbol}] ${client_order_id}`);
    res.json({ ok: true, engine: result });
});

// POST /api/modify-entry/:symbol — 修改挂单限价入场单价格
app.post('/api/modify-entry/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const { client_order_id, price } = req.body;
    if (!client_order_id || price == null) {
        return res.status(400).json({ error: 'client_order_id 和 price 必填' });
    }
    const result = await proxyToEngine('POST', '/modify-entry', { symbol, client_order_id, price: parseFloat(price) },
        { engine_offline: true });
    if (result.engine_offline || result.error) {
        return res.json({ ok: false, error: result.error || '引擎未启动' });
    }
    console.log(`✅ 修改限价入场单 [${symbol}] ${client_order_id} → ${price}`);
    res.json({ ok: true, engine: result });
});

// GET /api/risk — 今日风险状态（代理引擎）
app.get("/api/risk", async (req, res) => {
    try {
        const data = await proxyToEngine("GET", "/risk");
        res.json(data);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ─── Alpha Agent 交易建议（审批流）────────────────────────────────────

// Redis key 保留时长（默认 8h），与建议 expires_at 交易有效期无关
const PROPOSAL_REDIS_RETENTION = parseInt(
    process.env.PROPOSAL_REDIS_RETENTION_SECONDS || String(8 * 3600), 10
);

const PROPOSAL_INDEX = {
    pending: "proposal:pending:index",
    approved: "proposal:approved:index",
    rejected: "proposal:rejected:index",
    executed: "proposal:executed:index",
};

async function parseProposalHash(key) {
    const raw = await redis.hgetall(key);
    if (!raw || !Object.keys(raw).length) return null;
    const obj = {};
    for (const [k, v] of Object.entries(raw)) {
        try { obj[k] = JSON.parse(v); } catch { obj[k] = v; }
    }
    return obj;
}

async function listProposalsByStatus(status, { symbol, limit = 50, executionPhase = null } = {}) {
    const indexKey = PROPOSAL_INDEX[status];
    if (!indexKey) return [];
    const ids = await redis.zrevrange(indexKey, 0, limit - 1);
    const out = [];
    for (const id of ids) {
        const p = await parseProposalHash(`proposal:${status}:${id}`);
        if (!p) {
            await redis.zrem(indexKey, id);
            continue;
        }
        if (symbol && String(p.symbol || "").toUpperCase() !== symbol.toUpperCase()) continue;
        if (executionPhase && String(p.execution_phase || "") !== executionPhase) continue;
        out.push(p);
    }
    return out;
}

// GET /api/stack-health — 策略栈轻量健康检查（监控用）
app.get("/api/stack-health", async (req, res) => {
    const out = { ok: true, checks: {}, ts: Math.floor(Date.now() / 1000) };
    try {
        await redis.ping();
        out.checks.redis = { ok: true };
    } catch (e) {
        out.ok = false;
        out.checks.redis = { ok: false, error: e.message };
    }
    try {
        const pending = await redis.zcard(PROPOSAL_INDEX.pending);
        out.checks.proposals = { ok: true, pending };
    } catch (e) {
        out.checks.proposals = { ok: false, error: e.message };
    }
    res.json(out);
});

// GET /api/proposals?status=pending|approved|rejected|executed&symbol=QQQ&execution_phase=approved_wait
app.get("/api/proposals", async (req, res) => {
    try {
        const status = req.query.status || "pending";
        const symbol = req.query.symbol ? req.query.symbol.toUpperCase() : null;
        const executionPhase = req.query.execution_phase || null;
        const limit = Math.min(parseInt(req.query.limit || "50", 10), 200);
        const proposals = await listProposalsByStatus(status, { symbol, limit, executionPhase });
        res.json({ status, count: proposals.length, proposals });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /api/proposals/:id
app.get("/api/proposals/:id", async (req, res) => {
    const id = req.params.id;
    try {
        for (const st of ["pending", "approved", "rejected", "executed"]) {
            const p = await parseProposalHash(`proposal:${st}:${id}`);
            if (p) return res.json({ status: st, proposal: p });
        }
        res.status(404).json({ error: "proposal not found" });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

/** 批准时联动 Agent执行，避免 approved_live 与 auto_strategy 双开关静默失败 */
async function linkAgentExecutionOnApproval(symbol, decision) {
    const sym = String(symbol || "").toUpperCase();
    if (!sym || !decision.startsWith("approved")) {
        return null;
    }
    const raw = await redis.get(`settings:${sym}`);
    const settings = raw ? JSON.parse(raw) : {};
    const prev = {
        auto_strategy: !!settings.auto_strategy,
        auto_observe: !!settings.auto_observe,
    };
    if (decision === "approved_live") {
        settings.auto_strategy = true;
        settings.auto_observe = false;
    } else if (decision === "approved_observe") {
        settings.auto_observe = true;
        settings.auto_strategy = false;
    } else {
        return null;
    }
    settings.trail_mode = 0;
    await redis.set(`settings:${sym}`, JSON.stringify(settings));
    const mode = decision === "approved_live" ? "live" : "observe";
    const changed = prev.auto_strategy !== !!settings.auto_strategy
        || prev.auto_observe !== !!settings.auto_observe;
    console.log(`⚙️  批准联动 Agent执行 [${sym}] → ${mode}`, settings);
    logKey("Approval", "info", `联动 Agent执行 ${sym} → ${mode}`, { changed, previous: prev });
    return { symbol: sym, mode, changed, settings, previous: prev };
}

async function applyProposalDecision(id, decision, approver = "operator", comment = "") {
    const valid = ["approved_live", "approved_observe", "rejected"];
    if (!valid.includes(decision)) {
        throw new Error(`decision 必须是 ${valid.join("|")}`);
    }
    if (decision.startsWith("approved")) {
        const rawHb = await redis.get("engine:heartbeat");
        const { online } = parseEngineHeartbeat(rawHb);
        if (!online) {
            logKey("Approval", "warn", `拒绝批准 id=${id}`, "引擎离线");
            const err = new Error("引擎离线，暂不允许批准（避免重启后意外自动执行）");
            err.statusCode = 503;
            throw err;
        }
    }
    if (decision === "approved_live" && !LIVE_TRADING_ALLOWED) {
        logKey("Approval", "warn", `拒绝实盘批准 id=${id}`, `TRADING_ENV=${TRADING_ENV}`);
        const err = new Error(
            `TRADING_ENV=${TRADING_ENV}，禁止批准实盘。请在 .env 设置 TRADING_ENV=live 并重启引擎`
        );
        err.statusCode = 403;
        throw err;
    }
    const keyPending = `proposal:pending:${id}`;
    const proposal = await parseProposalHash(keyPending);
    if (!proposal) {
        logKey("Approval", "warn", `建议不存在 id=${id} decision=${decision}`);
        const err = new Error("待审批建议不存在或已处理");
        err.statusCode = 404;
        throw err;
    }
    const now = Math.floor(Date.now() / 1000);
    const isApproved = decision.startsWith("approved");
    if (isApproved) {
        const exp = parseInt(proposal.expires_at || "0", 10);
        if (exp && now > exp) {
            logKey("Approval", "warn", `拒绝批准 id=${id}`, "建议已过期");
            const err = new Error("建议已过期，无法批准");
            err.statusCode = 410;
            throw err;
        }
    }
    let agentExec = null;
    if (isApproved) {
        agentExec = await linkAgentExecutionOnApproval(proposal.symbol, decision);
    }
    const payload = {
        ...proposal,
        status: isApproved ? "approved" : "rejected",
        decision,
        approver,
        comment,
        decided_at: now,
    };
    if (isApproved) {
        payload.execution_phase = "approved_wait";
        payload.approved_at = now;
        if (payload.execution_mode === "st_super_immediate") {
            payload.execution_phase = "ready_to_execute";
            payload.reclaim_note = "超级信号：审批通过，立即市价下单";
            if (agentExec?.mode === "live") {
                payload.reclaim_note += "（已自动开启 Agent执行）";
            }
        } else if (payload.touch_reclaimed_at_submit) {
            payload.reclaim_note = "触线当根已收回，仍由执行层确认后下单";
        } else {
            payload.reclaim_note = payload.reclaim_label || "等待回踩 reclaim 完成后执行";
        }
        if (agentExec) {
            payload.agent_exec = agentExec;
        }
    }
    const targetStatus = payload.status;
    const targetKey = `proposal:${targetStatus}:${id}`;
    const targetIndex = PROPOSAL_INDEX[targetStatus];
    const hashMapping = {};
    for (const [k, v] of Object.entries(payload)) {
        hashMapping[k] = JSON.stringify(v);
    }
    const pipe = redis.pipeline();
    pipe.hset(targetKey, hashMapping);
    pipe.expire(targetKey, PROPOSAL_REDIS_RETENTION);
    pipe.zadd(targetIndex, now, id);
    pipe.del(keyPending);
    pipe.zrem(PROPOSAL_INDEX.pending, id);
    pipe.publish("proposal:update", JSON.stringify({
        event: decision,
        proposal_id: id,
        symbol: payload.symbol,
        side: payload.side,
        signal_type: payload.signal_type,
        decision,
        approver,
        agent_exec: agentExec,
    }));
    await pipe.exec();
    logKey("Approval", "info", `建议审批成功 ${payload.symbol} ${decision}`, {
        id, approver, phase: payload.execution_phase, agent_exec: agentExec?.mode,
    });
    if (
        isApproved
        && payload.execution_phase === "ready_to_execute"
        && (decision === "approved_live" || decision === "approved_observe")
    ) {
        const execResult = await proxyToEngine("POST", "/execute-proposal", {
            symbol: payload.symbol,
            proposal_id: id,
        }, { engine_offline: true });
        if (execResult?.engine_offline || execResult?.error) {
            logKey("Approval", "warn", `立即执行未送达引擎 ${payload.symbol}`, execResult);
        } else {
            logKey("Approval", "info", `立即执行已触发 ${payload.symbol}`, execResult);
        }
        payload.immediate_exec = execResult;
    }
    return payload;
}

async function applyProposalCancel(id, approver = "operator", comment = "") {
    const keyApproved = `proposal:approved:${id}`;
    const proposal = await parseProposalHash(keyApproved);
    if (!proposal) {
        const err = new Error("已批准建议不存在或已处理");
        err.statusCode = 404;
        throw err;
    }
    const phase = String(proposal.execution_phase || "");
    if (proposal.executed_at) {
        const err = new Error("已执行，无法取消");
        err.statusCode = 400;
        throw err;
    }
    if (!["approved_wait", "ready_to_execute"].includes(phase)) {
        const err = new Error(`当前阶段不可取消: ${phase || "unknown"}`);
        err.statusCode = 400;
        throw err;
    }
    const now = Math.floor(Date.now() / 1000);
    const payload = {
        ...proposal,
        status: "rejected",
        decision: "rejected",
        execution_phase: "cancelled",
        approver,
        comment: comment || "用户取消批准",
        cancelled_at: now,
        decided_at: proposal.decided_at || now,
    };
    const hashMapping = {};
    for (const [k, v] of Object.entries(payload)) {
        hashMapping[k] = JSON.stringify(v);
    }
    const pipe = redis.pipeline();
    pipe.hset(`proposal:rejected:${id}`, hashMapping);
    pipe.expire(`proposal:rejected:${id}`, PROPOSAL_REDIS_RETENTION);
    pipe.zadd(PROPOSAL_INDEX.rejected, now, id);
    pipe.del(keyApproved);
    pipe.zrem(PROPOSAL_INDEX.approved, id);
    pipe.del(`proposal:exec_claim:${id}`);
    pipe.publish("proposal:update", JSON.stringify({
        event: "cancelled",
        proposal_id: id,
        symbol: payload.symbol,
        side: payload.side,
        operator: approver,
    }));
    await pipe.exec();
    logKey("Approval", "info", `建议取消批准 ${payload.symbol}`, { id, approver });
    return payload;
}

// POST /api/proposals/:id/decision
// body: { decision: approved_live|approved_observe|rejected, approver?, comment? }
app.post("/api/proposals/:id/decision", requireApiSecret, async (req, res) => {
    const id = req.params.id;
    const { decision, approver = "operator", comment = "" } = req.body || {};
    try {
        const payload = await applyProposalDecision(id, decision, approver, comment);
        res.json({
            ok: true,
            proposal: payload,
            agent_exec: payload.agent_exec || null,
        });
    } catch (e) {
        logKey("Approval", e.statusCode >= 500 ? "error" : "warn",
            `审批 API 失败 id=${id} decision=${decision}`, e.message);
        res.status(e.statusCode || 500).json({ error: e.message });
    }
});

// POST /api/proposals/:id/cancel — 撤销已批准（approved_wait / ready_to_execute）
app.post("/api/proposals/:id/cancel", requireApiSecret, async (req, res) => {
    const id = req.params.id;
    const { approver = "operator", comment = "" } = req.body || {};
    try {
        const payload = await applyProposalCancel(id, approver, comment);
        res.json({ ok: true, proposal: payload });
    } catch (e) {
        logKey("Approval", "warn", `取消批准失败 id=${id}`, e.message);
        res.status(e.statusCode || 500).json({ error: e.message });
    }
});

// POST /api/feishu/webhook — 飞书事件订阅 + 卡片按钮回调
app.post("/api/feishu/webhook", async (req, res) => {
    const body = req.body || {};

    // URL 验证（旧版 challenge 或 schema 2.0）
    if (body.challenge) {
        return res.json({ challenge: body.challenge });
    }
    const eventType = body.header?.event_type || body.type;
    if (eventType === "url_verification") {
        const challenge = body.event?.challenge || body.challenge;
        return res.json({ challenge });
    }

    if (FEISHU_REQUIRE_TOKEN && !FEISHU_VERIFICATION_TOKEN) {
        return res.status(503).json({ error: "FEISHU_VERIFICATION_TOKEN 未配置" });
    }
    if (FEISHU_VERIFICATION_TOKEN) {
        const token = body.header?.token || body.token;
        if (!token || token !== FEISHU_VERIFICATION_TOKEN) {
            return res.status(403).json({ error: "invalid verification token" });
        }
    } else if (NAUTILUS_API_SECRET && eventType === "card.action.trigger") {
        return res.status(403).json({ error: "飞书审批需配置 FEISHU_VERIFICATION_TOKEN" });
    }

    if (eventType === "card.action.trigger") {
        const action = body.event?.action || body.action || {};
        const value = action.value || {};
        const proposalId = value.proposal_id;
        const decision = value.decision;
        const operator = body.event?.operator?.open_id || body.operator?.open_id || "feishu";
        if (!proposalId || !decision) {
            return res.json({
                toast: { type: "warning", content: "无效回调参数" },
            });
        }
        try {
            const payload = await applyProposalDecision(
                proposalId, decision, `feishu:${operator}`, "feishu card"
            );
            const label = {
                approved_live: "已批准实盘",
                approved_observe: "已批准观察",
                rejected: "已驳回",
            }[decision] || "已处理";
            return res.json({
                toast: { type: "info", content: `${label} ${payload.symbol || ""}`.trim() },
            });
        } catch (e) {
            return res.json({
                toast: { type: "error", content: e.message || "审批失败" },
            });
        }
    }

    // 飞书 IM 消息 → 异步调用 Cursor Agent（须 3 秒内 HTTP 响应）
    if (eventType === "im.message.receive_v1" && FEISHU_AGENT_ENABLED) {
        try {
            spawnFeishuAgentJob(body);
        } catch (e) {
            console.error(`[FeishuAgent] queue failed: ${e.message}`);
        }
        return res.json({});
    }

    res.json({ ok: true });
});

// GET /api/settings/:symbol — 读取当前策略开关设置
// POST /api/proposals/clear — 一键清除所有 pending 信号提案（pending → rejected）
app.post("/api/proposals/clear", requireApiSecret, async (req, res) => {
    const operator = String((req.body && req.body.operator) || "console").slice(0, 32);
    const reason = String((req.body && req.body.reason) || "console_clear").slice(0, 64);
    try {
        const ids = await redis.zrevrange(PROPOSAL_INDEX.pending, 0, 499);
        const now = Math.floor(Date.now() / 1000);
        let cleared = 0;
        for (const id of ids) {
            const p = await parseProposalHash(`proposal:pending:${id}`);
            const pipe = redis.pipeline();
            if (p) {
                const payload = { ...p, status: "rejected", decision: "rejected",
                    approver: operator, comment: reason, decided_at: now, purge_reason: reason };
                for (const [k, v] of Object.entries(payload)) {
                    pipe.hset(`proposal:rejected:${id}`, k, JSON.stringify(v));
                }
                pipe.expire(`proposal:rejected:${id}`, PROPOSAL_REDIS_RETENTION);
                pipe.zadd(PROPOSAL_INDEX.rejected, now, id);
            }
            pipe.del(`proposal:pending:${id}`);
            pipe.zrem(PROPOSAL_INDEX.pending, id);
            pipe.publish("proposal:update", JSON.stringify({
                event: "rejected", proposal_id: id, symbol: p && p.symbol, reason, operator,
            }));
            await pipe.exec();
            cleared++;
        }
        logKey("Approval", "info", `一键清除 pending 建议: ${cleared} 个`, { operator });
        res.json({ ok: true, cleared });
    } catch (e) {
        logKey("Approval", "error", `一键清除失败: ${e.message}`);
        res.status(500).json({ error: e.message });
    }
});

app.get("/api/settings/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const raw = await redis.get(`settings:${symbol}`);
        res.json(raw ? JSON.parse(raw) : { trail_mode: 0 });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// POST /api/settings/:symbol — ST 跟踪止盈等开关

app.post("/api/settings/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    const existing = await redis.get(`settings:${symbol}`);
    const settings = existing ? JSON.parse(existing) : {};
    Object.assign(settings, req.body);
    // 自动策略开启时强制关闭 ExitManager 跟踪止盈（后端兜底，防 Redis 直写）
    if (req.body.auto_strategy || req.body.auto_observe) {
        settings.trail_mode = 0;
    }
    await redis.set(`settings:${symbol}`, JSON.stringify(settings));
    console.log(`⚙️  设置更新 [${symbol}]:`, settings);

    // trail_mode 直接写 Redis 即可：ExitManager 每根 bar 从 Redis 读取，无需同步引擎内存
    res.json({ ok: true, settings });
});

// GET /api/premarket-ref/:symbol — 盘前锚定（开盘突破）
app.get("/api/premarket-ref/:symbol", async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const raw = await redis.get(`premarket:ref:${symbol}`);
        if (!raw) return res.json(null);
        res.json(JSON.parse(raw));
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// ── 交易日志 / 绩效（落库自 journal.js，独立于 Redis 过期）──────────────
// GET /api/journal/stats — 总体 + 按信号类型的胜率/期望/平均R/滑点
app.get("/api/journal/stats", (_req, res) => {
    try { res.json(journal.getStats()); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/equity — 资金曲线（累计已实现盈亏）
app.get("/api/journal/equity", (_req, res) => {
    try { res.json(journal.getEquityCurve()); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/trades?limit=100 — 最近往返成交
app.get("/api/journal/trades", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || "100", 10) || 100, 1000);
    try { res.json(journal.getTrades(limit)); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/rdist — R 倍数分布直方图
app.get("/api/journal/rdist", (_req, res) => {
    try { res.json(journal.getRDistribution()); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/decisions — 决策复盘聚合（漏斗/转化率/驳回原因）
app.get("/api/journal/decisions", (_req, res) => {
    try { res.json(journal.getDecisionStats()); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/proposals?limit=200 — 审批决策档案
app.get("/api/journal/proposals", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || "200", 10) || 200, 2000);
    try { res.json(journal.getProposals(limit)); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/timeline?proposal_id=&symbol=&limit= — 单提案全链路时间线
app.get("/api/journal/timeline", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || "500", 10) || 500, 5000);
    try {
        res.json(journal.getTimeline({
            proposal_id: req.query.proposal_id || null,
            symbol: req.query.symbol || null,
            limit,
        }));
    } catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/day?date=YYYY-MM-DD — 按 ET 交易日审计（默认今日）
app.get("/api/journal/day", (req, res) => {
    try { res.json(journal.getDayAudit(req.query.date || null)); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/orders?limit= — 最近订单事件
app.get("/api/journal/orders", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || "200", 10) || 200, 2000);
    try { res.json(journal.getOrders(limit)); }
    catch (e) { res.status(500).json({ error: e.message }); }
});

// GET /api/journal/signals?limit= — 最近信号事件
app.get("/api/journal/signals", (req, res) => {
    const limit = Math.min(parseInt(req.query.limit || "200", 10) || 200, 2000);
    try { res.json(journal.getSignals(limit)); }
    catch (e) { res.status(500).json({ error: e.message }); }
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
    console.log(`[✅ Redis订阅] 已连接，订阅 bars:1m:* / bars:5m:* / kline:1m:* / kline:5m:* / position:* / order:* / account:* / engine:* / auto:* / proposal:* / signal:* / premarket:* / entry:*`);
    redisSub.psubscribe(
        "bars:1m:*", "bars:5m:*", "kline:1m:*", "kline:5m:*",
        "position:*", "order:*", "account:*", "engine:*",
        "auto:*", "risk:*", "proposal:*", "signal:*",
        "premarket:*", "entry:*",
    ).catch(console.error);
});

// 判断当前时刻是否在美股正市（09:30-16:00 ET）
function isRTH() {
    const { secOfDay } = getETInfo();
    return secOfDay >= RTH_OPEN_SEC && secOfDay < RTH_CLOSE_SEC;
}

// 图表实时推送窗口：盘前 60 分 + 正市
function isChartLiveSession() {
    const { secOfDay } = getETInfo();
    return secOfDay >= PREMARKET_CHART_START_SEC && secOfDay < RTH_CLOSE_SEC;
}

redisSub.on("pmessage", (_pattern, channel, message) => {
    try {
        const parsed = JSON.parse(message);

        // auto:signal → 落库 + 前端广播
        if (channel === 'auto:signal') {
            try { journal.recordAutoSignal(parsed); } catch (e) { console.error(`[journal] auto:signal: ${e.message}`); }
            const autoPayload = JSON.stringify({ channel: 'auto:signal', data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(autoPayload));
            return;
        }

        // order:update → 落库（Redis 归因 proposal_id）+ 默认广播
        if (channel === 'order:update') {
            void (async () => {
                try {
                    const coid = parsed.client_order_id;
                    if (coid && !parsed.proposal_id) {
                        const pid = await redis.get(`journal:coid:${coid}`);
                        if (pid) parsed.proposal_id = pid;
                    }
                    journal.recordOrderUpdate(parsed);
                } catch (e) { console.error(`[journal] order:update: ${e.message}`); }
            })();
        }

        // risk:update → 日亏损熔断通知
        if (channel === 'risk:update') {
            const riskPayload = JSON.stringify({ channel: 'risk:update', data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(riskPayload));
            return;
        }

        // proposal:update → Alpha Agent 建议新增/审批/执行
        if (channel === 'proposal:update') {
            try { journal.recordProposalUpdate(parsed); } catch (e) { console.error(`[journal] proposal: ${e.message}`); }
            const proposalPayload = JSON.stringify({ channel: 'proposal:update', data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(proposalPayload));
            return;
        }

        // entry:update → 控制台进场票据状态变更（armed/triggered/filled/canceled/expired/modified/observed）
        if (channel === 'entry:update') {
            const entryPayload = JSON.stringify({ channel: 'entry:update', data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(entryPayload));
            return;
        }

        // position:update → 落库（持仓快照缓存 + 平仓结算往返交易）
        if (channel === 'position:update') {
            try { journal.recordPositionUpdate(parsed); } catch (e) { console.error(`[journal] position: ${e.message}`); }
            // 不 return：仍走默认广播逻辑推给前端
        }

        // signal:touch / signal:touch:backfill → 落库 + 图表标记
        if (channel === 'signal:touch' || channel === 'signal:touch:backfill') {
            try { journal.recordSignalTouch(parsed, channel); } catch (e) { console.error(`[journal] signal:touch: ${e.message}`); }
            const touchPayload = JSON.stringify({ channel, data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(touchPayload));
            return;
        }

        // premarket:ref → 盘前锚定线 UI
        if (channel.startsWith('premarket:ref:')) {
            const refPayload = JSON.stringify({ channel, data: parsed });
            wss.clients.forEach(c => c.readyState === 1 && c.send(refPayload));
            return;
        }

        // kline:5m: 收盘事件 → 更新日内高低突破信号并广播（仅正市期间）
        if (channel.startsWith('kline:5m:')) {
            const sym = channel.split(':')[2];
            if (sym && ALL_SYMBOLS.includes(sym) && isRTH()) {
                const score = updateHLState(sym, parsed.close);
                // 广播 hl:update 事件给前端（用于语音播报）
                const hlPayload = JSON.stringify({
                    channel: 'hl:update',
                    data: { symbol: sym, score, close: parsed.close },
                });
                wss.clients.forEach(c => c.readyState === 1 && c.send(hlPayload));
            }
        }


        // 盘前/盘后：拦截 kline 和 bars 实时推送（盘前 08:30 起放行）
        const isBarChannel = channel.startsWith('kline:') || channel.startsWith('bars:');
        if (isBarChannel && !isChartLiveSession()) {
            return;
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

server.on("error", (err) => {
    if (err.code === "EADDRINUSE") {
        console.error(`[启动失败] 端口 ${PORT} 仍被占用，请检查: lsof -i :${PORT}`);
    } else {
        console.error(`[启动失败] ${err.message}`);
    }
    process.exit(1);
});

server.listen(PORT, HOST, () => {
    const urlHost = HOST === "0.0.0.0" || HOST === "::" ? "localhost" : HOST;
    printBanner(`SUCCESS: 前端已启动 http://${urlHost}:${PORT}  PID=${process.pid}`);
    logKey("Config", "info", "前端配置", {
        TRADING_ENV,
        live_trading_allowed: LIVE_TRADING_ALLOWED,
        bind: `${HOST}:${PORT}`,
        alpha_symbols: DEFAULT_ALPHA_SYMBOLS,
    });
    console.log(`   pidfile=${PID_FILE}`);
    console.log(`   监听频道: bars:1m:* / bars:5m:* / signal:*`);
});
