/**
 * TrendStateMachine 单元测试（node 版，照 scripts/test_supertrend.py 范式）。
 *
 * 状态机输入是已算好的指标快照（st_dir / dc_* / ema20 / atr），测试直接喂指标
 * 序列驱动四态，无需模拟 K 线重算指标。覆盖：预热 / S1 进入 / S2 回踩 / S3 突破 /
 * S3 退出 / 空头镜像 / S4 双底反转 / setState 手动修正 / replay 可追溯。
 *
 * 运行：node scripts/test_trend_state.js
 */
'use strict';
const assert = require('assert');
const { TrendStateMachine } = require('../frontend/public/shared/trend-state-machine.js');

const P = TrendStateMachine.PHASE;

/** 构造一根 M5 bar（指标快照）。close 默认 open=high±0.5=low±0.5。 */
function bar(t, close, o = {}) {
  const h = o.high != null ? o.high : close + 0.5;
  const l = o.low != null ? o.low : close - 0.5;
  return {
    time: t, open: close, high: h, low: l, close,
    st_dir: o.st_dir || 0,
    dc_upper: o.dc_upper != null ? o.dc_upper : null,
    dc_lower: o.dc_lower != null ? o.dc_lower : null,
    dc_mid: o.dc_mid != null ? o.dc_mid : null,
    ema20: o.ema20 != null ? o.ema20 : null,
    atr: o.atr != null ? o.atr : null,
  };
}

let passed = 0;
function ok(name, cond) {
  if (!cond) throw new Error(`FAIL: ${name}`);
  console.log(`  ✓ ${name}`);
  passed++;
}

function test_prewarm() {
  const sm = new TrendStateMachine();
  for (let i = 0; i < 5; i++) {
    const r = sm.update(bar(i, 100, {}));  // 全 null → 预热
    assert.strictEqual(r.phase, P.UNKNOWN);
  }
  ok('预热期（DC/ST/EMA 任一缺失）phase=unknown', true);
}

function test_s1_first_trend_bull() {
  const sm = new TrendStateMachine();
  sm.update(bar(1, 96, { st_dir: -1, dc_upper: 100, dc_lower: 95, dc_mid: 97.5, ema20: 98, atr: 1 }));  // 设 prev
  const r = sm.update(bar(2, 101, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1, high: 101.5 }));
  assert.strictEqual(r.phase, P.FIRST_TREND);
  assert.strictEqual(r.direction, 1);
  assert.strictEqual(r.changed, true);
  ok('S1 进入：ST 翻多 + 突破 DC 上轨（多头）', true);
  return sm;
}

function test_s2_pullback(sm) {
  const r = sm.update(bar(3, 98, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1 }));
  assert.strictEqual(r.phase, P.FIRST_PULLBACK);
  ok('S2 进入：回踩 EMA20 附近（|close-ema20|<0.5×ATR）', true);
  return sm;
}

function test_s3_second_trend(sm) {
  const r = sm.update(bar(4, 102, { st_dir: 1, dc_upper: 103, dc_lower: 97, dc_mid: 100, ema20: 99, atr: 1, high: 102.5 }));
  assert.strictEqual(r.phase, P.SECOND_TREND);
  ok('S3 进入：突破 wave1High', true);
  return sm;
}

function test_s3_exit_on_flip(sm) {
  const r = sm.update(bar(5, 95, { st_dir: -1, dc_upper: 103, dc_lower: 97, dc_mid: 100, ema20: 99, atr: 1 }));
  assert.strictEqual(r.phase, P.UNKNOWN);
  ok('S3 退出：ST 翻空 → unknown（保守结束）', true);
}

function test_s1_bear_mirror() {
  const sm = new TrendStateMachine();
  sm.update(bar(1, 99, { st_dir: 1, dc_upper: 100, dc_lower: 95, dc_mid: 97.5, ema20: 98, atr: 1 }));  // 设 prev
  const r = sm.update(bar(2, 94, { st_dir: -1, dc_upper: 99, dc_lower: 94, dc_mid: 96.5, ema20: 98, atr: 1, low: 93.5 }));
  assert.strictEqual(r.phase, P.FIRST_TREND);
  assert.strictEqual(r.direction, -1);
  ok('S1 空头镜像：ST 翻空 + 跌破 DC 下轨', true);
}

function test_s4_double_bottom() {
  const sm = new TrendStateMachine();
  sm.update(bar(1, 93, { st_dir: -1, dc_upper: 100, dc_lower: 90, dc_mid: 95, ema20: 95, atr: 2 }));  // 设 prev
  const ra = sm.update(bar(2, 90.2, { st_dir: -1, dc_upper: 100, dc_lower: 90, dc_mid: 95, ema20: 95, atr: 2, low: 89.7 }));
  assert.strictEqual(ra.phase, P.DOUBLE_BOTTOM);
  assert.strictEqual(ra.direction, -1);
  assert.strictEqual(ra.bottomCount, 1);
  const rb = sm.update(bar(3, 91, { st_dir: -1, dc_upper: 100, dc_lower: 90, dc_mid: 95, ema20: 95, atr: 2 }));
  assert.strictEqual(rb.bottomCount, 1);  // 未触轨，不增
  const rc = sm.update(bar(4, 90.3, { st_dir: -1, dc_upper: 100, dc_lower: 90, dc_mid: 95, ema20: 95, atr: 2, low: 89.8 }));
  assert.strictEqual(rc.bottomCount, 2);
  const rd = sm.update(bar(5, 101, { st_dir: 1, dc_upper: 102, dc_lower: 92, dc_mid: 97, ema20: 95, atr: 2, high: 101.5 }));
  assert.strictEqual(rd.phase, P.FIRST_TREND);
  assert.strictEqual(rd.direction, 1);
  ok('S4 双底反转：两次触下轨 + ST 翻多突破上轨 → S1', true);
}

function test_setstate_override() {
  const sm = new TrendStateMachine();
  sm.update(bar(1, 96, { st_dir: -1, dc_upper: 100, dc_lower: 95, dc_mid: 97.5, ema20: 98, atr: 1 }));
  sm.update(bar(2, 101, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1, high: 101.5 }));  // S1
  sm.setState(P.SECOND_TREND, 1);
  const r = sm.update(bar(3, 100, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1 }));
  assert.strictEqual(r.phase, P.SECOND_TREND);
  assert.strictEqual(r.overridden, true);
  ok('setState 手动修正：强制 S3，后续 update 基于此继续', true);
}

function test_replay_history() {
  const sm = new TrendStateMachine();
  const bars = [
    bar(1, 96, { st_dir: -1, dc_upper: 100, dc_lower: 95, dc_mid: 97.5, ema20: 98, atr: 1 }),
    bar(2, 101, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1, high: 101.5 }),
    bar(3, 98, { st_dir: 1, dc_upper: 102, dc_lower: 96, dc_mid: 99, ema20: 98, atr: 1 }),
    bar(4, 102, { st_dir: 1, dc_upper: 103, dc_lower: 97, dc_mid: 100, ema20: 99, atr: 1, high: 102.5 }),
    bar(5, 95, { st_dir: -1, dc_upper: 103, dc_lower: 97, dc_mid: 100, ema20: 99, atr: 1 }),
  ];
  const hist = sm.replay(bars);
  assert.strictEqual(hist.length, 3);
  assert.strictEqual(hist[0].to, P.FIRST_TREND);
  assert.strictEqual(hist[1].to, P.FIRST_PULLBACK);
  assert.strictEqual(hist[2].to, P.SECOND_TREND);
  ok('replay 重放历史 → 转换序列可追溯（S1→S2→S3，unknown 退出不入历史）', true);
}

function main() {
  console.log('TrendStateMachine 单元测试');
  test_prewarm();
  let sm = test_s1_first_trend_bull();
  sm = test_s2_pullback(sm);
  sm = test_s3_second_trend(sm);
  test_s3_exit_on_flip(sm);
  test_s1_bear_mirror();
  test_s4_double_bottom();
  test_setstate_override();
  test_replay_history();
  console.log(`\n✅ 全部通过（${passed} 项断言）`);
}

main();
