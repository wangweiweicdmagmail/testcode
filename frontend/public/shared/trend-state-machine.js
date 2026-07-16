/**
 * 定势陷阱行情状态机（前端 if-else 规则引擎）。
 *
 * 四态：first_trend(第一波趋势) / first_pullback(第一波回归) /
 *       second_trend(第二波趋势) / double_bottom(双底反转)
 * 全程顺势：direction 由 st_dir 决定，空头镜像。
 *
 * 指标输入（Python M5 策略算好，随 m5_bar 经 kline:5m / bars:5m 传到前端）：
 *   st_value / st_dir / dc_upper / dc_lower / dc_mid / ema20 / atr
 *
 * 判定用"上一根冻结的 DC 值"(_prevDc*)，与后端 STState flip 时序一致
 * （supertrend.py 用 _prev_upper_b 判翻转），避免当根 high 刷新 DC.upper
 * 同时 close 突破的假信号。
 *
 * 兼容：浏览器挂 window.TrendStateMachine；node 测试用 module.exports。
 */
(function (global) {
  'use strict';

  const PHASE_UNKNOWN = 'unknown';
  const PHASE_FIRST_TREND = 'first_trend';
  const PHASE_FIRST_PULLBACK = 'first_pullback';
  const PHASE_SECOND_TREND = 'second_trend';
  const PHASE_DOUBLE_BOTTOM = 'double_bottom';

  class TrendStateMachine {
    constructor(opts = {}) {
      this._pullbackAtrMult = opts.pullbackAtrMult != null ? opts.pullbackAtrMult : 0.5;
      this._bottomTouchAtr = opts.bottomTouchAtr != null ? opts.bottomTouchAtr : 0.3;
      this._breakoutEps = opts.breakoutEps != null ? opts.breakoutEps : 0.0;
      this._maxBottomSpacing = opts.maxBottomSpacingBars != null ? opts.maxBottomSpacingBars : 30;
      this._reset();
    }

    /** 全部内部状态归零（仅 replay / 构造时调用；清历史与 _prevDc*）。 */
    _reset() {
      this._phase = PHASE_UNKNOWN;
      this._direction = 0;
      this._prevStDir = 0;
      this._wave1High = null;
      this._wave1Low = null;
      this._bottomCount = 0;
      this._bottomLows = [];
      this._bottomTimes = [];
      this._barIndex = 0;
      this._prevDcUpper = 0;
      this._prevDcLower = 0;
      this._prevDcMid = 0;
      this._history = [];
      this._lastResult = null;
      this._overridden = false;
    }

    /** 状态退出到 unknown（update 内调用）：清当前态与累积，保留历史与 prevDc、barIndex（可追溯、连续）。 */
    _exitUnknown() {
      this._phase = PHASE_UNKNOWN;
      this._direction = 0;
      this._wave1High = null;
      this._wave1Low = null;
      this._bottomCount = 0;
      this._bottomLows = [];
      this._bottomTimes = [];
    }

    /** 喂一根 M5 bar，返回当前态。bar 字段见文件头注释。 */
    update(bar) {
      const prevPhase = this._phase;
      this._barIndex += 1;

      const h = bar.high, lo = bar.low, c = bar.close;
      const stDir = bar.st_dir || 0;
      const dcUpper = bar.dc_upper;
      const dcLower = bar.dc_lower;
      const dcMid = bar.dc_mid;
      const ema20 = bar.ema20;
      const atr = bar.atr;
      const barTime = bar.time || 0;

      // DC/ST/EMA 任一未就绪 → 预热期，强制 unknown（但仍累积 _prevDc*/_prevStDir）
      const prewarm = dcUpper == null || stDir === 0 || ema20 == null;
      if (!prewarm) {
        const atrVal = (atr && atr > 0) ? atr : 0;
        const flipBull = this._prevStDir === -1 && stDir === 1;
        const flipBear = this._prevStDir === 1 && stDir === -1;
        const nearEma = this._isNearEma(c, ema20, atrVal);
        const touchLower = this._isTouchLower(c, this._prevDcLower, atrVal);
        const touchUpper = this._isTouchUpper(c, this._prevDcUpper, atrVal);
        const brkUp = this._prevDcUpper > 0 && c > this._prevDcUpper * (1 + this._breakoutEps);
        const brkDn = this._prevDcLower > 0 && c < this._prevDcLower * (1 - this._breakoutEps);

        switch (this._phase) {
          case PHASE_UNKNOWN:
            if (stDir === 1 && flipBull && brkUp) {
              this._enterFirstTrend(1, h, lo);
            } else if (stDir === -1 && flipBear && brkDn) {
              this._enterFirstTrend(-1, h, lo);
            } else if (stDir === -1 && touchLower) {
              this._enterDoubleBottom(-1, lo);
            } else if (stDir === 1 && touchUpper) {
              this._enterDoubleBottom(1, h);
            }
            break;

          case PHASE_FIRST_TREND:
            if (this._direction === 1) {
              if (stDir === -1 || c <= this._prevDcLower) this._exitUnknown();
              else if (nearEma && c > this._prevDcLower) this._phase = PHASE_FIRST_PULLBACK;
            } else {
              if (stDir === 1 || c >= this._prevDcUpper) this._exitUnknown();
              else if (nearEma && c < this._prevDcUpper) this._phase = PHASE_FIRST_PULLBACK;
            }
            break;

          case PHASE_FIRST_PULLBACK:
            if (this._direction === 1) {
              if (stDir === -1 || c <= this._prevDcLower) this._exitUnknown();
              else if (this._wave1High != null && c > this._wave1High) this._phase = PHASE_SECOND_TREND;
            } else {
              if (stDir === 1 || c >= this._prevDcUpper) this._exitUnknown();
              else if (this._wave1Low != null && c < this._wave1Low) this._phase = PHASE_SECOND_TREND;
            }
            break;

          case PHASE_SECOND_TREND:
            // S3 只因 ST 翻转退出（保守结束，回调不再回 S2）
            if (this._direction === 1 && stDir === -1) this._exitUnknown();
            else if (this._direction === -1 && stDir === 1) this._exitUnknown();
            break;

          case PHASE_DOUBLE_BOTTOM:
            if (this._direction === -1) {
              if (c < this._prevDcLower) {
                this._exitUnknown();                    // 跌破下轨，双底失败
              } else if (flipBull && brkUp) {
                this._enterFirstTrend(1, h, lo);        // ST 翻多+突破上轨 → 反转确认进 S1
              } else if (touchLower) {
                this._recordBottom(lo);
              }
            } else {
              if (c > this._prevDcUpper) {
                this._exitUnknown();
              } else if (flipBear && brkDn) {
                this._enterFirstTrend(-1, h, lo);
              } else if (touchUpper) {
                this._recordBottom(h);
              }
            }
            break;
        }
      } else {
        this._phase = PHASE_UNKNOWN;
        this._direction = 0;
      }

      // first_trend 期间持续追踪极值（S3 突破判定用）
      if (this._phase === PHASE_FIRST_TREND) {
        if (this._direction === 1) {
          this._wave1High = this._wave1High == null ? h : Math.max(this._wave1High, h);
        } else {
          this._wave1Low = this._wave1Low == null ? lo : Math.min(this._wave1Low, lo);
        }
      }

      // 保存上一根冻结值（出预热后即可用）
      if (dcUpper != null) this._prevDcUpper = dcUpper;
      if (dcLower != null) this._prevDcLower = dcLower;
      if (dcMid != null) this._prevDcMid = dcMid;
      if (stDir !== 0) this._prevStDir = stDir;

      const changed = prevPhase !== this._phase;
      if (changed && this._phase !== PHASE_UNKNOWN) {
        this._history.push({
          time: barTime,
          from: prevPhase,
          to: this._phase,
          direction: this._direction,
          barIndex: this._barIndex,
          manual: false,
        });
      }

      const result = {
        phase: this._phase,
        direction: this._direction,
        dcUpper: dcUpper != null ? dcUpper : this._prevDcUpper,
        dcLower: dcLower != null ? dcLower : this._prevDcLower,
        dcMid: dcMid != null ? dcMid : this._prevDcMid,
        stDir,
        ema20,
        atr,
        changed,
        wave1High: this._wave1High,
        wave1Low: this._wave1Low,
        bottomCount: this._bottomCount,
        barTime,
        overridden: this._overridden,
      };
      this._lastResult = result;
      return result;
    }

    _enterFirstTrend(direction, h, lo) {
      this._phase = PHASE_FIRST_TREND;
      this._direction = direction;
      this._wave1High = h;
      this._wave1Low = lo;
      this._bottomCount = 0;
      this._bottomLows = [];
      this._bottomTimes = [];
    }

    _enterDoubleBottom(direction, extreme) {
      this._phase = PHASE_DOUBLE_BOTTOM;
      this._direction = direction;
      this._bottomCount = 1;
      this._bottomLows = [extreme];
      this._bottomTimes = [this._barIndex];
    }

    _recordBottom(extreme) {
      // 距上个底间距超时 → 双底失效，重新计底
      if (this._bottomTimes.length >= 1 &&
          this._barIndex - this._bottomTimes[this._bottomTimes.length - 1] > this._maxBottomSpacing) {
        this._bottomCount = 0;
        this._bottomLows = [];
        this._bottomTimes = [];
      }
      this._bottomCount += 1;
      this._bottomLows.push(extreme);
      this._bottomTimes.push(this._barIndex);
    }

    _isNearEma(c, ema, atr) {
      if (atr > 0) return Math.abs(c - ema) < this._pullbackAtrMult * atr;
      return Math.abs(c - ema) < 0.01 * c;   // ATR 缺失 fallback：0.01% 价格
    }

    _isTouchLower(c, dcLower, atr) {
      if (!(dcLower > 0)) return false;
      const tol = atr > 0 ? this._bottomTouchAtr * atr : 0.01 * c;
      return Math.abs(c - dcLower) < tol;
    }

    _isTouchUpper(c, dcUpper, atr) {
      if (!(dcUpper > 0)) return false;
      const tol = atr > 0 ? this._bottomTouchAtr * atr : 0.01 * c;
      return Math.abs(c - dcUpper) < tol;
    }

    /** 批量重放历史 bar，返回有效转换序列（可追溯）。会清空 override 与历史。 */
    replay(bars) {
      this._reset();
      for (const b of bars) this.update(b);
      return this._history.slice();
    }

    /** 手动修正当前态：重置内部累积字段，后续 update 基于此继续。记入历史（manual 标记）。 */
    setState(phase, direction) {
      const prev = this._phase;
      this._phase = phase;
      this._direction = direction;
      this._wave1High = null;
      this._wave1Low = null;
      this._bottomCount = 0;
      this._bottomLows = [];
      this._bottomTimes = [];
      this._overridden = true;
      if (phase !== PHASE_UNKNOWN) {
        this._history.push({
          time: 0,
          from: prev,
          to: phase,
          direction,
          barIndex: this._barIndex,
          manual: true,
        });
      }
      this._lastResult = {
        phase: this._phase,
        direction: this._direction,
        dcUpper: this._prevDcUpper,
        dcLower: this._prevDcLower,
        dcMid: this._prevDcMid,
        stDir: this._prevStDir,
        ema20: null,
        atr: null,
        changed: false,
        wave1High: null,
        wave1Low: null,
        bottomCount: 0,
        barTime: 0,
        overridden: true,
      };
    }

    /** 清除手动修正，回到自动模式（外部随后调 replay 重建自然态）。保留历史与 _prevDc*。 */
    clearOverride() {
      this._overridden = false;
      this._phase = PHASE_UNKNOWN;
      this._direction = 0;
      this._wave1High = null;
      this._wave1Low = null;
      this._bottomCount = 0;
      this._bottomLows = [];
      this._bottomTimes = [];
    }

    getHistory() {
      return this._history.slice();
    }

    getState() {
      return this._lastResult;
    }
  }

  // 阶段常量导出（供外部判定 phase 名）
  TrendStateMachine.PHASE = {
    UNKNOWN: PHASE_UNKNOWN,
    FIRST_TREND: PHASE_FIRST_TREND,
    FIRST_PULLBACK: PHASE_FIRST_PULLBACK,
    SECOND_TREND: PHASE_SECOND_TREND,
    DOUBLE_BOTTOM: PHASE_DOUBLE_BOTTOM,
  };

  global.TrendStateMachine = TrendStateMachine;
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { TrendStateMachine };
  }
})(typeof window !== 'undefined' ? window : globalThis);
