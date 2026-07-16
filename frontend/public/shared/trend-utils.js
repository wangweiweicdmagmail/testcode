/**
 * 定势状态机四态映射（phase → 标签/颜色/形状），供 index.html / multi.html 复用。
 * 颜色与 trend-state-machine.js 的 PHASE 语义一致；空头镜像时趋势态箭头朝下、偏红。
 */
(function (global) {
  'use strict';

  const PHASE_MAP = {
    unknown:        { label: '—',      color: '#4b5563', shape: 'circle',  text: '' },
    first_trend:    { label: 'S1趋势', color: '#26a69a', shape: 'arrowUp', text: 'S1' },
    first_pullback: { label: 'S2回归', color: '#f59e0b', shape: 'circle',  text: 'S2' },
    second_trend:   { label: 'S3二波', color: '#22d3ee', shape: 'arrowUp', text: 'S3' },
    double_bottom:  { label: 'S4双底', color: '#a78bfa', shape: 'circle',  text: 'S4' },
  };

  function phaseInfo(phase, dir) {
    const base = PHASE_MAP[phase] || PHASE_MAP.unknown;
    if (dir === -1) {
      const shape = base.shape === 'arrowUp' ? 'arrowDown' : base.shape;
      let color = base.color;
      if (phase === 'first_trend') color = '#ef5350';
      else if (phase === 'second_trend') color = '#f43f5e';
      return { label: base.label, color, shape, text: base.text };
    }
    return { label: base.label, color: base.color, shape: base.shape, text: base.text };
  }

  function hexToRgb(hex) {
    const r = parseInt(hex.slice(1, 3), 16);
    const g = parseInt(hex.slice(3, 5), 16);
    const b = parseInt(hex.slice(5, 7), 16);
    return `${r},${g},${b}`;
  }

  function badge(phase, dir) {
    if (!phase || phase === 'unknown') return '<span style="color:#4b5563">—</span>';
    const info = phaseInfo(phase, dir);
    return `<span class="badge" style="background:rgba(${hexToRgb(info.color)},.2);color:${info.color}">${info.label}</span>`;
  }

  global.TrendUtils = { PHASE_MAP, phaseInfo, badge, hexToRgb };
  if (typeof module !== 'undefined' && module.exports) {
    module.exports = { TrendUtils: global.TrendUtils || { PHASE_MAP, phaseInfo, badge, hexToRgb } };
  }
})(typeof window !== 'undefined' ? window : globalThis);
