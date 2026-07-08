/**
 * Alpha 审批文案与状态机 — 全站统一（按 execution_mode 分支）
 */
(function (global) {
  const EXEC_ST_SUPER = 'st_super_immediate';
  const EXEC_RECLAIM = 'conditional_reclaim';

  const SIGNAL_LABEL = {
    st_super: '超级信号',
    pullback_vwap: '回踩 VWAP',
    pullback_supertrend: '回踩 ST',
    pullback_dema20: '回踩 DEMA20',
    pullback_dema: '回踩 DEMA',
  };

  function isStSuper(proposal) {
    if (!proposal) return false;
    return proposal.execution_mode === EXEC_ST_SUPER || proposal.signal_type === 'st_super';
  }

  function signalLabel(signalType) {
    return SIGNAL_LABEL[signalType] || signalType || '—';
  }

  function sideZh(side) {
    return side === 'SHORT' ? '空' : '多';
  }

  /** 阶段 pill / 标签文案 */
  function phaseLabel(proposal, status) {
    if (!proposal) return '无';
    const st = isStSuper(proposal);
    const phase = proposal.execution_phase;
    const dec = proposal.decision;

    if (status === 'pending' || proposal.status === 'pending') return '待审批';
    if (dec === 'rejected' || status === 'rejected') return '已驳回';

    if (st) {
      if (phase === 'ready_to_execute') {
        return dec === 'approved_observe' ? '观察·待执行' : '待执行';
      }
      if (phase === 'executed' || proposal.executed_at) return '已执行';
      return '待执行';
    }

    if (phase === 'approved_wait') {
      return dec === 'approved_observe' ? '观察·等待回踩' : '等待回踩';
    }
    if (phase === 'ready_to_execute') {
      return dec === 'approved_observe' ? '观察·待执行' : '待执行';
    }
    if (phase === 'failed') return '回踩失败';
    if (dec === 'approved_live') return '实盘';
    if (dec === 'approved_observe') return '观察';
    return phase || status || '—';
  }

  /** 批准后执行说明 */
  function postApproveHint(proposal) {
    if (!proposal) return '';
    if (isStSuper(proposal)) {
      return '批准后将开启 Agent 实盘，立即市价入场。';
    }
    return proposal.reclaim_label || '批准后将等待 reclaim 条件，满足后引擎执行。';
  }

  /** 流程步骤（st_super / legacy） */
  function workflowSteps(proposal, uiPhase) {
    const st = isStSuper(proposal);
    const labels = st
      ? ['①信号', '②建议', '③审批', '④执行']
      : ['①扫描', '②审批', '③回踩', '④执行'];

    const states = ['todo', 'todo', 'todo', 'todo'];
    if (uiPhase === 'none') {
      states[0] = 'active';
    } else if (uiPhase === 'pending') {
      if (st) {
        states[0] = 'done';
        states[1] = 'done';
        states[2] = 'active';
      } else {
        states[0] = 'done';
        states[1] = 'active';
      }
    } else if (uiPhase === 'wait') {
      states[0] = 'done';
      states[1] = 'done';
      states[2] = 'active';
    } else if (uiPhase === 'ready' || uiPhase === 'executing') {
      states[0] = 'done';
      states[1] = 'done';
      states[2] = 'done';
      states[3] = 'active';
    }
    return labels.map((label, i) => ({ label, state: states[i] }));
  }

  function pillForPhase(uiPhase) {
    if (uiPhase === 'pending') return { cls: 'pill-pending', text: '待审批' };
    if (uiPhase === 'executing') return { cls: 'pill-executing', text: '执行中' };
    if (uiPhase === 'wait') return { cls: 'pill-wait', text: '等待回踩' };
    if (uiPhase === 'ready') return { cls: 'pill-ready', text: '待执行' };
    return { cls: 'pill-none', text: '无信号' };
  }

  function formatIdleDetail(ctx) {
    if (!ctx || !ctx.superSide) {
      return '暂无超级信号 · 无待审批建议';
    }
    const align = ctx.stAligned
      ? '<span class="sig-ok">ST 同向</span>'
      : `<span class="sig-warn">ST 分歧 ${ctx.st5Label}/${ctx.st1Label}</span>`;
    const touch = ctx.touchTime ? `触线 ${ctx.touchTime}` : '';
    const parts = [`<b class="${ctx.sideCls}">${ctx.sideLabel}</b>`, align];
    if (touch) parts.push(touch);
    parts.push('<span class="sig-muted">无待审批</span>');
    return parts.join(' · ');
  }

  function formatProposalDetail(proposal) {
    if (!proposal) return '暂无活跃建议';
    const sig = signalLabel(proposal.signal_type);
    const side = sideZh(proposal.side);
    const trig = proposal.trigger_level != null ? Number(proposal.trigger_level).toFixed(2) : '—';
    const entry = proposal.entry_price != null ? Number(proposal.entry_price).toFixed(2) : '—';
    const stop = proposal.stop_price != null ? Number(proposal.stop_price).toFixed(2) : '—';
    const tp = proposal.tp_half_price ?? proposal.tp_price;
    const tpStr = tp != null ? Number(tp).toFixed(2) : '—';
    const rr = proposal.rr_half_est != null ? Number(proposal.rr_half_est).toFixed(1) : '—';
    const ttl = ttlText(proposal);
    let lines = `${side} · ${sig}\n入 ${entry}  损 ${stop}  TP½ ${tpStr}  R:R ${rr}`;
    if (proposal.thesis) lines += `\n${proposal.thesis}`;
    if (ttl) lines += `\n⏱ ${ttl}`;
    return lines;
  }

  function ttlText(proposal) {
    const exp = Number(proposal?.expires_at || 0);
    if (!exp) return '';
    const sec = exp - Math.floor(Date.now() / 1000);
    if (sec <= 0) return '已过期';
    const m = Math.floor(sec / 60);
    const s = sec % 60;
    return m > 0 ? `剩余 ${m} 分 ${s} 秒` : `剩余 ${s} 秒`;
  }

  function approveModalBody(proposal, engineOk) {
    const sym = String(proposal.symbol || '').toUpperCase();
    const side = sideZh(proposal.side);
    const entry = proposal.entry_price != null ? Number(proposal.entry_price).toFixed(2) : '—';
    const stop = proposal.stop_price != null ? Number(proposal.stop_price).toFixed(2) : '—';
    const tp = proposal.tp_half_price ?? proposal.tp_price;
    const tpStr = tp != null ? Number(tp).toFixed(2) : '—';
    const rr = proposal.rr_half_est != null ? Number(proposal.rr_half_est).toFixed(1) : '—';
    const conf = proposal.confidence != null
      ? `${(Number(proposal.confidence) * 100).toFixed(0)}%` : '—';
    return {
      title: `批准实盘 · ${sym}`,
      rows: [
        ['方向', `${side} · ${signalLabel(proposal.signal_type)}`],
        ['入场', entry],
        ['止损', stop],
        ['半仓止盈', tpStr],
        ['R:R½', rr],
        ['置信度', conf],
        ['有效期', ttlText(proposal) || '—'],
      ],
      execHint: postApproveHint(proposal),
      engineOk: engineOk !== false,
      engineText: engineOk === false ? '引擎离线 — 无法批准' : '引擎在线',
    };
  }

  /** 四宫格是否展示「观察」批准（st_super 仅实盘） */
  function showObserveApprove(proposal) {
    return proposal && !isStSuper(proposal);
  }

  /** 执行结果文案 */
  function execResultLabel(result) {
    const m = {
      executed: '已实盘执行',
      observed: '已观察记录',
      risk_rejected: '风控拒绝',
      invalid_intent: '无效意图',
    };
    return m[result] || result || '已处理';
  }

  global.AlphaCopy = {
    EXEC_ST_SUPER,
    EXEC_RECLAIM,
    isStSuper,
    signalLabel,
    sideZh,
    phaseLabel,
    postApproveHint,
    workflowSteps,
    pillForPhase,
    formatProposalDetail,
    formatIdleDetail,
    ttlText,
    approveModalBody,
    showObserveApprove,
    execResultLabel,
    SIGNAL_LABEL,
  };
})(window);
