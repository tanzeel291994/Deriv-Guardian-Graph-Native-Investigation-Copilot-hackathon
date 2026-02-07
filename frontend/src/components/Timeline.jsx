import { useState, useEffect, useRef, useMemo } from 'react';
import { api } from '../api';

export default function Timeline({ partnerId }) {
  const [globalData, setGlobalData] = useState(null);
  const [partnerData, setPartnerData] = useState(null);
  const [sliderValue, setSliderValue] = useState(100); // 0-100%
  const canvasRef = useRef();
  const containerRef = useRef();

  // Load full global timeline + partner-specific data
  useEffect(() => {
    api.getTimeline(null, false, 10000).then(setGlobalData).catch(console.error);
  }, []);

  useEffect(() => {
    if (!partnerId) { setPartnerData(null); return; }
    api.getTimeline(partnerId, false, 5000).then(setPartnerData).catch(console.error);
  }, [partnerId]);

  const allDays = useMemo(() => globalData?.daily_summary || [], [globalData]);

  const visibleCount = useMemo(() => {
    return Math.max(1, Math.ceil(allDays.length * (sliderValue / 100)));
  }, [allDays, sliderValue]);

  // Phase detection for narrative
  const phase = useMemo(() => {
    if (!allDays.length) return null;
    const visible = allDays.slice(0, visibleCount);
    const totalOpp = visible.reduce((s, d) => s + (d.opposite_count || 0), 0);
    const totalTrades = visible.reduce((s, d) => s + (d.trade_count || 0), 0);
    const oppRatio = totalTrades > 0 ? totalOpp / totalTrades : 0;
    const pct = sliderValue;

    if (pct <= 25) return {
      label: 'GROOMING PHASE',
      color: '#22c55e',
      description: 'Normal trading activity. Small test transactions. Rule-based systems see nothing suspicious.',
      icon: '🟢',
    };
    if (pct <= 50) return {
      label: 'ESCALATION PHASE',
      color: '#f59e0b',
      description: `Opposite trades appearing (${totalOpp} detected). Clients begin mirrored trading patterns. Kumo GNN detects subtle structural anomalies.`,
      icon: '🟡',
    };
    if (pct <= 75) return {
      label: 'ACTIVE FRAUD PHASE',
      color: '#ef4444',
      description: `${totalOpp} opposite trades detected (${(oppRatio * 100).toFixed(0)}% of volume). Coordinated ring activity at peak. Commission extraction accelerating.`,
      icon: '🔴',
    };
    return {
      label: 'FULL EXPOSURE',
      color: '#a855f7',
      description: `Complete timeline: ${totalOpp} opposite trades across ${visible.length} days. The GNN identified this pattern weeks before rule-based systems would trigger.`,
      icon: '🟣',
    };
  }, [allDays, visibleCount, sliderValue]);

  // Draw the timeline chart on canvas
  useEffect(() => {
    if (!canvasRef.current || !allDays.length) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    const container = containerRef.current;
    if (!container) return;

    const dpr = window.devicePixelRatio || 1;
    const width = container.offsetWidth;
    const height = 120;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    const padding = { left: 20, right: 20, top: 10, bottom: 24 };
    const chartW = width - padding.left - padding.right;
    const chartH = height - padding.top - padding.bottom;
    const barGap = 2;
    const barWidth = Math.max((chartW / allDays.length) - barGap, 3);
    const maxCount = Math.max(...allDays.map(d => d.trade_count || 0), 1);

    // Build partner daily lookup
    const partnerDayMap = {};
    if (partnerData?.daily_summary) {
      for (const d of partnerData.daily_summary) {
        partnerDayMap[d.date] = d;
      }
    }

    allDays.forEach((d, i) => {
      const isVisible = i < visibleCount;
      const x = padding.left + i * (barWidth + barGap);
      const tradeH = ((d.trade_count || 0) / maxCount) * chartH;
      const y = padding.top + chartH - tradeH;

      // Background bar (normal trades)
      ctx.fillStyle = isVisible
        ? 'rgba(59, 130, 246, 0.35)'
        : 'rgba(59, 130, 246, 0.07)';
      ctx.beginPath();
      ctx.roundRect(x, y, barWidth, tradeH, [2, 2, 0, 0]);
      ctx.fill();

      // Opposite trades overlay (stacked from bottom)
      const oppCount = d.opposite_count || 0;
      if (oppCount > 0) {
        const oppH = (oppCount / maxCount) * chartH;
        ctx.fillStyle = isVisible
          ? 'rgba(239, 68, 68, 0.75)'
          : 'rgba(239, 68, 68, 0.12)';
        ctx.beginPath();
        ctx.roundRect(x, padding.top + chartH - oppH, barWidth, oppH, [2, 2, 0, 0]);
        ctx.fill();
      }

      // Partner-specific highlight (bright outline)
      const pd = partnerDayMap[d.date];
      if (pd && isVisible) {
        const pdH = ((pd.trade_count || 0) / maxCount) * chartH;
        ctx.strokeStyle = '#06b6d4';
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        ctx.roundRect(x - 1, padding.top + chartH - pdH - 1, barWidth + 2, pdH + 2, [3, 3, 0, 0]);
        ctx.stroke();
      }
    });

    // Cutoff line (purple dashed vertical)
    if (visibleCount < allDays.length) {
      const cutoffX = padding.left + visibleCount * (barWidth + barGap) - barGap / 2;
      ctx.save();
      ctx.strokeStyle = 'rgba(168, 85, 247, 0.7)';
      ctx.lineWidth = 2;
      ctx.setLineDash([5, 4]);
      ctx.beginPath();
      ctx.moveTo(cutoffX, padding.top);
      ctx.lineTo(cutoffX, padding.top + chartH);
      ctx.stroke();
      ctx.restore();

      // "NOW" arrow
      ctx.font = 'bold 9px Inter, sans-serif';
      ctx.fillStyle = '#a855f7';
      ctx.textAlign = 'center';
      ctx.fillText('▼ NOW', cutoffX, padding.top - 1);
    }

    // X-axis date labels
    ctx.font = '9px JetBrains Mono, monospace';
    ctx.fillStyle = '#64748b';
    const labelEvery = Math.max(1, Math.floor(allDays.length / 6));
    allDays.forEach((d, i) => {
      if (i % labelEvery === 0 || i === allDays.length - 1) {
        const x = padding.left + i * (barWidth + barGap) + barWidth / 2;
        ctx.textAlign = 'center';
        ctx.fillText(d.date.slice(5), x, height - 6); // show MM-DD
      }
    });
  }, [allDays, visibleCount, partnerData]);

  if (!globalData) {
    return (
      <div style={{
        height: 180, background: 'var(--bg-secondary)',
        borderTop: '1px solid var(--border)',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
      }}>
        <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>Loading temporal data...</p>
      </div>
    );
  }

  const visibleDays = allDays.slice(0, visibleCount);
  const totalTrades = visibleDays.reduce((s, d) => s + (d.trade_count || 0), 0);
  const totalOpp = visibleDays.reduce((s, d) => s + (d.opposite_count || 0), 0);
  const totalVolume = visibleDays.reduce((s, d) => s + (d.total_volume || 0), 0);

  return (
    <div style={{
      flexShrink: 0, background: 'var(--bg-secondary)',
      borderTop: '1px solid var(--border)',
    }}>
      {/* Header + Legend */}
      <div style={{
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '10px 20px 0',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ fontSize: 14, fontWeight: 700 }}>⏱ Temporal Intelligence</span>
          <span style={{
            fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 10,
            background: phase ? `${phase.color}22` : 'transparent',
            color: phase?.color,
            letterSpacing: '0.04em',
          }}>
            {phase?.icon} {phase?.label}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 14, fontSize: 11 }}>
          <span style={{ color: '#3b82f6' }}>■ All Trades ({totalTrades.toLocaleString()})</span>
          <span style={{ color: '#ef4444' }}>■ Opposite ({totalOpp.toLocaleString()})</span>
          {partnerId && <span style={{ color: '#06b6d4' }}>□ {partnerId}</span>}
        </div>
      </div>

      {/* Narrative text */}
      {phase && (
        <div style={{
          padding: '4px 20px 6px', fontSize: 11, color: 'var(--text-secondary)',
          lineHeight: 1.4, maxWidth: 700,
        }}>
          {phase.description}
        </div>
      )}

      {/* Canvas chart */}
      <div ref={containerRef} style={{ padding: '0 20px' }}>
        <canvas ref={canvasRef} style={{ width: '100%', borderRadius: 6, cursor: 'crosshair' }} />
      </div>

      {/* Slider */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '6px 20px 10px' }}>
        <span style={{ fontSize: 10, color: 'var(--text-muted)', width: 80, fontFamily: 'var(--font-mono)' }}>
          {allDays[0]?.date?.slice(5) || ''}
        </span>
        <input
          type="range"
          min={5}
          max={100}
          value={sliderValue}
          onChange={e => setSliderValue(Number(e.target.value))}
          style={{ flex: 1, accentColor: '#a855f7', height: 6, cursor: 'pointer' }}
        />
        <span style={{ fontSize: 10, color: 'var(--text-muted)', width: 80, textAlign: 'right', fontFamily: 'var(--font-mono)' }}>
          {allDays[allDays.length - 1]?.date?.slice(5) || ''}
        </span>
      </div>

      {/* Stats bar */}
      <div style={{
        display: 'flex', gap: 24, padding: '0 20px 10px', fontSize: 11,
      }}>
        <Stat label="Visible Days" value={visibleCount} />
        <Stat label="Trades" value={totalTrades.toLocaleString()} />
        <Stat label="Opposite Trades" value={totalOpp.toLocaleString()} color={totalOpp > 0 ? '#ef4444' : undefined} />
        <Stat label="Opp. Ratio" value={`${totalTrades > 0 ? ((totalOpp / totalTrades) * 100).toFixed(1) : 0}%`} color={totalOpp / Math.max(totalTrades, 1) > 0.2 ? '#ef4444' : undefined} />
        <Stat label="Volume" value={`$${(totalVolume / 1000).toFixed(0)}k`} />
      </div>
    </div>
  );
}

function Stat({ label, value, color }) {
  return (
    <div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 1 }}>{label}</div>
      <div style={{ fontSize: 13, fontWeight: 700, fontFamily: 'var(--font-mono)', color: color || 'var(--text-primary)' }}>
        {value}
      </div>
    </div>
  );
}
