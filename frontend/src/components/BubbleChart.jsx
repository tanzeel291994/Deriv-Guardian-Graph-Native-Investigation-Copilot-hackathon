import { useRef, useEffect, useState, useCallback } from 'react';

const RISK_COLORS = {
  CRITICAL: '#ef4444',
  HIGH: '#f97316',
  MEDIUM: '#f59e0b',
  LOW: '#22c55e',
};

export default function BubbleChart({ data, onPartnerClick }) {
  const canvasRef = useRef();
  const containerRef = useRef();
  const [dims, setDims] = useState({ width: 600, height: 380 });
  const [tooltip, setTooltip] = useState(null);
  const bubblesRef = useRef([]);

  // Responsive
  useEffect(() => {
    function update() {
      if (containerRef.current) {
        setDims({
          width: containerRef.current.offsetWidth,
          height: 380,
        });
      }
    }
    update();
    window.addEventListener('resize', update);
    return () => window.removeEventListener('resize', update);
  }, []);

  // Draw
  useEffect(() => {
    if (!canvasRef.current || !data?.length) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const { width, height } = dims;

    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = `${width}px`;
    canvas.style.height = `${height}px`;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    const pad = { top: 40, right: 30, bottom: 50, left: 60 };
    const chartW = width - pad.left - pad.right;
    const chartH = height - pad.top - pad.bottom;

    // Scales
    const maxClients = Math.max(...data.map(d => d.num_referred_clients || 0), 1);
    const maxOpp = Math.max(...data.map(d => d.opp_ratio || 0), 0.01);
    const maxVol = Math.max(...data.map(d => d.total_volume || 0), 1);

    const xScale = (v) => pad.left + (v / (maxClients * 1.1)) * chartW;
    const yScale = (v) => pad.top + chartH - (v / (Math.min(maxOpp * 1.2, 1))) * chartH;
    const rScale = (v) => 4 + Math.sqrt(v / maxVol) * 20;

    // Grid lines
    ctx.strokeStyle = 'rgba(30, 41, 59, 0.5)';
    ctx.lineWidth = 0.5;
    for (let i = 0; i <= 5; i++) {
      const y = pad.top + (chartH / 5) * i;
      ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(pad.left + chartW, y); ctx.stroke();
      const x = pad.left + (chartW / 5) * i;
      ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, pad.top + chartH); ctx.stroke();
    }

    // Danger zone (high opp_ratio)
    const dangerY = yScale(0.3);
    if (dangerY > pad.top && dangerY < pad.top + chartH) {
      ctx.fillStyle = 'rgba(239, 68, 68, 0.04)';
      ctx.fillRect(pad.left, pad.top, chartW, dangerY - pad.top);
      ctx.strokeStyle = 'rgba(239, 68, 68, 0.3)';
      ctx.lineWidth = 1;
      ctx.setLineDash([6, 4]);
      ctx.beginPath(); ctx.moveTo(pad.left, dangerY); ctx.lineTo(pad.left + chartW, dangerY); ctx.stroke();
      ctx.setLineDash([]);
      ctx.font = '9px Inter, sans-serif';
      ctx.fillStyle = 'rgba(239, 68, 68, 0.6)';
      ctx.textAlign = 'right';
      ctx.fillText('⚠ Fraud Threshold', pad.left + chartW - 4, dangerY - 4);
    }

    // Store bubble positions for hit-testing
    const bubbles = [];

    // Draw bubbles (clean first, then fraud on top)
    const sorted = [...data].sort((a, b) => (a.is_fraudulent ? 1 : 0) - (b.is_fraudulent ? 1 : 0));

    sorted.forEach(d => {
      const x = xScale(d.num_referred_clients || 0);
      const y = yScale(d.opp_ratio || 0);
      const r = rScale(d.total_volume || 0);
      const color = RISK_COLORS[d.risk_tier] || RISK_COLORS.LOW;

      // Glow for critical
      if (d.risk_tier === 'CRITICAL') {
        ctx.shadowColor = color;
        ctx.shadowBlur = 12;
      }

      ctx.globalAlpha = d.is_fraudulent ? 0.85 : 0.45;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();

      // Stroke
      ctx.globalAlpha = 1;
      ctx.strokeStyle = d.is_fraudulent ? color : 'rgba(100,116,139,0.3)';
      ctx.lineWidth = d.is_fraudulent ? 1.5 : 0.5;
      ctx.stroke();

      ctx.shadowColor = 'transparent';
      ctx.shadowBlur = 0;

      bubbles.push({ ...d, cx: x, cy: y, r });
    });

    bubblesRef.current = bubbles;

    // Axes labels
    ctx.globalAlpha = 1;
    ctx.font = '10px Inter, sans-serif';
    ctx.fillStyle = '#94a3b8';

    // X-axis
    ctx.textAlign = 'center';
    ctx.fillText('Referred Clients (Network Size) →', pad.left + chartW / 2, height - 8);
    for (let i = 0; i <= 5; i++) {
      const val = Math.round((maxClients * 1.1 / 5) * i);
      ctx.fillText(val, xScale(val), pad.top + chartH + 16);
    }

    // Y-axis
    ctx.save();
    ctx.translate(14, pad.top + chartH / 2);
    ctx.rotate(-Math.PI / 2);
    ctx.textAlign = 'center';
    ctx.fillText('Opposite Trade Ratio (Fraud Intensity) →', 0, 0);
    ctx.restore();

    for (let i = 0; i <= 5; i++) {
      const val = (Math.min(maxOpp * 1.2, 1) / 5) * i;
      ctx.textAlign = 'right';
      ctx.fillText(`${(val * 100).toFixed(0)}%`, pad.left - 6, yScale(val) + 3);
    }

    // Title
    ctx.font = 'bold 12px Inter, sans-serif';
    ctx.fillStyle = '#f1f5f9';
    ctx.textAlign = 'left';
    ctx.fillText('Risk Landscape', pad.left, 16);
    ctx.font = '10px Inter, sans-serif';
    ctx.fillStyle = '#64748b';
    ctx.fillText('Each bubble = 1 partner · Size = trade volume · Color = risk tier', pad.left, 30);

  }, [data, dims]);

  // Mouse interaction
  const handleMouse = useCallback((e) => {
    if (!canvasRef.current) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;

    const hit = bubblesRef.current.find(b => {
      const dx = mx - b.cx;
      const dy = my - b.cy;
      return dx * dx + dy * dy <= (b.r + 4) * (b.r + 4);
    });

    if (hit) {
      canvasRef.current.style.cursor = 'pointer';
      setTooltip({
        x: e.clientX,
        y: e.clientY,
        data: hit,
      });
    } else {
      canvasRef.current.style.cursor = 'crosshair';
      setTooltip(null);
    }
  }, []);

  const handleClick = useCallback((e) => {
    if (!canvasRef.current || !onPartnerClick) return;
    const rect = canvasRef.current.getBoundingClientRect();
    const mx = e.clientX - rect.left;
    const my = e.clientY - rect.top;
    const hit = bubblesRef.current.find(b => {
      const dx = mx - b.cx;
      const dy = my - b.cy;
      return dx * dx + dy * dy <= (b.r + 4) * (b.r + 4);
    });
    if (hit) onPartnerClick(hit);
  }, [onPartnerClick]);

  return (
    <div ref={containerRef} style={{ position: 'relative' }}>
      <canvas
        ref={canvasRef}
        onMouseMove={handleMouse}
        onMouseLeave={() => setTooltip(null)}
        onClick={handleClick}
        style={{ width: '100%', borderRadius: 8, cursor: 'crosshair' }}
      />
      {tooltip && (
        <div style={{
          position: 'fixed', left: tooltip.x + 12, top: tooltip.y - 10,
          background: 'rgba(10, 14, 23, 0.95)', backdropFilter: 'blur(8px)',
          border: `1px solid ${RISK_COLORS[tooltip.data.risk_tier] || '#1e293b'}`,
          borderRadius: 8, padding: '10px 14px', fontSize: 11,
          pointerEvents: 'none', zIndex: 1000, maxWidth: 260,
        }}>
          <div style={{ fontWeight: 700, fontSize: 12, marginBottom: 4, color: RISK_COLORS[tooltip.data.risk_tier] }}>
            {tooltip.data.partner_id} — {tooltip.data.risk_tier}
          </div>
          <div style={{ color: '#94a3b8', marginBottom: 6 }}>{tooltip.data.entity_name}</div>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '3px 12px', color: '#cbd5e1' }}>
            <span>Clients:</span><span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>{tooltip.data.num_referred_clients}</span>
            <span>Opp. Ratio:</span><span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: tooltip.data.opp_ratio > 0.3 ? '#ef4444' : '#cbd5e1' }}>{(tooltip.data.opp_ratio * 100).toFixed(1)}%</span>
            <span>Volume:</span><span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600 }}>${(tooltip.data.total_volume / 1000).toFixed(0)}k</span>
            {tooltip.data.fraud_score != null && <>
              <span>GNN Score:</span><span style={{ fontFamily: 'var(--font-mono)', fontWeight: 600, color: tooltip.data.fraud_score > 0.8 ? '#ef4444' : '#f59e0b' }}>{(tooltip.data.fraud_score * 100).toFixed(1)}%</span>
            </>}
            <span>Pattern:</span><span style={{ fontWeight: 500 }}>{tooltip.data.primary_pattern_type || '—'}</span>
          </div>
          <div style={{ marginTop: 6, fontSize: 10, color: '#64748b' }}>Click to investigate →</div>
        </div>
      )}
    </div>
  );
}

