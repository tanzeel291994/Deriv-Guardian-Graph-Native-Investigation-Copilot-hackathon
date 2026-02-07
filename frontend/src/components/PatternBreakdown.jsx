import { useRef, useEffect, useState } from 'react';

const PATTERN_COLORS = {
  'FAN-IN': '#3b82f6',
  'SCATTER-GATHER': '#a855f7',
  'GATHER-SCATTER': '#f59e0b',
  'CYCLE': '#06b6d4',
  'BIPARTITE': '#ec4899',
  'UNKNOWN': '#64748b',
};

const RISK_COLORS = {
  CRITICAL: '#ef4444',
  HIGH: '#f97316',
  MEDIUM: '#f59e0b',
  LOW: '#22c55e',
};

const VECTOR_COLORS = {
  clean_trades: '#22c55e',
  opposite_trades: '#ef4444',
  bonus_abuse_trades: '#f59e0b',
  other_fraud_trades: '#a855f7',
};

const VECTOR_LABELS = {
  clean_trades: 'Clean Trades',
  opposite_trades: 'Opposite Trading',
  bonus_abuse_trades: 'Bonus Abuse',
  other_fraud_trades: 'Other Fraud',
};

export default function PatternBreakdown({ macroData }) {
  if (!macroData) return null;

  const { pattern_breakdown, risk_distribution, attack_vectors, ring_patterns, summary, model_performance } = macroData;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
      {/* Attack Vectors — Donut */}
      <Card title="Attack Vectors" subtitle="Trade-level fraud type breakdown">
        <DonutChart data={attack_vectors} colors={VECTOR_COLORS} labels={VECTOR_LABELS} total={attack_vectors.total_trades} />
      </Card>

      {/* Risk Distribution — Horizontal bars */}
      <Card title="Risk Distribution" subtitle="Partners by GNN risk tier">
        <RiskBars data={risk_distribution} total={summary.total_partners} />
      </Card>

      {/* Fraud Pattern Types — Horizontal bars */}
      <Card title="Fraud Ring Patterns" subtitle="Types of detected fraud schemes">
        <PatternBars data={pattern_breakdown} />
      </Card>

      {/* Model Performance */}
      <Card title="Kumo GNN Performance" subtitle="Model evaluation metrics">
        <ModelStats data={model_performance} summary={summary} />
      </Card>
    </div>
  );
}

// ── Sub-components ──────────────────────────────────────────────────────────

function Card({ title, subtitle, children }) {
  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderRadius: 10, padding: 16,
    }}>
      <h4 style={{ fontSize: 13, fontWeight: 700, marginBottom: 2 }}>{title}</h4>
      <p style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 12 }}>{subtitle}</p>
      {children}
    </div>
  );
}

function DonutChart({ data, colors, labels, total }) {
  const canvasRef = useRef();

  useEffect(() => {
    if (!canvasRef.current) return;
    const canvas = canvasRef.current;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    const size = 140;
    canvas.width = size * dpr;
    canvas.height = size * dpr;
    canvas.style.width = `${size}px`;
    canvas.style.height = `${size}px`;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, size, size);

    const cx = size / 2, cy = size / 2, outerR = 60, innerR = 38;
    let angle = -Math.PI / 2;

    const entries = Object.entries(data).filter(([k]) => k !== 'total_trades' && data[k] > 0);
    const sum = entries.reduce((s, [, v]) => s + v, 0) || 1;

    entries.forEach(([key, value]) => {
      const slice = (value / sum) * Math.PI * 2;
      ctx.beginPath();
      ctx.arc(cx, cy, outerR, angle, angle + slice);
      ctx.arc(cx, cy, innerR, angle + slice, angle, true);
      ctx.closePath();
      ctx.fillStyle = colors[key] || '#64748b';
      ctx.fill();
      angle += slice;
    });

    // Center text
    ctx.font = 'bold 18px JetBrains Mono, monospace';
    ctx.fillStyle = '#f1f5f9';
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(total >= 1000 ? `${(total / 1000).toFixed(1)}k` : total, cx, cy - 6);
    ctx.font = '9px Inter, sans-serif';
    ctx.fillStyle = '#64748b';
    ctx.fillText('total trades', cx, cy + 10);
  }, [data, colors, total]);

  const entries = Object.entries(data).filter(([k]) => k !== 'total_trades');

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
      <canvas ref={canvasRef} />
      <div style={{ flex: 1 }}>
        {entries.map(([key, value]) => (
          <div key={key} style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
            <span style={{
              width: 8, height: 8, borderRadius: 2, flexShrink: 0,
              background: colors[key] || '#64748b',
            }} />
            <span style={{ fontSize: 11, color: 'var(--text-secondary)', flex: 1 }}>{labels[key] || key}</span>
            <span style={{ fontSize: 11, fontFamily: 'var(--font-mono)', fontWeight: 600 }}>{value.toLocaleString()}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function RiskBars({ data, total }) {
  const tiers = ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'];
  const maxVal = Math.max(...Object.values(data), 1);

  return (
    <div>
      {tiers.map(tier => {
        const count = data[tier] || 0;
        const pct = total > 0 ? ((count / total) * 100).toFixed(1) : 0;
        return (
          <div key={tier} style={{ marginBottom: 8 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 3 }}>
              <span style={{ fontWeight: 600, color: RISK_COLORS[tier] }}>{tier}</span>
              <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
                {count} ({pct}%)
              </span>
            </div>
            <div style={{ height: 6, background: 'var(--bg-primary)', borderRadius: 3, overflow: 'hidden' }}>
              <div style={{
                height: '100%', borderRadius: 3,
                width: `${(count / maxVal) * 100}%`,
                background: RISK_COLORS[tier],
                transition: 'width 0.5s ease',
              }} />
            </div>
          </div>
        );
      })}
    </div>
  );
}

function PatternBars({ data }) {
  const entries = Object.entries(data).sort((a, b) => b[1] - a[1]);
  const maxVal = Math.max(...entries.map(([, v]) => v), 1);

  return (
    <div>
      {entries.map(([pattern, count]) => (
        <div key={pattern} style={{ marginBottom: 8 }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 11, marginBottom: 3 }}>
            <span style={{ fontWeight: 600, color: PATTERN_COLORS[pattern] || '#94a3b8' }}>{pattern}</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: 'var(--text-secondary)' }}>
              {count} partners
            </span>
          </div>
          <div style={{ height: 6, background: 'var(--bg-primary)', borderRadius: 3, overflow: 'hidden' }}>
            <div style={{
              height: '100%', borderRadius: 3,
              width: `${(count / maxVal) * 100}%`,
              background: PATTERN_COLORS[pattern] || '#94a3b8',
              transition: 'width 0.5s ease',
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function ModelStats({ data, summary }) {
  // data comes from evaluation_report.json with nested keys: all_accounts, partners
  const allMetrics = data?.all_accounts || {};
  const partnerMetrics = data?.partners || {};

  const items = [
    { label: 'Fraud Partners', value: `${summary.fraud_partners} / ${summary.total_partners}`, color: '#ef4444' },
    { label: 'Fraud Rings', value: summary.total_fraud_rings, color: '#f59e0b' },
    { label: 'Fraud Volume', value: `$${(summary.fraud_trade_volume / 1e6).toFixed(2)}M`, color: '#a855f7' },
  ];

  // Use partner-specific metrics if available, fallback to all_accounts
  const m = partnerMetrics.accuracy ? partnerMetrics : allMetrics;
  const label = partnerMetrics.accuracy ? '(Partners)' : '(All)';

  if (m.accuracy) items.push({ label: `Accuracy ${label}`, value: `${(m.accuracy * 100).toFixed(1)}%`, color: '#3b82f6' });
  if (m.precision) items.push({ label: `Precision ${label}`, value: `${(m.precision * 100).toFixed(1)}%`, color: '#06b6d4' });
  if (m.recall) items.push({ label: `Recall ${label}`, value: `${(m.recall * 100).toFixed(1)}%`, color: '#22c55e' });
  if (m.f1_score) items.push({ label: `F1 Score ${label}`, value: `${(m.f1_score * 100).toFixed(1)}%`, color: '#f59e0b' });
  if (m.auc_roc) items.push({ label: `AUC-ROC ${label}`, value: `${(m.auc_roc * 100).toFixed(1)}%`, color: '#ec4899' });

  return (
    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
      {items.map(({ label, value, color }) => (
        <div key={label} style={{
          padding: '6px 10px', background: 'var(--bg-primary)', borderRadius: 6,
        }}>
          <div style={{ fontSize: 9, color: 'var(--text-muted)', marginBottom: 1 }}>{label}</div>
          <div style={{ fontSize: 14, fontWeight: 700, fontFamily: 'var(--font-mono)', color }}>{value}</div>
        </div>
      ))}
    </div>
  );
}

