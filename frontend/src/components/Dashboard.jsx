import { useState, useEffect } from 'react';
import { api } from '../api';
import BubbleChart from './BubbleChart';
import PatternBreakdown from './PatternBreakdown';

export default function Dashboard({ stats, partners, onSelect }) {
  const [macroData, setMacroData] = useState(null);
  const [macroLoading, setMacroLoading] = useState(true);

  useEffect(() => {
    api.getMacro()
      .then(data => { setMacroData(data); setMacroLoading(false); })
      .catch(e => { console.error('Macro load error:', e); setMacroLoading(false); });
  }, []);

  if (!stats) return null;

  const fraudPartners = partners.filter(p => p.is_fraudulent);
  const topFraud = fraudPartners.slice(0, 6);

  const handleBubbleClick = (partnerData) => {
    const match = partners.find(p => p.partner_id === partnerData.partner_id);
    if (match) onSelect(match);
  };

  return (
    <div style={{ flex: 1, overflow: 'auto', padding: 32 }} className="animate-in">
      <div style={{ maxWidth: 1100, margin: '0 auto' }}>
        {/* Header */}
        <h2 style={{ fontSize: 28, fontWeight: 800, letterSpacing: '-0.03em', marginBottom: 4 }}>
          Fraud Intelligence Overview
        </h2>
        <p style={{ color: 'var(--text-secondary)', fontSize: 14, marginBottom: 24 }}>
          Powered by Kumo.ai Graph Neural Network · Real-time partner risk analysis
        </p>

        {/* Top Stat Cards */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12, marginBottom: 24 }}>
          <StatCard label="Total Partners" value={stats.total_partners} icon="👥" />
          <StatCard label="Fraud Partners" value={stats.fraud_partners} icon="🚨" color="var(--danger)" sub={`${stats.fraud_partner_pct}%`} />
          <StatCard label="Fraud Rings" value={stats.total_fraud_rings} icon="🕸️" color="var(--warning)" />
          <StatCard label="Opposite Trades" value={stats.opposite_trades} icon="🔄" color="var(--purple)" />
          <StatCard label="Bonus Abuse" value={stats.bonus_abuse_trades} icon="💎" color="#06b6d4" />
        </div>

        {/* ══════════ MACRO VIEW: Risk Landscape ══════════ */}
        <div style={{
          background: 'var(--bg-card)', border: '1px solid var(--border)',
          borderRadius: 12, padding: 20, marginBottom: 24,
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4 }}>
            <span style={{ fontSize: 16 }}>🌐</span>
            <h3 style={{ fontSize: 16, fontWeight: 700 }}>Global Fraud Landscape</h3>
          </div>
          <p style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 16 }}>
            "I see the specific cases, but what's the big picture? What types of attacks are hitting us?"
          </p>

          {macroLoading ? (
            <div style={{ height: 380, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <p style={{ color: 'var(--text-muted)' }}>Loading fraud landscape...</p>
            </div>
          ) : macroData ? (
            <>
              {/* Bubble Chart */}
              <BubbleChart data={macroData.bubble_chart} onPartnerClick={handleBubbleClick} />

              {/* Legend */}
              <div style={{ display: 'flex', gap: 20, justifyContent: 'center', marginTop: 12, marginBottom: 8 }}>
                {[
                  { label: 'Critical', color: '#ef4444' },
                  { label: 'High', color: '#f97316' },
                  { label: 'Medium', color: '#f59e0b' },
                  { label: 'Low', color: '#22c55e' },
                ].map(l => (
                  <div key={l.label} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 11 }}>
                    <span style={{
                      width: 10, height: 10, borderRadius: '50%', background: l.color,
                      boxShadow: `0 0 6px ${l.color}40`,
                    }} />
                    <span style={{ color: 'var(--text-secondary)' }}>{l.label}</span>
                  </div>
                ))}
                <div style={{ fontSize: 11, color: 'var(--text-muted)', borderLeft: '1px solid var(--border)', paddingLeft: 12 }}>
                  ⬤ size = trade volume
                </div>
              </div>
            </>
          ) : null}
        </div>

        {/* ══════════ PATTERN BREAKDOWN ══════════ */}
        {macroData && (
          <div style={{ marginBottom: 24 }}>
            <PatternBreakdown macroData={macroData} />
          </div>
        )}

        {/* ══════════ TOP FLAGGED PARTNERS ══════════ */}
        <h3 style={{ fontSize: 16, fontWeight: 700, marginBottom: 12 }}>
          🔴 Top Flagged Partners
        </h3>
        <p style={{ color: 'var(--text-secondary)', fontSize: 12, marginBottom: 12 }}>
          "Standard systems see {stats.total_clients} separate users. Our system sees coordinated rings."
        </p>

        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 10 }}>
          {topFraud.map((p) => (
            <button
              key={p.partner_id}
              onClick={() => onSelect(p)}
              style={{
                background: 'var(--bg-card)', border: '1px solid var(--border)',
                borderRadius: 10, padding: 14, textAlign: 'left', cursor: 'pointer',
                transition: 'all 0.2s', color: 'var(--text-primary)',
              }}
              onMouseEnter={e => { e.currentTarget.style.borderColor = 'var(--danger)'; e.currentTarget.style.background = 'var(--bg-hover)'; }}
              onMouseLeave={e => { e.currentTarget.style.borderColor = 'var(--border)'; e.currentTarget.style.background = 'var(--bg-card)'; }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 6 }}>
                <span style={{ fontWeight: 700, fontSize: 13, fontFamily: 'var(--font-mono)' }}>{p.partner_id}</span>
                <span style={{
                  fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 12,
                  background: 'rgba(239, 68, 68, 0.15)', color: 'var(--danger)',
                  letterSpacing: '0.04em',
                }}>FRAUD</span>
              </div>
              <p style={{ fontSize: 11, color: 'var(--text-secondary)', marginBottom: 4 }}>
                {p.entity_name || 'Unknown Entity'}
              </p>
              <div style={{ display: 'flex', gap: 12, fontSize: 10, color: 'var(--text-muted)' }}>
                <span>{p.num_referred_clients} clients</span>
                <span>${(p.total_trade_volume || 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                {p.primary_pattern_type && <span style={{ color: '#a855f7' }}>{p.primary_pattern_type}</span>}
              </div>
            </button>
          ))}
        </div>

        {/* CTA */}
        <div style={{
          marginTop: 24, padding: 20, background: 'var(--bg-card)', borderRadius: 12,
          border: '1px solid var(--border)', textAlign: 'center',
        }}>
          <p style={{ fontSize: 14, fontWeight: 600, marginBottom: 6 }}>
            Select a partner from the list or click a bubble to investigate
          </p>
          <p style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
            View the network graph, timeline intelligence, and AI-generated investigation reports
          </p>
        </div>
      </div>
    </div>
  );
}

function StatCard({ label, value, icon, color = 'var(--text-primary)', sub }) {
  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderRadius: 10, padding: '14px 16px',
    }}>
      <div style={{ fontSize: 11, color: 'var(--text-muted)', marginBottom: 6 }}>
        {icon} {label}
      </div>
      <div style={{ fontSize: 22, fontWeight: 800, color, fontFamily: 'var(--font-mono)', letterSpacing: '-0.02em' }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
      {sub && <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>{sub}</div>}
    </div>
  );
}
