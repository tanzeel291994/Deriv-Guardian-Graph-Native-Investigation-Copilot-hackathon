import { useState, useEffect, useRef } from 'react';
import { api } from '../api';

export default function Copilot({ partnerId, onClose }) {
  const [quickSummary, setQuickSummary] = useState(null);
  const [llmReport, setLlmReport] = useState(null);
  const [displayedText, setDisplayedText] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [mode, setMode] = useState('quick'); // quick | llm
  const [loading, setLoading] = useState(true);
  const scrollRef = useRef();

  // Load quick summary immediately
  useEffect(() => {
    setLoading(true);
    setQuickSummary(null);
    setLlmReport(null);
    setDisplayedText('');
    setMode('quick');

    api.getPartnerReport(partnerId, true).then(data => {
      setQuickSummary(data);
      setLoading(false);
    }).catch(e => {
      console.error(e);
      setLoading(false);
    });
  }, [partnerId]);

  // Typewriter effect for LLM report
  useEffect(() => {
    if (!llmReport) return;
    setIsTyping(true);
    setDisplayedText('');
    let i = 0;
    const text = llmReport;
    const timer = setInterval(() => {
      if (i < text.length) {
        setDisplayedText(text.substring(0, i + 1));
        i++;
      } else {
        setIsTyping(false);
        clearInterval(timer);
      }
    }, 12);
    return () => clearInterval(timer);
  }, [llmReport]);

  // Auto-scroll during typing
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [displayedText]);

  const handleLLM = async () => {
    setMode('llm');
    setLoading(true);
    try {
      const data = await api.getPartnerReport(partnerId, false);
      setLlmReport(data.report);
    } catch (e) {
      setLlmReport(`Error generating report: ${e.message}`);
    }
    setLoading(false);
  };

  const riskColors = {
    CRITICAL: { bg: 'rgba(239, 68, 68, 0.15)', text: '#ef4444', border: '#ef4444' },
    HIGH: { bg: 'rgba(249, 115, 22, 0.15)', text: '#f97316', border: '#f97316' },
    MEDIUM: { bg: 'rgba(245, 158, 11, 0.15)', text: '#f59e0b', border: '#f59e0b' },
    LOW: { bg: 'rgba(34, 197, 94, 0.15)', text: '#22c55e', border: '#22c55e' },
  };

  return (
    <div style={{
      width: 400, flexShrink: 0, background: 'var(--bg-secondary)',
      borderLeft: '1px solid var(--border)', display: 'flex',
      flexDirection: 'column', overflow: 'hidden',
      animation: 'slideIn 0.3s ease-out',
    }}>
      <style>{`@keyframes slideIn { from { transform: translateX(100%); } to { transform: translateX(0); } }`}</style>

      {/* Header */}
      <div style={{
        padding: '16px 20px', borderBottom: '1px solid var(--border)',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
      }}>
        <div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <span style={{ fontSize: 16 }}>🤖</span>
            <span style={{ fontSize: 14, fontWeight: 700 }}>AI Copilot</span>
            {isTyping && <span style={{ fontSize: 10, color: 'var(--success)', fontWeight: 500 }}>● Generating...</span>}
          </div>
          <p style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
            Investigation report for {partnerId}
          </p>
        </div>
        <button
          onClick={onClose}
          style={{
            background: 'var(--bg-card)', border: '1px solid var(--border)',
            borderRadius: 6, width: 28, height: 28, cursor: 'pointer',
            color: 'var(--text-secondary)', fontSize: 14, display: 'flex',
            alignItems: 'center', justifyContent: 'center',
          }}
        >✕</button>
      </div>

      {/* Content */}
      <div ref={scrollRef} style={{ flex: 1, overflow: 'auto', padding: 20 }}>
        {loading && !quickSummary && (
          <div style={{ textAlign: 'center', padding: 40, color: 'var(--text-muted)' }}>
            <div style={{ fontSize: 24, marginBottom: 12 }}>🔍</div>
            Analyzing network graph...
          </div>
        )}

        {quickSummary && mode === 'quick' && (
          <div className="animate-in">
            {/* Risk Badge */}
            <div style={{
              padding: '12px 16px', borderRadius: 10, marginBottom: 16,
              background: riskColors[quickSummary.risk_level]?.bg || riskColors.LOW.bg,
              border: `1px solid ${riskColors[quickSummary.risk_level]?.border || 'var(--border)'}`,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{
                  fontSize: 13, fontWeight: 800, letterSpacing: '0.05em',
                  color: riskColors[quickSummary.risk_level]?.text,
                }}>
                  {quickSummary.risk_level} RISK
                </span>
                {quickSummary.kumo_risk_score != null && (
                  <span style={{
                    fontSize: 11, fontFamily: 'var(--font-mono)', fontWeight: 600,
                    color: 'var(--text-secondary)',
                  }}>
                    GNN Score: {(quickSummary.kumo_risk_score * 100).toFixed(1)}%
                  </span>
                )}
              </div>
              <p style={{ fontSize: 12, fontWeight: 600, marginTop: 6, color: 'var(--text-primary)' }}>
                {quickSummary.recommendation}
              </p>
            </div>

            {/* Entity */}
            <div style={{ marginBottom: 16 }}>
              <p style={{ fontSize: 15, fontWeight: 700 }}>{quickSummary.entity_name}</p>
              <p style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>{partnerId}</p>
            </div>

            {/* Evidence */}
            <Section title="Evidence">
              {quickSummary.evidence.map((e, i) => (
                <Bullet key={i} text={e} />
              ))}
            </Section>

            {/* Financial Impact */}
            <Section title="Financial Impact">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <MiniCard
                  label="Est. Fraudulent Loss"
                  value={`$${quickSummary.financial_impact.estimated_fraudulent_loss.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                  color="var(--danger)"
                />
                <MiniCard
                  label="Total Commissions"
                  value={`$${quickSummary.financial_impact.total_commissions.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                />
                <MiniCard
                  label="Trade Volume"
                  value={`$${quickSummary.financial_impact.total_trade_volume.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                />
              </div>
            </Section>

            {/* Network Stats */}
            <Section title="Network Intelligence">
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 8 }}>
                <MiniCard label="Referred Clients" value={quickSummary.network_stats.num_referred_clients} />
                <MiniCard label="Fraud Ring Clients" value={quickSummary.network_stats.num_fraud_ring_clients} color="var(--danger)" />
                <MiniCard label="Opposite Trades" value={quickSummary.network_stats.num_opposite_trades} color="var(--warning)" />
                <MiniCard label="Total Trades" value={quickSummary.network_stats.num_trades} />
              </div>
            </Section>

            {/* Timing */}
            {quickSummary.timing_analysis?.min_gap_seconds != null && (
              <Section title="Temporal Analysis">
                <div style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.6 }}>
                  Paired trades executed within{' '}
                  <span style={{ color: 'var(--danger)', fontWeight: 600 }}>
                    {quickSummary.timing_analysis.min_gap_seconds.toFixed(0)}s
                  </span>
                  {' '}–{' '}
                  <span style={{ color: 'var(--warning)', fontWeight: 600 }}>
                    {quickSummary.timing_analysis.max_gap_seconds.toFixed(0)}s
                  </span>
                  {' '}of each other
                </div>
              </Section>
            )}

            {/* Fraud Rings */}
            {quickSummary.associated_fraud_rings?.length > 0 && (
              <Section title="Fraud Rings">
                {quickSummary.associated_fraud_rings.map((r, i) => (
                  <div key={i} style={{
                    padding: '8px 12px', background: 'var(--bg-card)', borderRadius: 6,
                    marginBottom: 4, fontSize: 11,
                  }}>
                    <span style={{ fontWeight: 600 }}>Ring #{r.ring_id}</span>
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>{r.pattern_type}</span>
                    <span style={{ color: 'var(--text-muted)', marginLeft: 8 }}>{r.num_accounts} accounts</span>
                  </div>
                ))}
              </Section>
            )}

            {/* Actions */}
            <Section title="Suggested Actions">
              {quickSummary.suggested_actions.map((a, i) => (
                <Bullet key={i} text={a} color="var(--accent)" />
              ))}
            </Section>
          </div>
        )}

        {/* LLM Report */}
        {mode === 'llm' && displayedText && (
          <div className="animate-in" style={{
            fontSize: 13, lineHeight: 1.7, color: 'var(--text-secondary)',
            fontFamily: 'var(--font-sans)', whiteSpace: 'pre-wrap',
          }}>
            {displayedText}
            {isTyping && <span style={{ color: 'var(--accent)', animation: 'blink 0.8s infinite' }}>▊</span>}
            <style>{`@keyframes blink { 0%,100% { opacity: 1; } 50% { opacity: 0; } }`}</style>
          </div>
        )}
      </div>

      {/* Footer — Generate LLM Report button */}
      <div style={{ padding: '12px 20px', borderTop: '1px solid var(--border)' }}>
        {mode === 'quick' ? (
          <button
            onClick={handleLLM}
            disabled={loading}
            style={{
              width: '100%', padding: '10px 16px', fontSize: 13, fontWeight: 600,
              background: 'linear-gradient(135deg, var(--accent), var(--purple))',
              border: 'none', borderRadius: 8, color: 'white', cursor: 'pointer',
              opacity: loading ? 0.5 : 1, transition: 'opacity 0.2s',
            }}
          >
            🤖 Generate Deep AI Investigation Report
          </button>
        ) : (
          <button
            onClick={() => setMode('quick')}
            style={{
              width: '100%', padding: '10px 16px', fontSize: 13, fontWeight: 600,
              background: 'var(--bg-card)', border: '1px solid var(--border)',
              borderRadius: 8, color: 'var(--text-secondary)', cursor: 'pointer',
            }}
          >
            ← Back to Quick Summary
          </button>
        )}
      </div>
    </div>
  );
}

function Section({ title, children }) {
  return (
    <div style={{ marginBottom: 16 }}>
      <h4 style={{ fontSize: 11, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.08em', color: 'var(--text-muted)', marginBottom: 8 }}>
        {title}
      </h4>
      {children}
    </div>
  );
}

function Bullet({ text, color = 'var(--warning)' }) {
  return (
    <div style={{ display: 'flex', gap: 8, marginBottom: 6, fontSize: 12, lineHeight: 1.5 }}>
      <span style={{ color, flexShrink: 0, marginTop: 2 }}>•</span>
      <span style={{ color: 'var(--text-secondary)' }}>{text}</span>
    </div>
  );
}

function MiniCard({ label, value, color = 'var(--text-primary)' }) {
  return (
    <div style={{ padding: '8px 10px', background: 'var(--bg-card)', borderRadius: 6 }}>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: 14, fontWeight: 700, color, fontFamily: 'var(--font-mono)' }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
    </div>
  );
}

