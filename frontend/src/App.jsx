import { useState, useEffect } from 'react';
import { api } from './api';
import Dashboard from './components/Dashboard';
import GraphView from './components/GraphView';
import Timeline from './components/Timeline';
import Copilot from './components/Copilot';
import PartnerList from './components/PartnerList';
import PatternLab from './components/PatternLab';

const TABS = [
  { id: 'dashboard', label: '🛡️ Dashboard', desc: 'Fraud Overview' },
  { id: 'investigate', label: '🔍 Investigate', desc: 'Partner Deep-Dive' },
  { id: 'lab', label: '📘 Pattern Lab', desc: 'Fraud Topology Reference' },
];

export default function App() {
  const [stats, setStats] = useState(null);
  const [partners, setPartners] = useState([]);
  const [selectedPartner, setSelectedPartner] = useState(null);
  const [graphData, setGraphData] = useState(null);
  const [showCopilot, setShowCopilot] = useState(false);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [activeTab, setActiveTab] = useState('dashboard');

  // Load initial data
  useEffect(() => {
    async function init() {
      try {
        const [statsData, partnersData] = await Promise.all([
          api.getStats(),
          api.getPartners(false, 200),
        ]);
        setStats(statsData);
        setPartners(partnersData.partners);
        setLoading(false);
      } catch (e) {
        setError(e.message);
        setLoading(false);
      }
    }
    init();
  }, []);

  // Load graph when partner selected
  useEffect(() => {
    if (!selectedPartner) {
      setGraphData(null);
      setShowCopilot(false);
      return;
    }
    async function loadGraph() {
      try {
        const data = await api.getPartnerGraph(selectedPartner.partner_id);
        setGraphData(data);
      } catch (e) {
        console.error('Failed to load graph:', e);
      }
    }
    loadGraph();
  }, [selectedPartner]);

  // When a partner is selected from Dashboard, switch to investigate tab
  function handlePartnerSelect(p) {
    setSelectedPartner(p);
    setShowCopilot(false);
    setActiveTab('investigate');
  }

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', flexDirection: 'column', gap: '16px' }}>
        <div style={{ width: 48, height: 48, border: '3px solid var(--border)', borderTop: '3px solid var(--accent)', borderRadius: '50%', animation: 'spin 1s linear infinite' }} />
        <p style={{ color: 'var(--text-secondary)' }}>Connecting to Deriv Guardian...</p>
        <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100vh', flexDirection: 'column', gap: '16px' }}>
        <p style={{ color: 'var(--danger)', fontSize: '18px', fontWeight: 600 }}>Connection Failed</p>
        <p style={{ color: 'var(--text-secondary)', maxWidth: 500, textAlign: 'center' }}>
          Make sure the API is running:<br />
          <code style={{ color: 'var(--cyan)', fontFamily: 'var(--font-mono)' }}>
            uvicorn pipeline.api:app --reload --port 8000
          </code>
        </p>
        <p style={{ color: 'var(--text-muted)', fontSize: 13 }}>{error}</p>
      </div>
    );
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
      {/* Header */}
      <header style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        padding: '10px 24px', background: 'var(--bg-secondary)',
        borderBottom: '1px solid var(--border)', flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <div style={{ width: 32, height: 32, background: 'linear-gradient(135deg, var(--accent), var(--purple))', borderRadius: 8, display: 'flex', alignItems: 'center', justifyContent: 'center', fontWeight: 800, fontSize: 16 }}>
            G
          </div>
          <div>
            <h1 style={{ fontSize: 16, fontWeight: 700, letterSpacing: '-0.02em' }}>Deriv Graph Native Guardian</h1>
            <p style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: -2 }}>Graph-Native Investigation Copilot</p>
          </div>
        </div>

        {/* Tab Navigation */}
        <div style={{ display: 'flex', gap: 2, background: 'var(--bg-primary)', borderRadius: 10, padding: 3 }}>
          {TABS.map(tab => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              style={{
                padding: '7px 16px', border: 'none', borderRadius: 8, cursor: 'pointer',
                fontSize: 12, fontWeight: 600, transition: 'all 0.15s',
                background: activeTab === tab.id ? 'var(--bg-card)' : 'transparent',
                color: activeTab === tab.id ? 'var(--text-primary)' : 'var(--text-muted)',
                boxShadow: activeTab === tab.id ? '0 1px 3px rgba(0,0,0,0.3)' : 'none',
              }}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {stats && (
          <div style={{ display: 'flex', gap: 24, fontSize: 12 }}>
            <StatBadge label="Partners" value={stats.total_partners} />
            <StatBadge label="Fraud Rings" value={stats.total_fraud_rings} color="var(--danger)" />
            <StatBadge label="Opposite Trades" value={stats.opposite_trades} color="var(--warning)" />
          </div>
        )}
      </header>

      {/* Main Content */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>

        {/* ── TAB: Dashboard ── */}
        {activeTab === 'dashboard' && (
          <Dashboard stats={stats} partners={partners} onSelect={handlePartnerSelect} />
        )}

        {/* ── TAB: Investigate ── */}
        {activeTab === 'investigate' && (
          <>
            {/* Left Panel — Partner List */}
            <PartnerList
              partners={partners}
              selectedPartner={selectedPartner}
              onSelect={(p) => { setSelectedPartner(p); setShowCopilot(false); }}
            />

            {/* Center — Graph + Timeline */}
            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
              {selectedPartner && graphData ? (
                <>
                  {/* Scene 1: Network Graph */}
                  <div style={{ flex: 1, position: 'relative', minHeight: 0 }}>
                    <GraphView
                      data={graphData}
                      partner={selectedPartner}
                      onNodeClick={(nodeId) => {
                        if (nodeId === selectedPartner.partner_id) {
                          setShowCopilot(true);
                        }
                      }}
                    />
                    {/* Graph overlay info */}
                    <div style={{
                      position: 'absolute', top: 16, left: 16,
                      background: 'rgba(10, 14, 23, 0.85)', backdropFilter: 'blur(8px)',
                      padding: '12px 16px', borderRadius: 10, border: '1px solid var(--border)',
                    }}>
                      <p style={{ fontSize: 13, fontWeight: 600 }}>
                        {selectedPartner.entity_name || selectedPartner.partner_id}
                      </p>
                      <p style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
                        {graphData.stats.total_nodes} nodes · {graphData.stats.total_edges} edges · {graphData.stats.fraud_clients} fraud clients
                      </p>
                      <p style={{ fontSize: 11, color: selectedPartner.is_fraudulent ? 'var(--danger)' : 'var(--success)', marginTop: 4, fontWeight: 600 }}>
                        {selectedPartner.is_fraudulent ? '⚠ FRAUDULENT PARTNER' : '✓ Clean Partner'}
                      </p>
                      {selectedPartner.primary_pattern_type && (
                        <p style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                          Pattern: <span style={{ color: '#a855f7', fontWeight: 600 }}>{selectedPartner.primary_pattern_type}</span>
                        </p>
                      )}
                    </div>
                    {/* Click hint */}
                    {!showCopilot && (
                      <div style={{
                        position: 'absolute', bottom: 16, left: '50%', transform: 'translateX(-50%)',
                        background: 'rgba(10, 14, 23, 0.85)', backdropFilter: 'blur(8px)',
                        padding: '8px 16px', borderRadius: 20, border: '1px solid var(--border)',
                        fontSize: 12, color: 'var(--text-secondary)',
                      }}>
                        Click the <span style={{ color: 'var(--danger)', fontWeight: 600 }}>Partner node</span> to open the AI Copilot
                      </div>
                    )}
                  </div>

                  {/* Scene 2: Timeline */}
                  <Timeline partnerId={selectedPartner.partner_id} />
                </>
              ) : (
                /* Placeholder when no partner selected */
                <div style={{
                  flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
                  flexDirection: 'column', gap: 12,
                }}>
                  <div style={{ fontSize: 48, opacity: 0.3 }}>🔍</div>
                  <p style={{ fontSize: 16, fontWeight: 600, color: 'var(--text-secondary)' }}>
                    Select a partner to investigate
                  </p>
                  <p style={{ fontSize: 12, color: 'var(--text-muted)', maxWidth: 400, textAlign: 'center' }}>
                    Choose a partner from the list on the left to view their network graph,
                    temporal trading patterns, and AI-generated investigation report.
                  </p>
                </div>
              )}
            </div>

            {/* Scene 3: Copilot Sidebar */}
            {showCopilot && selectedPartner && (
              <Copilot
                partnerId={selectedPartner.partner_id}
                onClose={() => setShowCopilot(false)}
              />
            )}
          </>
        )}

        {/* ── TAB: Pattern Lab ── */}
        {activeTab === 'lab' && (
          <PatternLab />
        )}
      </div>
    </div>
  );
}

function StatBadge({ label, value, color = 'var(--text-primary)' }) {
  return (
    <div style={{ textAlign: 'center' }}>
      <div style={{ fontWeight: 700, color, fontSize: 15, fontFamily: 'var(--font-mono)' }}>
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
      <div style={{ color: 'var(--text-muted)', fontSize: 10, textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</div>
    </div>
  );
}
