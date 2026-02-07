import { useState, useRef, useEffect, useMemo } from 'react';
import { api } from '../api';

// ── Pattern Definitions ─────────────────────────────────────────────────────

const PATTERNS = [
  {
    id: 'fan-in',
    name: 'Fan-In',
    subtitle: 'Many → One',
    icon: '🎯',
    color: '#3b82f6',
    glow: 'rgba(59, 130, 246, 0.3)',
    description: 'Multiple accounts send money to a single hub account. This mirrors how fraudulent clients funnel commissions to one partner.',
    howGNNDetects: 'The GNN identifies nodes with abnormally high in-degree (many incoming edges). Kumo\'s message-passing learns that a node receiving from 20+ unique sources in a short window is structurally different from organic referral patterns.',
    affiliateContext: 'A fraudulent Partner registers 20+ fake "Clients." Each client deposits and trades, generating commissions that all flow back to the Partner. The fan-in shape IS the commission funnel.',
    generate: (cx, cy, r) => {
      const hub = { x: cx, y: cy, type: 'hub', label: 'Partner' };
      const spokes = [];
      const edges = [];
      const n = 12;
      for (let i = 0; i < n; i++) {
        const angle = (i / n) * Math.PI * 2 - Math.PI / 2;
        const node = { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r, type: 'leaf', label: `C${i + 1}` };
        spokes.push(node);
        edges.push({ from: node, to: hub, type: 'flow' });
      }
      return { nodes: [hub, ...spokes], edges };
    },
  },
  {
    id: 'fan-out',
    name: 'Fan-Out',
    subtitle: 'One → Many',
    icon: '💥',
    color: '#f97316',
    glow: 'rgba(249, 115, 22, 0.3)',
    description: 'A single account distributes money to many recipient accounts. The reverse of Fan-In — one source, many targets.',
    howGNNDetects: 'High out-degree nodes stand out in the graph embedding. The GNN learns that rapid, equal-amount distributions from a single source correlate strongly with money laundering layering.',
    affiliateContext: 'The Partner receives a large payout, then rapidly distributes it back to fake client accounts to create the illusion of legitimate trading activity. This "spraying" pattern is a layering technique.',
    generate: (cx, cy, r) => {
      const hub = { x: cx, y: cy, type: 'hub', label: 'Partner' };
      const spokes = [];
      const edges = [];
      const n = 12;
      for (let i = 0; i < n; i++) {
        const angle = (i / n) * Math.PI * 2 - Math.PI / 2;
        const node = { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r, type: 'leaf', label: `C${i + 1}` };
        spokes.push(node);
        edges.push({ from: hub, to: node, type: 'flow' });
      }
      return { nodes: [hub, ...spokes], edges };
    },
  },
  {
    id: 'scatter-gather',
    name: 'Scatter-Gather',
    subtitle: 'Distribute → Collect',
    icon: '🌀',
    color: '#a855f7',
    glow: 'rgba(168, 85, 247, 0.3)',
    description: 'Money is scattered from one source to many intermediaries, then re-gathered into a different destination. A two-phase laundering operation.',
    howGNNDetects: 'The GNN detects temporal 2-hop paths: Source → Intermediaries → Sink, where timestamps are sequential. The embedding captures the "scatter" phase and "gather" phase as correlated neighborhood structures.',
    affiliateContext: 'Partner A distributes funds to 15 clients. Those clients then trade and generate commissions — which flow to Partner B (or back to A under a different identity). The GNN sees the coordinated timing.',
    generate: (cx, cy, r) => {
      const source = { x: cx - r * 1.3, y: cy, type: 'hub', label: 'Source' };
      const sink = { x: cx + r * 1.3, y: cy, type: 'hub', label: 'Sink' };
      const mids = [];
      const edges = [];
      const n = 6;
      for (let i = 0; i < n; i++) {
        const angle = ((i / n) * Math.PI - Math.PI / 2) * 0.8;
        const node = { x: cx + Math.cos(angle) * r * 0.45, y: cy + Math.sin(angle) * r * 0.7, type: 'mid', label: `M${i + 1}` };
        mids.push(node);
        edges.push({ from: source, to: node, type: 'scatter' });
        edges.push({ from: node, to: sink, type: 'gather' });
      }
      return { nodes: [source, sink, ...mids], edges };
    },
  },
  {
    id: 'gather-scatter',
    name: 'Gather-Scatter',
    subtitle: 'Collect → Distribute',
    icon: '🔀',
    color: '#f59e0b',
    glow: 'rgba(245, 158, 11, 0.3)',
    description: 'The inverse of Scatter-Gather. Money from many sources is gathered into one point, then redistributed. A consolidation-then-layering pattern.',
    howGNNDetects: 'Kumo identifies the "funnel → spray" signature: high in-degree followed by high out-degree at the same node within a time window. The temporal ordering of edges is critical — incoming before outgoing.',
    affiliateContext: 'Multiple fake clients deposit funds (gather phase). The Partner collects commissions and then redistributes to other accounts for withdrawal (scatter phase). This is the most common affiliate fraud topology in our dataset.',
    generate: (cx, cy, r) => {
      const hub = { x: cx, y: cy, type: 'hub', label: 'Hub' };
      const leftNodes = [];
      const rightNodes = [];
      const edges = [];
      const nL = 5, nR = 5;
      for (let i = 0; i < nL; i++) {
        const angle = Math.PI + ((i / (nL - 1)) * Math.PI * 0.6 - Math.PI * 0.3);
        const node = { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r * 0.7, type: 'leaf', label: `S${i + 1}` };
        leftNodes.push(node);
        edges.push({ from: node, to: hub, type: 'gather' });
      }
      for (let i = 0; i < nR; i++) {
        const angle = (i / (nR - 1)) * Math.PI * 0.6 - Math.PI * 0.3;
        const node = { x: cx + Math.cos(angle) * r, y: cy + Math.sin(angle) * r * 0.7, type: 'leaf', label: `D${i + 1}` };
        rightNodes.push(node);
        edges.push({ from: hub, to: node, type: 'scatter' });
      }
      return { nodes: [hub, ...leftNodes, ...rightNodes], edges };
    },
  },
  {
    id: 'cycle',
    name: 'Cycle',
    subtitle: 'Circular Flow',
    icon: '🔁',
    color: '#06b6d4',
    glow: 'rgba(6, 182, 212, 0.3)',
    description: 'Money flows in a closed loop: A→B→C→D→A. The same funds circle back to the origin, inflating transaction counts without real economic activity.',
    howGNNDetects: 'Cycle detection is where GNNs truly shine over rule-based systems. With 3+ message-passing layers, the GNN\'s embedding for node A contains information from A→B→C→D→A — it literally "sees" itself in its own neighborhood.',
    affiliateContext: 'Partner creates 4 accounts. Each account trades and passes funds to the next, generating commissions at each hop. The money returns to the start, but 4 commission events were logged. Pure artificial volume.',
    generate: (cx, cy, r) => {
      const n = 6;
      const nodes = [];
      const edges = [];
      for (let i = 0; i < n; i++) {
        const angle = (i / n) * Math.PI * 2 - Math.PI / 2;
        nodes.push({ x: cx + Math.cos(angle) * r * 0.7, y: cy + Math.sin(angle) * r * 0.7, type: i === 0 ? 'hub' : 'mid', label: i === 0 ? 'Origin' : `N${i}` });
      }
      for (let i = 0; i < n; i++) {
        edges.push({ from: nodes[i], to: nodes[(i + 1) % n], type: 'cycle' });
      }
      return { nodes, edges };
    },
  },
  {
    id: 'bipartite',
    name: 'Bipartite',
    subtitle: 'Two-Group Trading',
    icon: '⚡',
    color: '#ec4899',
    glow: 'rgba(236, 72, 153, 0.3)',
    description: 'Two distinct groups of accounts trade exclusively with each other. Group A only transacts with Group B and vice versa — creating an unnatural partition in the graph.',
    howGNNDetects: 'The GNN identifies communities with abnormally segregated connectivity. In a bipartite fraud ring, the learned embeddings for Group A and Group B are distinctly clustered but densely cross-connected — a signature that never occurs in organic trading.',
    affiliateContext: 'Partner controls two sets of accounts. Set A places BUY orders, Set B places SELL orders on the same instrument at the same time (Opposite Trading). The bipartite structure IS the mirror trading mechanism.',
    generate: (cx, cy, r) => {
      const nodes = [];
      const edges = [];
      const nA = 5, nB = 5;
      const groupA = [], groupB = [];
      for (let i = 0; i < nA; i++) {
        const node = { x: cx - r * 0.65, y: cy + ((i - (nA - 1) / 2) / (nA - 1)) * r * 1.3, type: 'hub', label: `A${i + 1}` };
        groupA.push(node);
        nodes.push(node);
      }
      for (let i = 0; i < nB; i++) {
        const node = { x: cx + r * 0.65, y: cy + ((i - (nB - 1) / 2) / (nB - 1)) * r * 1.3, type: 'leaf', label: `B${i + 1}` };
        groupB.push(node);
        nodes.push(node);
      }
      // Cross-connect
      for (let a of groupA) {
        for (let b of groupB) {
          if (Math.random() > 0.5) {
            edges.push({ from: a, to: b, type: 'trade' });
          }
        }
      }
      // Ensure at least some edges
      if (edges.length < 6) {
        for (let i = 0; i < nA; i++) {
          edges.push({ from: groupA[i], to: groupB[i % nB], type: 'trade' });
        }
      }
      return { nodes, edges };
    },
  },
  {
    id: 'opposite-trading',
    name: 'Opposite Trading',
    subtitle: 'Mirror BUY/SELL',
    icon: '🪞',
    color: '#ef4444',
    glow: 'rgba(239, 68, 68, 0.3)',
    description: 'Two accounts controlled by the same partner place perfectly mirrored trades: one BUYs, the other SELLs the same instrument at nearly the same time. Guaranteed to generate commissions with minimal net market risk.',
    howGNNDetects: 'The GNN detects correlated temporal features across connected nodes: same instrument, opposite direction, near-identical timestamps. The direction correlation (-0.99) between paired clients under one partner is a strong learned signal.',
    affiliateContext: 'This is the primary attack vector in our dataset. Partner registers Client A and Client B. At timestamp T, Client A buys 1.0 lot EURUSD, Client B sells 1.02 lots EURUSD. One wins, one loses — but the Partner earns commission on BOTH trades.',
    generate: (cx, cy, r) => {
      const partner = { x: cx, y: cy - r * 0.5, type: 'hub', label: 'Partner' };
      const buyClient = { x: cx - r * 0.7, y: cy + r * 0.3, type: 'buy', label: 'Client A\n(BUY)' };
      const sellClient = { x: cx + r * 0.7, y: cy + r * 0.3, type: 'sell', label: 'Client B\n(SELL)' };
      const market = { x: cx, y: cy + r * 0.9, type: 'market', label: 'EURUSD\nMarket' };
      return {
        nodes: [partner, buyClient, sellClient, market],
        edges: [
          { from: partner, to: buyClient, type: 'referral' },
          { from: partner, to: sellClient, type: 'referral' },
          { from: buyClient, to: market, type: 'buy' },
          { from: sellClient, to: market, type: 'sell' },
          { from: buyClient, to: sellClient, type: 'mirror' },
        ],
      };
    },
  },
];

// ── Main Component ──────────────────────────────────────────────────────────

export default function PatternLab() {
  const [selectedPattern, setSelectedPattern] = useState(PATTERNS[0]);
  const [macroData, setMacroData] = useState(null);
  const [animPhase, setAnimPhase] = useState(0);

  useEffect(() => {
    api.getMacro().then(setMacroData).catch(console.error);
  }, []);

  // Animation ticker
  useEffect(() => {
    const interval = setInterval(() => {
      setAnimPhase(prev => (prev + 1) % 360);
    }, 50);
    return () => clearInterval(interval);
  }, []);

  const realStats = useMemo(() => {
    if (!macroData) return null;
    const { pattern_breakdown, ring_patterns, attack_vectors, summary } = macroData;
    return { pattern_breakdown, ring_patterns, attack_vectors, summary };
  }, [macroData]);

  return (
    <div style={{ flex: 1, display: 'flex', overflow: 'hidden' }} className="animate-in">
      {/* Left: Pattern Selector */}
      <div style={{
        width: 240, flexShrink: 0, background: 'var(--bg-secondary)',
        borderRight: '1px solid var(--border)', overflow: 'auto', padding: '16px 0',
      }}>
        <div style={{ padding: '0 16px', marginBottom: 16 }}>
          <h3 style={{ fontSize: 14, fontWeight: 700, marginBottom: 4 }}>📘 Pattern Lab</h3>
          <p style={{ fontSize: 10, color: 'var(--text-muted)', lineHeight: 1.5 }}>
            Interactive fraud topology reference. Learn what the GNN detects.
          </p>
        </div>

        {PATTERNS.map(p => (
          <button
            key={p.id}
            onClick={() => setSelectedPattern(p)}
            style={{
              display: 'flex', alignItems: 'center', gap: 10, width: '100%',
              padding: '10px 16px', border: 'none', cursor: 'pointer',
              background: selectedPattern.id === p.id ? 'var(--bg-hover)' : 'transparent',
              borderLeft: selectedPattern.id === p.id ? `3px solid ${p.color}` : '3px solid transparent',
              color: 'var(--text-primary)', textAlign: 'left',
              transition: 'all 0.15s',
            }}
          >
            <span style={{ fontSize: 18 }}>{p.icon}</span>
            <div>
              <div style={{ fontSize: 12, fontWeight: 600, color: selectedPattern.id === p.id ? p.color : 'var(--text-primary)' }}>
                {p.name}
              </div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)' }}>{p.subtitle}</div>
            </div>
          </button>
        ))}

        {/* Stats summary at bottom */}
        {realStats && (
          <div style={{ padding: '16px', borderTop: '1px solid var(--border)', marginTop: 16 }}>
            <h4 style={{ fontSize: 10, textTransform: 'uppercase', color: 'var(--text-muted)', letterSpacing: '0.05em', marginBottom: 8 }}>
              Our Dataset
            </h4>
            <div style={{ fontSize: 11, lineHeight: 1.8, color: 'var(--text-secondary)' }}>
              <div><strong style={{ color: 'var(--text-primary)' }}>{realStats.summary.total_partners}</strong> partners analyzed</div>
              <div><strong style={{ color: '#ef4444' }}>{realStats.summary.fraud_partners}</strong> flagged as fraudulent</div>
              <div><strong style={{ color: '#f59e0b' }}>{realStats.summary.total_fraud_rings}</strong> fraud rings detected</div>
              <div><strong style={{ color: '#a855f7' }}>{realStats.attack_vectors.opposite_trades}</strong> opposite trades</div>
            </div>
          </div>
        )}
      </div>

      {/* Right: Pattern Detail */}
      <div style={{ flex: 1, overflow: 'auto', padding: 32 }}>
        <PatternDetail
          pattern={selectedPattern}
          animPhase={animPhase}
          realStats={realStats}
        />
      </div>
    </div>
  );
}

// ── Pattern Detail View ─────────────────────────────────────────────────────

function PatternDetail({ pattern, animPhase, realStats }) {
  return (
    <div style={{ maxWidth: 900, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 16, marginBottom: 24 }}>
        <div style={{
          width: 56, height: 56, borderRadius: 14, display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 28, background: pattern.glow, border: `2px solid ${pattern.color}`,
        }}>
          {pattern.icon}
        </div>
        <div>
          <h2 style={{ fontSize: 24, fontWeight: 800, letterSpacing: '-0.02em' }}>
            {pattern.name} <span style={{ fontSize: 16, fontWeight: 400, color: 'var(--text-muted)' }}>— {pattern.subtitle}</span>
          </h2>
          <p style={{ fontSize: 13, color: 'var(--text-secondary)', marginTop: 2 }}>
            {pattern.description}
          </p>
        </div>
      </div>

      {/* Canvas Visualization */}
      <div style={{
        background: 'var(--bg-card)', border: '1px solid var(--border)',
        borderRadius: 12, overflow: 'hidden', marginBottom: 24,
      }}>
        <div style={{ padding: '10px 16px', borderBottom: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
            Idealized Topology
          </span>
          <span style={{ fontSize: 10, color: pattern.color, fontWeight: 600 }}>
            ● LIVE ANIMATION
          </span>
        </div>
        <TopologyCanvas pattern={pattern} animPhase={animPhase} />
      </div>

      {/* Two-column: How GNN Detects + Affiliate Context */}
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 24 }}>
        <InfoCard
          icon="🧠"
          title="How the GNN Detects This"
          text={pattern.howGNNDetects}
          accentColor={pattern.color}
        />
        <InfoCard
          icon="🏢"
          title="Affiliate Fraud Context"
          text={pattern.affiliateContext}
          accentColor={pattern.color}
        />
      </div>

      {/* Real Data Stats */}
      {realStats && (
        <RealDataBadge pattern={pattern} stats={realStats} />
      )}
    </div>
  );
}

// ── Topology Canvas ─────────────────────────────────────────────────────────

function TopologyCanvas({ pattern, animPhase }) {
  const canvasRef = useRef();
  const containerRef = useRef();
  const [dims, setDims] = useState({ width: 800, height: 400 });
  const graphRef = useRef(null);

  useEffect(() => {
    function update() {
      if (containerRef.current) {
        setDims({ width: containerRef.current.offsetWidth, height: 400 });
      }
    }
    update();
    window.addEventListener('resize', update);
    return () => window.removeEventListener('resize', update);
  }, []);

  // Generate graph once per pattern
  useEffect(() => {
    const { width, height } = dims;
    const cx = width / 2;
    const cy = height / 2;
    const r = Math.min(width, height) * 0.35;
    graphRef.current = pattern.generate(cx, cy, r);
  }, [pattern, dims]);

  // Draw each frame
  useEffect(() => {
    if (!canvasRef.current || !graphRef.current) return;
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

    const { nodes, edges } = graphRef.current;
    const t = animPhase / 360;

    // Draw edges with animated flow
    edges.forEach((edge, idx) => {
      const fx = edge.from.x, fy = edge.from.y;
      const tx = edge.to.x, ty = edge.to.y;

      // Base line
      ctx.beginPath();
      ctx.moveTo(fx, fy);
      ctx.lineTo(tx, ty);
      ctx.strokeStyle = edge.type === 'mirror' ? 'rgba(239, 68, 68, 0.25)' : 'rgba(100, 116, 139, 0.15)';
      ctx.lineWidth = edge.type === 'mirror' ? 2 : 1;
      if (edge.type === 'mirror') {
        ctx.setLineDash([4, 4]);
      }
      ctx.stroke();
      ctx.setLineDash([]);

      // Animated particle
      const particleT = ((t * 2 + idx * 0.12) % 1);
      const px = fx + (tx - fx) * particleT;
      const py = fy + (ty - fy) * particleT;

      let particleColor = pattern.color;
      if (edge.type === 'buy') particleColor = '#22c55e';
      if (edge.type === 'sell') particleColor = '#ef4444';
      if (edge.type === 'mirror') particleColor = '#f59e0b';

      ctx.beginPath();
      ctx.arc(px, py, 3, 0, Math.PI * 2);
      ctx.fillStyle = particleColor;
      ctx.shadowColor = particleColor;
      ctx.shadowBlur = 8;
      ctx.fill();
      ctx.shadowBlur = 0;

      // Arrow head on the line
      const angle = Math.atan2(ty - fy, tx - fx);
      const arrowPos = 0.65;
      const ax = fx + (tx - fx) * arrowPos;
      const ay = fy + (ty - fy) * arrowPos;
      const arrowLen = 6;
      ctx.beginPath();
      ctx.moveTo(ax, ay);
      ctx.lineTo(ax - arrowLen * Math.cos(angle - 0.4), ay - arrowLen * Math.sin(angle - 0.4));
      ctx.moveTo(ax, ay);
      ctx.lineTo(ax - arrowLen * Math.cos(angle + 0.4), ay - arrowLen * Math.sin(angle + 0.4));
      ctx.strokeStyle = 'rgba(100,116,139,0.3)';
      ctx.lineWidth = 1.5;
      ctx.stroke();
    });

    // Draw nodes
    nodes.forEach((node) => {
      const pulse = 1 + Math.sin(animPhase * 0.05 + node.x * 0.01) * 0.08;
      let r = 16;
      let fillColor = 'rgba(100,116,139,0.5)';
      let strokeColor = 'rgba(100,116,139,0.7)';
      let textColor = '#f1f5f9';

      if (node.type === 'hub') {
        r = 22 * pulse;
        fillColor = pattern.color;
        strokeColor = pattern.color;
        ctx.shadowColor = pattern.color;
        ctx.shadowBlur = 16;
      } else if (node.type === 'buy') {
        r = 18 * pulse;
        fillColor = '#22c55e';
        strokeColor = '#22c55e';
        ctx.shadowColor = '#22c55e';
        ctx.shadowBlur = 10;
      } else if (node.type === 'sell') {
        r = 18 * pulse;
        fillColor = '#ef4444';
        strokeColor = '#ef4444';
        ctx.shadowColor = '#ef4444';
        ctx.shadowBlur = 10;
      } else if (node.type === 'market') {
        r = 20;
        fillColor = 'rgba(100,116,139,0.3)';
        strokeColor = '#64748b';
        ctx.shadowBlur = 0;
      } else if (node.type === 'mid') {
        r = 14 * pulse;
        fillColor = `${pattern.color}80`;
        strokeColor = pattern.color;
        ctx.shadowColor = pattern.color;
        ctx.shadowBlur = 6;
      } else {
        r = 10;
        ctx.shadowBlur = 0;
      }

      // Node circle
      ctx.beginPath();
      ctx.arc(node.x, node.y, r, 0, Math.PI * 2);
      ctx.fillStyle = fillColor;
      ctx.globalAlpha = 0.9;
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.strokeStyle = strokeColor;
      ctx.lineWidth = 2;
      ctx.stroke();
      ctx.shadowBlur = 0;

      // Label
      const lines = node.label.split('\n');
      ctx.font = `${node.type === 'hub' || node.type === 'market' ? 'bold ' : ''}${node.type === 'leaf' ? 8 : 9}px Inter, sans-serif`;
      ctx.fillStyle = textColor;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      lines.forEach((line, i) => {
        ctx.fillText(line, node.x, node.y + (i - (lines.length - 1) / 2) * 11);
      });
    });

  }, [pattern, animPhase, dims]);

  return (
    <div ref={containerRef}>
      <canvas ref={canvasRef} style={{ width: '100%', display: 'block' }} />
    </div>
  );
}

// ── Info Card ───────────────────────────────────────────────────────────────

function InfoCard({ icon, title, text, accentColor }) {
  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderRadius: 10, padding: 16,
      borderTop: `3px solid ${accentColor}`,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 8 }}>
        <span style={{ fontSize: 16 }}>{icon}</span>
        <h4 style={{ fontSize: 13, fontWeight: 700 }}>{title}</h4>
      </div>
      <p style={{ fontSize: 12, color: 'var(--text-secondary)', lineHeight: 1.7 }}>
        {text}
      </p>
    </div>
  );
}

// ── Real Data Badge ─────────────────────────────────────────────────────────

function RealDataBadge({ pattern, stats }) {
  // Map pattern IDs to real data keys
  const patternToKey = {
    'fan-in': 'FAN-IN',
    'fan-out': 'FAN-OUT',
    'scatter-gather': 'SCATTER-GATHER',
    'gather-scatter': 'GATHER-SCATTER',
    'cycle': 'CYCLE',
    'bipartite': 'BIPARTITE',
    'opposite-trading': null,
  };

  const key = patternToKey[pattern.id];
  const ringCount = key ? (stats.ring_patterns[key] || 0) : 0;
  const partnerCount = key ? (stats.pattern_breakdown[key] || 0) : 0;

  const isOpposite = pattern.id === 'opposite-trading';

  return (
    <div style={{
      background: 'var(--bg-card)', border: '1px solid var(--border)',
      borderRadius: 10, padding: 16,
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12 }}>
        <span style={{ fontSize: 14 }}>📊</span>
        <h4 style={{ fontSize: 13, fontWeight: 700 }}>In Our Dataset</h4>
        <span style={{ fontSize: 10, background: `${pattern.color}20`, color: pattern.color, padding: '2px 8px', borderRadius: 8, fontWeight: 600 }}>
          REAL DATA
        </span>
      </div>

      {isOpposite ? (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
          <DataStat label="Opposite Trades" value={stats.attack_vectors.opposite_trades} color="#ef4444" />
          <DataStat label="Bonus Abuse Trades" value={stats.attack_vectors.bonus_abuse_trades} color="#f59e0b" />
          <DataStat label="% of All Trades" value={`${((stats.attack_vectors.opposite_trades / stats.attack_vectors.total_trades) * 100).toFixed(1)}%`} color="#a855f7" />
        </div>
      ) : key ? (
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 12 }}>
          <DataStat label="Fraud Rings" value={ringCount} color={pattern.color} />
          <DataStat label="Fraud Partners" value={partnerCount} color={pattern.color} />
          <DataStat label="% of Rings" value={`${((ringCount / Math.max(stats.summary.total_fraud_rings, 1)) * 100).toFixed(1)}%`} color={pattern.color} />
        </div>
      ) : (
        <p style={{ fontSize: 12, color: 'var(--text-muted)' }}>No direct ring mapping for this pattern type.</p>
      )}
    </div>
  );
}

function DataStat({ label, value, color }) {
  return (
    <div style={{ background: 'var(--bg-primary)', borderRadius: 8, padding: '10px 14px', textAlign: 'center' }}>
      <div style={{ fontSize: 22, fontWeight: 800, fontFamily: 'var(--font-mono)', color }}>{value}</div>
      <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>{label}</div>
    </div>
  );
}

