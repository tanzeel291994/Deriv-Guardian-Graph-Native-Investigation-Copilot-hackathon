import { useRef, useCallback, useEffect, useState } from 'react';
import ForceGraph2D from 'react-force-graph-2d';

export default function GraphView({ data, partner, onNodeClick }) {
  const graphRef = useRef();
  const containerRef = useRef();
  const [dimensions, setDimensions] = useState({ width: 800, height: 500 });

  // Responsive sizing
  useEffect(() => {
    function updateSize() {
      if (containerRef.current) {
        setDimensions({
          width: containerRef.current.offsetWidth,
          height: containerRef.current.offsetHeight,
        });
      }
    }
    updateSize();
    window.addEventListener('resize', updateSize);
    return () => window.removeEventListener('resize', updateSize);
  }, []);

  // Center on partner node after mount
  useEffect(() => {
    if (graphRef.current && data) {
      setTimeout(() => {
        graphRef.current.zoomToFit(400, 50);
      }, 500);
    }
  }, [data]);

  // Build graph data for force-graph
  const graphData = useGraphData(data);

  const nodeCanvasObject = useCallback((node, ctx, globalScale) => {
    const isPartner = node.type === 'partner';
    const isFraud = node.is_fraudulent;
    const size = isPartner ? 14 : 6;
    const fontSize = Math.max(10 / globalScale, 3);

    // Glow for fraud nodes
    if (isFraud) {
      ctx.shadowColor = isPartner ? '#ef4444' : '#f97316';
      ctx.shadowBlur = isPartner ? 20 : 8;
    }

    // Draw node circle
    ctx.beginPath();
    ctx.arc(node.x, node.y, size, 0, 2 * Math.PI);
    ctx.fillStyle = node.color || (isPartner ? '#3b82f6' : '#6b7280');
    ctx.fill();

    // Partner ring
    if (isPartner) {
      ctx.lineWidth = 2;
      ctx.strokeStyle = isFraud ? '#ef4444' : '#3b82f6';
      ctx.stroke();
    }

    ctx.shadowColor = 'transparent';
    ctx.shadowBlur = 0;

    // Label
    if (globalScale > 0.8 || isPartner) {
      ctx.font = `${isPartner ? 'bold' : ''} ${fontSize}px Inter, sans-serif`;
      ctx.textAlign = 'center';
      ctx.textBaseline = 'top';
      ctx.fillStyle = isPartner ? '#f1f5f9' : 'rgba(148, 163, 184, 0.8)';
      const label = isPartner ? (node.label || node.id) : node.id;
      ctx.fillText(label, node.x, node.y + size + 2);
    }
  }, []);

  const linkCanvasObject = useCallback((link, ctx) => {
    const isOpposite = link.type === 'opposite_trade';
    ctx.beginPath();

    const start = link.source;
    const end = link.target;
    if (!start.x || !end.x) return;

    if (isOpposite) {
      // Dashed red line for opposite trades
      ctx.setLineDash([4, 4]);
      ctx.strokeStyle = 'rgba(239, 68, 68, 0.5)';
      ctx.lineWidth = 1.5;
    } else {
      ctx.setLineDash([]);
      ctx.strokeStyle = 'rgba(100, 116, 139, 0.2)';
      ctx.lineWidth = 0.5;
    }

    ctx.moveTo(start.x, start.y);
    ctx.lineTo(end.x, end.y);
    ctx.stroke();
    ctx.setLineDash([]);
  }, []);

  const handleNodeClick = useCallback((node) => {
    if (onNodeClick) onNodeClick(node.id);
  }, [onNodeClick]);

  if (!graphData) return null;

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', background: 'var(--bg-primary)' }}>
      <ForceGraph2D
        ref={graphRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={graphData}
        nodeCanvasObject={nodeCanvasObject}
        linkCanvasObjectMode={() => 'replace'}
        linkCanvasObject={linkCanvasObject}
        onNodeClick={handleNodeClick}
        nodePointerAreaPaint={(node, color, ctx) => {
          const size = node.type === 'partner' ? 14 : 6;
          ctx.beginPath();
          ctx.arc(node.x, node.y, size + 4, 0, 2 * Math.PI);
          ctx.fillStyle = color;
          ctx.fill();
        }}
        cooldownTicks={100}
        d3AlphaDecay={0.04}
        d3VelocityDecay={0.2}
        backgroundColor="transparent"
      />
    </div>
  );
}

function useGraphData(data) {
  if (!data) return null;

  const nodes = data.nodes.map(n => ({
    ...n,
    // Force-graph expects 'id'
    fx: n.type === 'partner' ? 0 : undefined,
    fy: n.type === 'partner' ? 0 : undefined,
  }));

  const links = data.edges.map(e => ({
    source: e.source,
    target: e.target,
    type: e.type,
    color: e.color,
  }));

  return { nodes, links };
}

