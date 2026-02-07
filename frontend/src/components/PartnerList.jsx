import { useState, useMemo } from 'react';

export default function PartnerList({ partners, selectedPartner, onSelect }) {
  const [search, setSearch] = useState('');
  const [filter, setFilter] = useState('all'); // all | fraud | clean

  const filtered = useMemo(() => {
    let list = partners;
    if (filter === 'fraud') list = list.filter(p => p.is_fraudulent);
    if (filter === 'clean') list = list.filter(p => !p.is_fraudulent);
    if (search) {
      const q = search.toLowerCase();
      list = list.filter(p =>
        p.partner_id.toLowerCase().includes(q) ||
        (p.entity_name || '').toLowerCase().includes(q)
      );
    }
    return list;
  }, [partners, filter, search]);

  return (
    <div style={{
      width: 280, flexShrink: 0, background: 'var(--bg-secondary)',
      borderRight: '1px solid var(--border)', display: 'flex',
      flexDirection: 'column', overflow: 'hidden',
    }}>
      {/* Search */}
      <div style={{ padding: '12px 12px 8px' }}>
        <input
          type="text"
          placeholder="Search partners..."
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            width: '100%', padding: '8px 12px', fontSize: 12,
            background: 'var(--bg-card)', border: '1px solid var(--border)',
            borderRadius: 8, color: 'var(--text-primary)', outline: 'none',
          }}
        />
      </div>

      {/* Filter tabs */}
      <div style={{ display: 'flex', padding: '0 12px 8px', gap: 4 }}>
        {['all', 'fraud', 'clean'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            style={{
              flex: 1, padding: '4px 0', fontSize: 10, fontWeight: 600,
              textTransform: 'uppercase', letterSpacing: '0.05em',
              borderRadius: 6, border: 'none', cursor: 'pointer',
              background: filter === f ? (f === 'fraud' ? 'rgba(239,68,68,0.15)' : 'var(--bg-card)') : 'transparent',
              color: filter === f ? (f === 'fraud' ? 'var(--danger)' : 'var(--text-primary)') : 'var(--text-muted)',
            }}
          >
            {f === 'all' ? `All (${partners.length})` :
             f === 'fraud' ? `Fraud (${partners.filter(p => p.is_fraudulent).length})` :
             `Clean (${partners.filter(p => !p.is_fraudulent).length})`}
          </button>
        ))}
      </div>

      {/* List */}
      <div style={{ flex: 1, overflow: 'auto', padding: '0 8px 8px' }}>
        {filtered.map(p => {
          const isSelected = selectedPartner?.partner_id === p.partner_id;
          return (
            <button
              key={p.partner_id}
              onClick={() => onSelect(p)}
              style={{
                width: '100%', padding: '10px 12px', marginBottom: 2,
                textAlign: 'left', cursor: 'pointer',
                borderRadius: 8, border: 'none',
                background: isSelected ? 'var(--bg-hover)' : 'transparent',
                borderLeft: isSelected ? '3px solid var(--accent)' : '3px solid transparent',
                color: 'var(--text-primary)', transition: 'all 0.15s',
              }}
              onMouseEnter={e => { if (!isSelected) e.currentTarget.style.background = 'var(--bg-card)'; }}
              onMouseLeave={e => { if (!isSelected) e.currentTarget.style.background = 'transparent'; }}
            >
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={{ fontSize: 12, fontWeight: 600, fontFamily: 'var(--font-mono)' }}>
                  {p.partner_id}
                </span>
                {p.is_fraudulent && (
                  <span style={{
                    width: 8, height: 8, borderRadius: '50%',
                    background: 'var(--danger)', flexShrink: 0,
                    boxShadow: '0 0 6px var(--danger-glow)',
                  }} />
                )}
              </div>
              <div style={{ fontSize: 11, color: 'var(--text-secondary)', marginTop: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {p.entity_name || 'Unknown'}
              </div>
              <div style={{ fontSize: 10, color: 'var(--text-muted)', marginTop: 2 }}>
                {p.num_referred_clients} clients · {p.primary_pattern_type || 'N/A'}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

