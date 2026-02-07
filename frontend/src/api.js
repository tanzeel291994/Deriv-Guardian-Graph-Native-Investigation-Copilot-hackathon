const API_BASE = '/api';

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`API error ${res.status}: ${await res.text()}`);
  return res.json();
}

export const api = {
  getStats: () => fetchJSON(`${API_BASE}/stats`),
  getPartners: (fraudOnly = false, limit = 200) =>
    fetchJSON(`${API_BASE}/partners?fraud_only=${fraudOnly}&limit=${limit}`),
  getPartnerDetail: (id) => fetchJSON(`${API_BASE}/partners/${id}`),
  getPartnerGraph: (id) => fetchJSON(`${API_BASE}/partners/${id}/graph`),
  getPartnerClients: (id) => fetchJSON(`${API_BASE}/partners/${id}/clients`),
  getPartnerReport: (id, quick = true, model = 'gpt-4o') =>
    fetchJSON(`${API_BASE}/partners/${id}/report?quick=${quick}&model=${model}`),
  getTimeline: (partnerId = null, fraudOnly = false, limit = 5000) => {
    const params = new URLSearchParams({ fraud_only: fraudOnly, limit });
    if (partnerId) params.set('partner_id', partnerId);
    return fetchJSON(`${API_BASE}/timeline?${params}`);
  },
  getFraudRings: (limit = 100) =>
    fetchJSON(`${API_BASE}/fraud-rings?limit=${limit}`),
  getFraudRing: (id) => fetchJSON(`${API_BASE}/fraud-rings/${id}`),
  getMacro: () => fetchJSON(`${API_BASE}/macro`),
};

