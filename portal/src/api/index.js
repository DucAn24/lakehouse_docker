const CATALOG_BASE = '/api/catalog';
const CHAT_BASE    = '/api/chat';

async function fetchJSON(url) {
  const res = await fetch(url);
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  return res.json();
}

async function postJSON(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status}: ${text}`);
  }
  return res.json();
}

// ---- Catalog API ----
export const catalogAPI = {
  tables:       (layer) => fetchJSON(`${CATALOG_BASE}/tables${layer ? `?layer=${layer}` : ''}`),
  tableDetail:  (layer, table) => fetchJSON(`${CATALOG_BASE}/tables/${layer}/${table}`),
  tableSample:  (layer, table) => fetchJSON(`${CATALOG_BASE}/tables/${layer}/${table}/sample`),
  search:       (q) => fetchJSON(`${CATALOG_BASE}/search?q=${encodeURIComponent(q)}`),
  lineage:      () => fetchJSON(`${CATALOG_BASE}/lineage`),
  tableLineage: (table) => fetchJSON(`${CATALOG_BASE}/lineage/${table}`),
  stats:        () => fetchJSON(`${CATALOG_BASE}/stats`),
};

// ---- Chat API (existing fastapi service) ----
export const chatAPI = {
  send:        (message, conversation_id = 'default') =>
    postJSON(`${CHAT_BASE}/chat`, { message, conversation_id }),
  history:     (conversation_id = 'default') =>
    fetchJSON(`${CHAT_BASE}/chat/history?conversation_id=${conversation_id}`),
  reset:       (conversation_id = 'default') =>
    postJSON(`${CHAT_BASE}/chat/reset`, { conversation_id }),
  suggestions: () =>
    fetchJSON(`${CHAT_BASE}/chat/suggestions`),
};
