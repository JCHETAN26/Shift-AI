const j = (url) => fetch(url).then((r) => r.json());

export const getOverview = () => j("/api/overview");
export const getQuality = () => j("/api/quality");
export const getDrift = () => j("/api/drift");
export const explainDrift = (table) => j(`/api/drift/explain?table=${encodeURIComponent(table)}`);
export const explainRecon = (table) => j(`/api/recon/explain?table=${encodeURIComponent(table)}`);
export const searchCatalog = (q, limit = 6) =>
  j(`/api/catalog/search?q=${encodeURIComponent(q)}&limit=${limit}`);
export const getProfile = (table, column) =>
  j(`/api/catalog/profile?table=${encodeURIComponent(table)}&column=${encodeURIComponent(column)}`);

export const fmt = (n) => (n == null ? "—" : Number(n).toLocaleString());
