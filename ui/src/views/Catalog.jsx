import React, { useState } from "react";
import { searchCatalog, getProfile, fmt } from "../api.js";

const EXAMPLES = [
  "which table has customer purchase history",
  "where are email addresses stored",
  "order revenue and totals",
  "product stock levels in warehouses",
];

export default function Catalog() {
  const [q, setQ] = useState(EXAMPLES[0]);
  const [results, setResults] = useState(null);
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(false);

  const run = async (query) => {
    setQ(query);
    setProfile(null);
    setLoading(true);
    const r = await searchCatalog(query, 6);
    setResults(r.results);
    setLoading(false);
  };

  const openProfile = async (table, column) => {
    if (!column) return;
    setProfile(await getProfile(table, column));
  };

  return (
    <>
      <div className="panel">
        <h2>Natural-language data catalog (RAG over Qdrant)</h2>
        <div className="search">
          <input
            value={q}
            onChange={(e) => setQ(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && run(q)}
            placeholder="Ask in plain English…"
          />
          <button className="go" onClick={() => run(q)}>Search</button>
        </div>
        <div className="muted" style={{ fontSize: 13, marginBottom: 8 }}>
          Try:{" "}
          {EXAMPLES.map((e) => (
            <a key={e} onClick={() => run(e)} style={{ color: "var(--accent)", cursor: "pointer", marginRight: 12 }}>
              {e}
            </a>
          ))}
        </div>

        {loading && <p className="spinner">Embedding query and searching…</p>}
        {results && (
          <table>
            <thead>
              <tr><th>Score</th><th>Table</th><th>Column</th><th>Type</th><th>Samples</th></tr>
            </thead>
            <tbody>
              {results.map((r, i) => (
                <tr key={i} className={r.column ? "clickable" : ""} onClick={() => openProfile(r.table, r.column)}>
                  <td className="score">{r.score}</td>
                  <td className="mono">{r.table}</td>
                  <td className="mono">{r.column || <span className="muted">(table)</span>}</td>
                  <td>{r.data_type || "—"}</td>
                  <td className="muted">{(r.sample_values || []).join(", ")}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {profile && !profile.error && (
        <div className="panel">
          <h2>Column profile — {profile.table_name}.{profile.column_name}</h2>
          <div className="cards">
            <div className="card"><div className="label">Data type</div><div className="value mono" style={{ fontSize: 18 }}>{profile.data_type}</div></div>
            <div className="card"><div className="label">Distinct</div><div className="value">{fmt(profile.distinct_count)}</div></div>
            <div className="card"><div className="label">Null rate</div><div className="value">{(profile.null_rate * 100).toFixed(2)}%</div></div>
            <div className="card"><div className="label">Rows</div><div className="value">{fmt(profile.row_count)}</div></div>
          </div>
          <p><span className="muted">min:</span> <span className="mono">{String(profile.min_value)}</span>{"   "}
             <span className="muted">max:</span> <span className="mono">{String(profile.max_value)}</span>
             {profile.mean_value != null && <> {"   "}<span className="muted">mean:</span> <span className="mono">{Number(profile.mean_value).toFixed(2)}</span></>}</p>
          <p><span className="muted">samples:</span> <span className="mono">{(profile.sample_values || []).join(", ")}</span></p>
        </div>
      )}
    </>
  );
}
