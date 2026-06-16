import React, { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend,
} from "recharts";
import { fmt } from "../api.js";

const STATUS_BADGE = {
  HEALTHY: "green", RECONCILED: "green",
  DRIFTED: "yellow", COUNT_MISMATCH: "red",
  CHECKSUM_MISMATCH: "red", SAMPLE_MISMATCH: "yellow", PENDING: "blue",
};

export default function Overview() {
  const [data, setData] = useState(null);

  // Live updates over SSE (falls back to the initial payload if the stream drops).
  useEffect(() => {
    const es = new EventSource("/api/stream/progress");
    es.addEventListener("progress", (e) => setData(JSON.parse(e.data)));
    es.onerror = () => es.close();
    return () => es.close();
  }, []);

  if (!data) return <p className="spinner">Connecting to live migration stream…</p>;

  const chart = data.tables.map((t) => ({
    table: t.table, source: t.source_rows, target: t.target_rows,
  }));

  return (
    <>
      <div className="cards">
        <div className="card">
          <div className="label">Overall status</div>
          <div className="value">
            <span className={`badge ${STATUS_BADGE[data.overall_status] || "blue"}`}>
              {data.overall_status}
            </span>
          </div>
        </div>
        <div className="card">
          <div className="label">Tables reconciled</div>
          <div className="value">{data.tables_reconciled}/{data.tables_total}</div>
        </div>
        <div className="card">
          <div className="label">Source rows</div>
          <div className="value">{fmt(data.total_source_rows)}</div>
        </div>
        <div className="card">
          <div className="label">Target rows (Snowflake)</div>
          <div className="value">{fmt(data.total_target_rows)}</div>
        </div>
      </div>

      <div className="panel">
        <h2>Row counts — source vs target</h2>
        <ResponsiveContainer width="100%" height={260}>
          <BarChart data={chart}>
            <CartesianGrid strokeDasharray="3 3" stroke="#2d333b" />
            <XAxis dataKey="table" stroke="#8b949e" />
            <YAxis stroke="#8b949e" />
            <Tooltip contentStyle={{ background: "#161b22", border: "1px solid #2d333b" }}
                     formatter={(v) => fmt(v)} />
            <Legend />
            <Bar dataKey="source" fill="#58a6ff" name="Source (Postgres)" />
            <Bar dataKey="target" fill="#3fb950" name="Target (Snowflake)" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="panel">
        <h2>Per-table reconciliation</h2>
        <table>
          <thead>
            <tr>
              <th>Table</th><th>Source</th><th>Target</th><th>Δ</th>
              <th>Checksum</th><th>Status</th><th>Last synced</th>
            </tr>
          </thead>
          <tbody>
            {data.tables.map((t) => (
              <tr key={t.table}>
                <td className="mono">{t.table}</td>
                <td>{fmt(t.source_rows)}</td>
                <td>{fmt(t.target_rows)}</td>
                <td>{t.delta === 0 ? "0" : `+${fmt(t.delta)}`}</td>
                <td>{t.checksum_match == null ? "—" : t.checksum_match ? "✓" : "✗"}</td>
                <td><span className={`badge ${STATUS_BADGE[t.status] || "blue"}`}>{t.status}</span></td>
                <td className="muted">{t.last_synced ? new Date(t.last_synced).toLocaleString() : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
