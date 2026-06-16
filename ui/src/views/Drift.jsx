import React, { useEffect, useState } from "react";
import { getDrift, explainDrift } from "../api.js";

const SEV_BADGE = { BREAKING: "red", AMBIGUOUS: "yellow", NON_BREAKING: "green" };

export default function Drift() {
  const [events, setEvents] = useState(null);
  const [selected, setSelected] = useState(null);
  const [analysis, setAnalysis] = useState(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => { getDrift().then((d) => setEvents(d.events)); }, []);

  const onSelect = async (table) => {
    setSelected(table);
    setAnalysis(null);
    setLoading(true);
    setAnalysis(await explainDrift(table));
    setLoading(false);
  };

  if (!events) return <p className="spinner">Loading…</p>;

  return (
    <>
      <div className="panel">
        <h2>Detected schema changes — click a row for the AI impact analysis</h2>
        <table>
          <thead>
            <tr><th>Severity</th><th>Table</th><th>Column</th><th>Change</th><th>Detail</th></tr>
          </thead>
          <tbody>
            {events.map((e) => (
              <tr key={e.id} className="clickable" onClick={() => onSelect(e.table_name)}>
                <td><span className={`badge ${SEV_BADGE[e.severity] || "blue"}`}>{e.severity}</span></td>
                <td className="mono">{e.table_name}</td>
                <td className="mono">{e.column_name}</td>
                <td>{e.change_type}</td>
                <td className="muted">{e.detail}</td>
              </tr>
            ))}
            {events.length === 0 && (
              <tr><td colSpan="5" className="muted">No drift detected — all schemas healthy.</td></tr>
            )}
          </tbody>
        </table>
      </div>

      {selected && (
        <div className="panel">
          <h2>AI impact analysis — {selected}</h2>
          {loading && <p className="spinner">Asking Claude to analyze the impact…</p>}
          {analysis && !analysis.error && (
            <div className="ai-box">
              <div className="k">Impact</div>
              <div className="v">{analysis.impact_summary}</div>
              <div className="k">Affected downstream models</div>
              <div className="v mono">{(analysis.affected_downstream_models || []).join(", ") || "none"}</div>
              <div className="k">Recommended action</div>
              <div className="v">{analysis.recommended_action}</div>
              <div className="k">Severity</div>
              <div className="v">
                <span className={`badge ${analysis.severity === "HIGH" ? "red" : analysis.severity === "MEDIUM" ? "yellow" : "green"}`}>
                  {analysis.severity}
                </span>
              </div>
            </div>
          )}
          {analysis && analysis.error && <p className="muted">{analysis.error}</p>}
        </div>
      )}
    </>
  );
}
