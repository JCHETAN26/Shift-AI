import React, { useEffect, useState } from "react";
import { getQuality, explainRecon, fmt } from "../api.js";

export default function Quality() {
  const [data, setData] = useState(null);
  const [explain, setExplain] = useState({});

  useEffect(() => { getQuality().then(setData); }, []);

  const onExplain = async (table) => {
    setExplain((s) => ({ ...s, [table]: { loading: true } }));
    const r = await explainRecon(table);
    setExplain((s) => ({ ...s, [table]: { loading: false, ...r } }));
  };

  if (!data) return <p className="spinner">Loading…</p>;

  return (
    <>
      {data.suites.map((s) => (
        <div className="panel" key={s.suite}>
          <h2>
            {s.suite}{" "}
            <span className={`badge ${s.success ? "green" : "red"}`}>
              {s.passed}/{s.total} {s.success ? "PASS" : "FAIL"}
            </span>
          </h2>
          <table>
            <thead>
              <tr><th>Expectation</th><th>Table</th><th>Severity</th><th>Result</th><th>Detail</th></tr>
            </thead>
            <tbody>
              {s.expectations.map((e, i) => (
                <tr key={i}>
                  <td className="mono">{e.expectation}</td>
                  <td>{e.table_name}</td>
                  <td>{e.severity}</td>
                  <td><span className={`badge ${e.success ? "green" : "red"}`}>{e.success ? "✓" : "✗"}</span></td>
                  <td className="muted">{e.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}

      <div className="panel">
        <h2>Reconciliation report</h2>
        <table>
          <thead>
            <tr><th>Table</th><th>Source</th><th>Target</th><th>Checksum</th><th>Sample</th><th>Status</th><th></th></tr>
          </thead>
          <tbody>
            {data.reconciliation.map((r) => (
              <React.Fragment key={r.table_name}>
                <tr>
                  <td className="mono">{r.table_name}</td>
                  <td>{fmt(r.source_rows)}</td>
                  <td>{fmt(r.target_rows)}</td>
                  <td>{r.checksum_match == null ? "—" : r.checksum_match ? "✓" : "✗"}</td>
                  <td>{r.sample_mismatches}/{r.sample_size}</td>
                  <td><span className={`badge ${r.status === "RECONCILED" ? "green" : "red"}`}>{r.status}</span></td>
                  <td>
                    {r.status !== "RECONCILED" && (
                      <button className="go" onClick={() => onExplain(r.table_name)}>Explain</button>
                    )}
                  </td>
                </tr>
                {explain[r.table_name] && (
                  <tr>
                    <td colSpan="7">
                      {explain[r.table_name].loading ? (
                        <span className="spinner">Asking Claude to explain the discrepancy…</span>
                      ) : (
                        <div className="ai-box">
                          <div className="k">Explanation</div>
                          <div className="v">{explain[r.table_name].explanation}</div>
                          <div className="k">Probable root cause</div>
                          <div className="v">{explain[r.table_name].probable_root_cause}</div>
                          <div className="k">Recommended action</div>
                          <div className="v">{explain[r.table_name].recommended_action}</div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}
