import React, { useState } from "react";
import Overview from "./views/Overview.jsx";
import Drift from "./views/Drift.jsx";
import Quality from "./views/Quality.jsx";
import Catalog from "./views/Catalog.jsx";

const TABS = [
  ["overview", "Migration Overview", Overview],
  ["drift", "Schema Drift", Drift],
  ["quality", "Data Quality", Quality],
  ["catalog", "AI Data Catalog", Catalog],
];

export default function App() {
  const [tab, setTab] = useState("overview");
  const View = TABS.find(([k]) => k === tab)[2];
  return (
    <div className="app">
      <header>
        <h1>Shift.ai</h1>
        <span className="tag">PostgreSQL → Snowflake migration dashboard</span>
      </header>
      <nav>
        {TABS.map(([k, label]) => (
          <button key={k} className={tab === k ? "active" : ""} onClick={() => setTab(k)}>
            {label}
          </button>
        ))}
      </nav>
      <View />
    </div>
  );
}
