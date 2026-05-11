"""Local web dashboard for the live options pipeline."""

from __future__ import annotations

import datetime as dt
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parent
RAW_SYMBOLS_DB_PATH = ROOT / "rawsymbols.db"
HOST = "127.0.0.1"
PORT = 8765
STALE_AFTER_SECONDS = 30
GROUPING_ORDER = {"ATM": 0, "OTM1": 1, "OTM2": 2}
SIDE_ORDER = {"C": 0, "P": 1}
RAW_UNIVERSE_COLUMNS = [
    "parent_symbol",
    "raw_option_symbol",
    "strike",
    "expiration_date",
    "side",
    "grouping",
    "decay_bucket",
    "days_to_expiry",
]
LIVE_CONTRACT_STATE_COLUMNS = [
    "parent_symbol",
    "raw_option_symbol",
    "side",
    "grouping",
    "decay_bucket",
    "strike",
    "expiration_date",
    "days_to_expiry",
    "bid",
    "ask",
    "mid",
    "spread",
    "spread_pct",
    "rolling_volume_10m",
    "rolling_volume_30m",
    "rolling_volume_1h",
    "underlying_price",
    "current_iv",
    "mean_vol_3d",
    "std_vol_3d",
    "z_vol_3d",
    "mean_vol_35d",
    "std_vol_35d",
    "z_vol_35d",
    "mean_mid_3d",
    "std_mid_3d",
    "z_mid_3d",
    "mean_mid_35d",
    "std_mid_35d",
    "z_mid_35d",
    "mean_iv_3d",
    "std_iv_3d",
    "z_iv_3d",
    "mean_iv_35d",
    "std_iv_35d",
    "z_iv_35d",
    "last_quote_ts",
    "last_trade_ts",
    "updated_at",
]
ALERT_HISTORY_COLUMNS = [
    "alert_timestamp",
    "alert_type",
    "parent_symbol",
    "raw_option_symbol",
    "underlying_price",
    "strike",
    "side",
    "grouping",
    "decay_bucket",
    "option_mid",
    "rolling_volume_10m",
    "current_iv",
    "z_vol_35d",
    "z_vol_3d",
    "z_mid_35d",
    "z_mid_3d",
    "z_iv_35d",
    "z_iv_3d",
]


HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Live Options Dashboard</title>
  <style>
    :root {
      --bg: #0b1320;
      --panel: #121d2e;
      --panel-2: #17263c;
      --text: #e8eef7;
      --muted: #8ca0b8;
      --accent: #f2a541;
      --green: #32c48d;
      --yellow: #ffd166;
      --red: #ef476f;
      --line: #22344e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      background:
        radial-gradient(circle at top left, rgba(242,165,65,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(50,196,141,0.14), transparent 22%),
        linear-gradient(180deg, #08101b 0%, var(--bg) 100%);
      color: var(--text);
    }
    .wrap {
      max-width: 1800px;
      margin: 0 auto;
      padding: 24px;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      align-items: end;
      gap: 24px;
      margin-bottom: 18px;
    }
    .hero h1 {
      margin: 0;
      font-size: 34px;
      letter-spacing: 0.02em;
    }
    .hero p {
      margin: 8px 0 0;
      color: var(--muted);
      max-width: 900px;
      line-height: 1.45;
    }
    .stamp {
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .cards {
      display: grid;
      grid-template-columns: repeat(6, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .card {
      background: linear-gradient(180deg, rgba(255,255,255,0.03), rgba(255,255,255,0.01)), var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px 16px;
      min-height: 102px;
    }
    .card .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    .card .value {
      margin-top: 10px;
      font-size: 28px;
      font-weight: 700;
    }
    .card .sub {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
    }
    .filters, .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 16px;
      margin-bottom: 18px;
    }
    .filters-grid {
      display: grid;
      grid-template-columns: 1.2fr 1.1fr 0.7fr 0.9fr 0.9fr 1.4fr 0.8fr 1fr;
      gap: 12px;
      align-items: end;
    }
    label {
      display: block;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 6px;
      text-transform: uppercase;
      letter-spacing: 0.12em;
    }
    select, input {
      width: 100%;
      border-radius: 10px;
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--text);
      padding: 11px 12px;
      font-size: 14px;
    }
    .layout {
      display: grid;
      grid-template-columns: minmax(0, 1.9fr) minmax(360px, 0.9fr);
      gap: 18px;
    }
    .panel h2 {
      margin: 0 0 12px;
      font-size: 19px;
      letter-spacing: 0.04em;
    }
    .table-wrap {
      overflow: auto;
      border-radius: 12px;
      border: 1px solid var(--line);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1450px;
      background: rgba(0,0,0,0.15);
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      font-size: 13px;
      text-align: left;
      white-space: nowrap;
    }
    th {
      position: sticky;
      top: 0;
      background: #122035;
      color: #e6edf8;
      z-index: 1;
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    tbody tr:nth-child(even) {
      background: rgba(255,255,255,0.02);
    }
    tbody tr:hover {
      background: rgba(242,165,65,0.08);
    }
    .metric-good { color: var(--green); font-weight: 700; }
    .metric-watch { color: var(--yellow); font-weight: 700; }
    .metric-hot { color: var(--red); font-weight: 700; }
    .stale {
      color: var(--red);
      font-weight: 700;
    }
    .fresh {
      color: var(--green);
      font-weight: 700;
    }
    .muted {
      color: var(--muted);
    }
    .alerts-table {
      min-width: 100%;
    }
    .alerts-table tbody tr {
      cursor: pointer;
    }
    .empty {
      padding: 24px;
      color: var(--muted);
      text-align: center;
    }
    @media (max-width: 1280px) {
      .cards { grid-template-columns: repeat(3, minmax(180px, 1fr)); }
      .filters-grid { grid-template-columns: repeat(2, minmax(180px, 1fr)); }
      .layout { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <div class="stamp">Live Options Monitor</div>
        <h1>Quotes, Volume, Baselines, Alerts</h1>
        <p>Universe is limited to parent symbols currently in <code>rawsymbols.db</code>. Pick a parent, filter the subscribed contracts, and watch live quote/volume state against the baseline stats actually driving your alerts.</p>
      </div>
      <div class="stamp" id="last-refresh">Waiting for first refresh...</div>
    </div>

    <div class="cards" id="summary-cards"></div>

    <div class="filters">
      <div class="filters-grid">
        <div>
          <label for="parent-search">Parent Search</label>
          <input id="parent-search" type="text" placeholder="Type ticker">
        </div>
        <div>
          <label for="parent-symbol">Parent Symbol</label>
          <select id="parent-symbol"></select>
        </div>
        <div>
          <label for="side-filter">Side</label>
          <select id="side-filter">
            <option value="ALL">All</option>
          </select>
        </div>
        <div>
          <label for="grouping-filter">Money Grouping</label>
          <select id="grouping-filter">
            <option value="ALL">All</option>
          </select>
        </div>
        <div>
          <label for="decay-filter">Decay Bucket</label>
          <select id="decay-filter">
            <option value="ALL">All</option>
          </select>
        </div>
        <div>
          <label for="raw-search">Raw Symbol Search</label>
          <input id="raw-search" type="text" placeholder="Filter raw symbols">
        </div>
        <div>
          <label for="alert-limit">Alert Rows</label>
          <select id="alert-limit">
            <option value="50">50</option>
            <option value="100" selected>100</option>
            <option value="250">250</option>
            <option value="500">500</option>
          </select>
        </div>
        <div>
          <label for="alert-scope">Alert Scope</label>
          <select id="alert-scope">
            <option value="ALL" selected>All Alerts</option>
            <option value="PARENT">Selected Parent</option>
          </select>
        </div>
      </div>
    </div>

    <div class="layout">
      <div class="panel">
        <h2>Subscribed Contracts</h2>
        <div class="table-wrap">
          <table id="contracts-table">
            <thead>
              <tr>
                <th>Raw Symbol</th>
                <th>Strike</th>
                <th>Side</th>
                <th>Grouping</th>
                <th>Decay</th>
                <th>DTE</th>
                <th>Underlying</th>
                <th>Bid</th>
                <th>Ask</th>
                <th>Mid</th>
                <th>Spread %</th>
                <th>Rolling 10m Vol</th>
                <th>Rolling 30m Vol</th>
                <th>Rolling 1h Vol</th>
                <th>Vol Mean 3d</th>
                <th>Vol Std 3d</th>
                <th>Vol Z 3d</th>
                <th>Vol Mean 35d</th>
                <th>Vol Std 35d</th>
                <th>Vol Z 35d</th>
                <th>Mid Mean 3d</th>
                <th>Mid Std 3d</th>
                <th>Mid Z 3d</th>
                <th>Mid Mean 35d</th>
                <th>Mid Std 35d</th>
                <th>Mid Z 35d</th>
                <th>IV</th>
                <th>IV Mean 3d</th>
                <th>IV Std 3d</th>
                <th>IV Z 3d</th>
                <th>IV Mean 35d</th>
                <th>IV Std 35d</th>
                <th>IV Z 35d</th>
                <th>Last Quote</th>
                <th>Last Trade</th>
                <th>Freshness</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>

      <div class="panel">
        <h2>Alert History</h2>
        <div class="table-wrap">
          <table class="alerts-table" id="alerts-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>Parent</th>
                <th>Raw Symbol</th>
                <th>Underlying</th>
                <th>Strike</th>
                <th>Side</th>
                <th>Grouping</th>
                <th>Decay</th>
                <th>Mid</th>
                <th>Vol 10m</th>
                <th>IV</th>
                <th>Vol Z 35d</th>
                <th>Vol Z 3d</th>
                <th>Mid Z 35d</th>
                <th>Mid Z 3d</th>
                <th>IV Z 35d</th>
                <th>IV Z 3d</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>
  </div>

  <script>
    const state = {
      parentSymbol: "",
      side: "ALL",
      grouping: "ALL",
      decayBucket: "ALL",
      rawSearch: "",
      alertLimit: "100",
      alertScope: "ALL",
      parentSearch: "",
      allParents: [],
    };

    const parentSelect = document.getElementById("parent-symbol");
    const parentSearchInput = document.getElementById("parent-search");
    const sideSelect = document.getElementById("side-filter");
    const groupingSelect = document.getElementById("grouping-filter");
    const decaySelect = document.getElementById("decay-filter");
    const rawSearchInput = document.getElementById("raw-search");
    const alertLimitSelect = document.getElementById("alert-limit");
    const alertScopeSelect = document.getElementById("alert-scope");

    function fmtNumber(value, digits = 2) {
      if (value === null || value === undefined || value === "") return "";
      const num = Number(value);
      if (!Number.isFinite(num)) return "";
      return num.toFixed(digits);
    }

    function fmtInt(value) {
      if (value === null || value === undefined || value === "") return "";
      const num = Number(value);
      if (!Number.isFinite(num)) return "";
      return num.toLocaleString();
    }

    function zClass(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return "";
      if (num >= 2.5) return "metric-hot";
      if (num >= 1.5) return "metric-watch";
      if (num > 0) return "metric-good";
      return "";
    }

    function freshnessCell(row) {
      const seconds = Number(row.seconds_since_update);
      if (!Number.isFinite(seconds)) return '<span class="muted">no updates yet</span>';
      if (seconds > 30) return `<span class="stale">${Math.round(seconds)}s stale</span>`;
      return `<span class="fresh">${Math.round(seconds)}s</span>`;
    }

    function buildQuery() {
      const params = new URLSearchParams();
      if (state.parentSymbol) params.set("parent_symbol", state.parentSymbol);
      if (state.side && state.side !== "ALL") params.set("side", state.side);
      if (state.grouping && state.grouping !== "ALL") params.set("grouping", state.grouping);
      if (state.decayBucket && state.decayBucket !== "ALL") params.set("decay_bucket", state.decayBucket);
      if (state.rawSearch) params.set("raw_search", state.rawSearch);
      params.set("alert_limit", state.alertLimit || "100");
      params.set("alert_scope", state.alertScope || "ALL");
      return params.toString();
    }

    function populateSelect(select, values, currentValue, includeAll = true) {
      const existing = Array.from(select.options).map(opt => opt.value);
      const desired = includeAll ? ["ALL", ...values] : values.slice();
      if (JSON.stringify(existing) === JSON.stringify(desired)) {
        select.value = desired.includes(currentValue) ? currentValue : desired[0] || "";
        return;
      }

      select.innerHTML = "";
      desired.forEach(value => {
        const opt = document.createElement("option");
        opt.value = value;
        opt.textContent = value === "ALL" ? "All" : value;
        select.appendChild(opt);
      });
      select.value = desired.includes(currentValue) ? currentValue : desired[0] || "";
    }

    function matchingParent(values, searchValue) {
      const needle = searchValue.trim().toUpperCase();
      if (!needle) return null;
      return (
        values.find(value => value === needle) ||
        values.find(value => value.startsWith(needle)) ||
        values.find(value => value.includes(needle)) ||
        null
      );
    }

    function populateParentSelect(values, currentValue) {
      state.allParents = values;
      const needle = state.parentSearch.trim().toUpperCase();
      const filteredValues = needle
        ? values.filter(value => value.includes(needle))
        : values;
      const visibleValues = filteredValues.length ? filteredValues : values;
      const selectedParent = matchingParent(values, needle) || currentValue;
      populateSelect(parentSelect, visibleValues, selectedParent, false);
    }

    function renderSummary(summary) {
      const cards = [
        { label: "Selected Parent", value: summary.selected_parent || "N/A", sub: `${summary.visible_contract_rows} visible contracts` },
        { label: "Universe Parents", value: fmtInt(summary.parent_count_total), sub: `${fmtInt(summary.contract_rows_total)} contract rows` },
        { label: "Live Rows", value: fmtInt(summary.live_rows_with_updates), sub: `${fmtInt(summary.stale_rows)} stale rows` },
        { label: "Alerts Logged", value: fmtInt(summary.alerts_total), sub: `showing ${fmtInt(summary.returned_alert_rows)} rows` },
        { label: "Quote / Volume Events", value: `${fmtInt(summary.quote_events_total)} / ${fmtInt(summary.volume_events_total)}`, sub: `total events ${fmtInt(summary.events_total)}` },
        { label: "Baseline Issues", value: `${fmtInt(summary.missing_baseline_count)} / ${fmtInt(summary.invalid_baseline_count)}`, sub: `last event ${summary.last_event_ts || "N/A"}` },
      ];

      document.getElementById("summary-cards").innerHTML = cards.map(card => `
        <div class="card">
          <div class="label">${card.label}</div>
          <div class="value">${card.value}</div>
          <div class="sub">${card.sub}</div>
        </div>
      `).join("");
    }

    function openParent(parent) {
      if (!parent) return;
      state.parentSymbol = parent;
      state.parentSearch = parent;
      parentSearchInput.value = parent;
      populateParentSelect(state.allParents, parent);
      loadDashboard().then(() => {
        document.getElementById("contracts-table").scrollIntoView({ behavior: "smooth", block: "start" });
      });
    }

    function renderContracts(rows) {
      const tbody = document.querySelector("#contracts-table tbody");
      if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="36" class="empty">No contracts match the current filters.</td></tr>`;
        return;
      }

      tbody.innerHTML = rows.map(row => `
        <tr data-parent-symbol="${row.parent_symbol || ""}" title="Click to open ${row.parent_symbol || "parent"}">
          <td>${row.raw_option_symbol}</td>
          <td>${fmtNumber(row.strike, 3)}</td>
          <td>${row.side || ""}</td>
          <td>${row.grouping || ""}</td>
          <td>${row.decay_bucket || ""}</td>
          <td>${row.days_to_expiry ?? ""}</td>
          <td>${fmtNumber(row.underlying_price, 2)}</td>
          <td>${fmtNumber(row.bid, 3)}</td>
          <td>${fmtNumber(row.ask, 3)}</td>
          <td>${fmtNumber(row.mid, 3)}</td>
          <td>${fmtNumber(row.spread_pct, 4)}</td>
          <td>${fmtInt(row.rolling_volume_10m)}</td>
          <td>${fmtInt(row.rolling_volume_30m)}</td>
          <td>${fmtInt(row.rolling_volume_1h)}</td>
          <td>${fmtNumber(row.mean_vol_3d, 2)}</td>
          <td>${fmtNumber(row.std_vol_3d, 2)}</td>
          <td class="${zClass(row.z_vol_3d)}">${fmtNumber(row.z_vol_3d, 2)}</td>
          <td>${fmtNumber(row.mean_vol_35d, 2)}</td>
          <td>${fmtNumber(row.std_vol_35d, 2)}</td>
          <td class="${zClass(row.z_vol_35d)}">${fmtNumber(row.z_vol_35d, 2)}</td>
          <td>${fmtNumber(row.mean_mid_3d, 3)}</td>
          <td>${fmtNumber(row.std_mid_3d, 3)}</td>
          <td class="${zClass(row.z_mid_3d)}">${fmtNumber(row.z_mid_3d, 2)}</td>
          <td>${fmtNumber(row.mean_mid_35d, 3)}</td>
          <td>${fmtNumber(row.std_mid_35d, 3)}</td>
          <td class="${zClass(row.z_mid_35d)}">${fmtNumber(row.z_mid_35d, 2)}</td>
          <td>${fmtNumber(row.current_iv, 3)}</td>
          <td>${fmtNumber(row.mean_iv_3d, 3)}</td>
          <td>${fmtNumber(row.std_iv_3d, 3)}</td>
          <td class="${zClass(row.z_iv_3d)}">${fmtNumber(row.z_iv_3d, 2)}</td>
          <td>${fmtNumber(row.mean_iv_35d, 3)}</td>
          <td>${fmtNumber(row.std_iv_35d, 3)}</td>
          <td class="${zClass(row.z_iv_35d)}">${fmtNumber(row.z_iv_35d, 2)}</td>
          <td>${row.last_quote_ts || ""}</td>
          <td>${row.last_trade_ts || ""}</td>
          <td>${freshnessCell(row)}</td>
        </tr>
      `).join("");

      tbody.querySelectorAll("tr[data-parent-symbol]").forEach(rowEl => {
        rowEl.addEventListener("click", () => {
          openParent(rowEl.dataset.parentSymbol);
        });
      });
    }

    function renderAlerts(rows) {
      const tbody = document.querySelector("#alerts-table tbody");
      if (!rows.length) {
        tbody.innerHTML = `<tr><td colspan="18" class="empty">No alerts logged yet.</td></tr>`;
        return;
      }

      tbody.innerHTML = rows.map(row => `
        <tr data-parent-symbol="${row.parent_symbol || ""}" title="Click to open ${row.parent_symbol || "parent"}">
          <td>${row.alert_timestamp || ""}</td>
          <td>${row.alert_type || ""}</td>
          <td>${row.parent_symbol || ""}</td>
          <td>${row.raw_option_symbol || ""}</td>
          <td>${fmtNumber(row.underlying_price, 2)}</td>
          <td>${fmtNumber(row.strike, 3)}</td>
          <td>${row.side || ""}</td>
          <td>${row.grouping || ""}</td>
          <td>${row.decay_bucket || ""}</td>
          <td>${fmtNumber(row.option_mid, 3)}</td>
          <td>${fmtInt(row.rolling_volume_10m)}</td>
          <td>${fmtNumber(row.current_iv, 3)}</td>
          <td class="${zClass(row.z_vol_35d)}">${fmtNumber(row.z_vol_35d, 2)}</td>
          <td class="${zClass(row.z_vol_3d)}">${fmtNumber(row.z_vol_3d, 2)}</td>
          <td class="${zClass(row.z_mid_35d)}">${fmtNumber(row.z_mid_35d, 2)}</td>
          <td class="${zClass(row.z_mid_3d)}">${fmtNumber(row.z_mid_3d, 2)}</td>
          <td class="${zClass(row.z_iv_35d)}">${fmtNumber(row.z_iv_35d, 2)}</td>
          <td class="${zClass(row.z_iv_3d)}">${fmtNumber(row.z_iv_3d, 2)}</td>
        </tr>
      `).join("");

      tbody.querySelectorAll("tr[data-parent-symbol]").forEach(rowEl => {
        rowEl.addEventListener("click", () => {
          openParent(rowEl.dataset.parentSymbol);
        });
      });
    }

    async function loadDashboard() {
      const response = await fetch(`/api/dashboard?${buildQuery()}`);
      const payload = await response.json();

      populateParentSelect(payload.filters.parents, payload.summary.selected_parent);
      populateSelect(sideSelect, payload.filters.sides, state.side, true);
      populateSelect(groupingSelect, payload.filters.groupings, state.grouping, true);
      populateSelect(decaySelect, payload.filters.decay_buckets, state.decayBucket, true);

      state.parentSymbol = parentSelect.value;
      state.side = sideSelect.value;
      state.grouping = groupingSelect.value;
      state.decayBucket = decaySelect.value;
      state.alertScope = alertScopeSelect.value;

      renderSummary(payload.summary);
      renderContracts(payload.contracts);
      renderAlerts(payload.alerts);
      document.getElementById("last-refresh").textContent = `Last refresh ${new Date().toLocaleTimeString()}`;
    }

    function hookControls() {
      parentSelect.addEventListener("change", () => { state.parentSymbol = parentSelect.value; loadDashboard(); });
      parentSearchInput.addEventListener("input", () => {
        state.parentSearch = parentSearchInput.value.trim().toUpperCase();
        parentSearchInput.value = state.parentSearch;
        const match = matchingParent(state.allParents, state.parentSearch);
        populateParentSelect(state.allParents, match || state.parentSymbol);
        if (match && match !== state.parentSymbol) {
          state.parentSymbol = match;
          loadDashboard();
        }
      });
      sideSelect.addEventListener("change", () => { state.side = sideSelect.value; loadDashboard(); });
      groupingSelect.addEventListener("change", () => { state.grouping = groupingSelect.value; loadDashboard(); });
      decaySelect.addEventListener("change", () => { state.decayBucket = decaySelect.value; loadDashboard(); });
      rawSearchInput.addEventListener("input", () => { state.rawSearch = rawSearchInput.value.trim(); loadDashboard(); });
      alertLimitSelect.addEventListener("change", () => { state.alertLimit = alertLimitSelect.value; loadDashboard(); });
      alertScopeSelect.addEventListener("change", () => { state.alertScope = alertScopeSelect.value; loadDashboard(); });
    }

    hookControls();
    loadDashboard();
    setInterval(loadDashboard, 2000);
  </script>
</body>
</html>
"""


def connect_duckdb(path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(path), read_only=read_only)


def to_json_value(value):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        ts = value
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return ts.isoformat()
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=dt.timezone.utc)
        return value.isoformat()
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, (float, int, str, bool)):
        return value
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def json_records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    records = []
    for row in df.to_dict(orient="records"):
        records.append({key: to_json_value(value) for key, value in row.items()})
    return records


def ensure_columns(df: pd.DataFrame, expected_columns: list[str]) -> pd.DataFrame:
    if df.empty and not len(df.columns):
        return pd.DataFrame(columns=expected_columns)

    missing = [column for column in expected_columns if column not in df.columns]
    for column in missing:
        df[column] = pd.NA
    return df


def parse_strike_from_raw_symbol(raw_symbol: str) -> float | None:
    try:
        return int(str(raw_symbol)[-8:]) / 1000.0
    except Exception:
        return None


def parse_expiration_date_from_raw_symbol(raw_symbol: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(str(raw_symbol)[-15:-9], "%y%m%d").date()
    except Exception:
        return None


def load_raw_universe(path: Path = RAW_SYMBOLS_DB_PATH) -> pd.DataFrame:
    con = connect_duckdb(path, read_only=True)
    try:
        df = con.execute("""
            SELECT parent_symbol, raw_option_symbol, side, grouping, decay_bucket, days_to_expiry
            FROM raw_symbols
            ORDER BY parent_symbol, grouping, side, raw_option_symbol
        """).fetchdf()
        if not df.empty:
            df["strike"] = df["raw_option_symbol"].map(parse_strike_from_raw_symbol)
            df["expiration_date"] = df["raw_option_symbol"].map(parse_expiration_date_from_raw_symbol)
        return ensure_columns(df, RAW_UNIVERSE_COLUMNS)
    finally:
        con.close()

def build_dashboard_payload_from_frames(
    *,
    raw_df: pd.DataFrame,
    state_df: pd.DataFrame,
    runtime_status: dict[str, Any] | None,
    alerts_df: pd.DataFrame,
    parent_symbol: str | None,
    side: str | None,
    grouping: str | None,
    decay_bucket: str | None,
    raw_search: str | None,
    alert_limit: int,
    alert_scope: str | None,
) -> dict:
    raw_df = ensure_columns(raw_df.copy(), RAW_UNIVERSE_COLUMNS)
    state_df = ensure_columns(state_df.copy(), LIVE_CONTRACT_STATE_COLUMNS)
    alerts_df = ensure_columns(alerts_df.copy(), ALERT_HISTORY_COLUMNS)
    runtime_status = runtime_status or {}

    if raw_df.empty:
        return {
            "filters": {"parents": [], "sides": [], "groupings": [], "decay_buckets": []},
            "summary": {
                "selected_parent": None,
                "parent_count_total": 0,
                "contract_rows_total": 0,
                "visible_contract_rows": 0,
                "live_rows_with_updates": 0,
                "stale_rows": 0,
                "alerts_total": 0,
                "returned_alert_rows": 0,
                "events_total": 0,
                "quote_events_total": 0,
                "volume_events_total": 0,
                "missing_baseline_count": 0,
                "invalid_baseline_count": 0,
                "last_event_ts": None,
            },
            "contracts": [],
            "alerts": [],
        }

    all_parents = sorted(raw_df["parent_symbol"].dropna().unique().tolist())
    selected_parent = parent_symbol if parent_symbol in all_parents else (all_parents[0] if all_parents else None)

    join_keys = ["parent_symbol", "raw_option_symbol", "side", "grouping", "decay_bucket"]
    merged_df = raw_df.merge(state_df, how="left", on=join_keys, suffixes=("", "_live"))

    if "alert_timestamp" in alerts_df.columns:
        alerts_df["alert_timestamp"] = pd.to_datetime(alerts_df["alert_timestamp"], errors="coerce", utc=True)

    if selected_parent:
        merged_df = merged_df[merged_df["parent_symbol"] == selected_parent]

    if side and side != "ALL":
        merged_df = merged_df[merged_df["side"] == side]
    if grouping and grouping != "ALL":
        merged_df = merged_df[merged_df["grouping"] == grouping]
    if decay_bucket and decay_bucket != "ALL":
        merged_df = merged_df[merged_df["decay_bucket"] == decay_bucket]
    if raw_search:
        merged_df = merged_df[merged_df["raw_option_symbol"].str.contains(raw_search, case=False, na=False)]

    if alert_scope == "PARENT" and selected_parent:
        alerts_df = alerts_df[alerts_df["parent_symbol"] == selected_parent]

    now_utc = pd.Timestamp.now(tz="UTC")
    if "updated_at" in merged_df.columns:
        updated = pd.to_datetime(merged_df["updated_at"], errors="coerce", utc=True)
        merged_df["seconds_since_update"] = (now_utc - updated).dt.total_seconds()
    else:
        merged_df["seconds_since_update"] = None

    merged_df["grouping_order"] = merged_df["grouping"].map(GROUPING_ORDER).fillna(99)
    merged_df["side_order"] = merged_df["side"].map(SIDE_ORDER).fillna(99)
    merged_df = merged_df.sort_values(
        by=["grouping_order", "side_order", "strike", "raw_option_symbol"],
        kind="stable",
    ).drop(columns=["grouping_order", "side_order"], errors="ignore")

    alerts_df = alerts_df.sort_values(by="alert_timestamp", ascending=False, kind="stable").head(alert_limit)

    visible_live_rows = merged_df["updated_at"].notna().sum() if "updated_at" in merged_df.columns else 0
    stale_rows = (
        (merged_df["seconds_since_update"] > STALE_AFTER_SECONDS).sum()
        if "seconds_since_update" in merged_df.columns else 0
    )

    summary = {
        "selected_parent": selected_parent,
        "parent_count_total": raw_df["parent_symbol"].nunique(),
        "contract_rows_total": len(raw_df),
        "visible_contract_rows": len(merged_df),
        "live_rows_with_updates": int(visible_live_rows),
        "stale_rows": int(stale_rows),
        "alerts_total": runtime_status.get("alerts_total", len(alerts_df)),
        "returned_alert_rows": len(alerts_df),
        "events_total": runtime_status.get("events_total", 0),
        "quote_events_total": runtime_status.get("quote_events_total", 0),
        "volume_events_total": runtime_status.get("volume_events_total", 0),
        "missing_baseline_count": runtime_status.get("missing_baseline_count", 0),
        "invalid_baseline_count": runtime_status.get("invalid_baseline_count", 0),
        "last_event_ts": to_json_value(runtime_status.get("last_event_ts")),
        "last_quote_ts": to_json_value(runtime_status.get("last_quote_ts")),
        "last_volume_ts": to_json_value(runtime_status.get("last_volume_ts")),
    }

    filters = {
        "parents": all_parents,
        "sides": sorted(raw_df["side"].dropna().unique().tolist()),
        "groupings": sorted(raw_df["grouping"].dropna().unique().tolist(), key=lambda item: GROUPING_ORDER.get(item, 99)),
        "decay_buckets": sorted(raw_df["decay_bucket"].dropna().unique().tolist()),
    }

    return {
        "filters": filters,
        "summary": summary,
        "contracts": json_records(merged_df),
        "alerts": json_records(alerts_df),
    }


def make_dashboard_handler(
    payload_builder: Callable[..., dict],
) -> type[BaseHTTPRequestHandler]:
    class DashboardHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body: str) -> None:
            encoded = body.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._send_html(HTML_PAGE)
                return

            if parsed.path == "/api/dashboard":
                params = parse_qs(parsed.query)
                try:
                    alert_limit = int(params.get("alert_limit", ["100"])[0])
                except Exception:
                    alert_limit = 100

                payload = payload_builder(
                    parent_symbol=params.get("parent_symbol", [None])[0],
                    side=params.get("side", [None])[0],
                    grouping=params.get("grouping", [None])[0],
                    decay_bucket=params.get("decay_bucket", [None])[0],
                    raw_search=params.get("raw_search", [None])[0],
                    alert_limit=alert_limit,
                    alert_scope=params.get("alert_scope", ["ALL"])[0],
                )
                self._send_json(payload)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def log_message(self, format: str, *args) -> None:
            return

    return DashboardHandler


def start_dashboard_server(
    payload_builder: Callable[..., dict],
    *,
    host: str = HOST,
    port: int = PORT,
) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_dashboard_handler(payload_builder))
    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True,
        name="live-dashboard-server",
    )
    thread.start()
    print(f"[DASHBOARD] serving http://{host}:{port}")
    return server


def main() -> None:
    raise SystemExit("Run live_alert_consumer.py to use the in-process dashboard.")


if __name__ == "__main__":
    main()
