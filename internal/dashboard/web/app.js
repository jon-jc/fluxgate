import {
  APIClient,
  SSEParser,
  colors,
  chartSegments,
  csv,
  decimate,
  endpoint,
  format,
  labelOf,
  normalizeResult,
  queryParams,
} from "./core.js";
import { DemoClient } from "./demo.js";

const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];
const escape = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const time = (value) =>
  new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: "UTC",
  }).format(new Date(value));
const shortTime = (value) =>
  new Intl.DateTimeFormat("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
  }).format(new Date(value));
const state = {
  mode: "demo",
  client: new DemoClient(),
  metrics: [],
  result: null,
  settings: {
    metric: "http.request.duration_ms",
    aggregation: "p95",
    range: "1h",
    filters: {},
  },
  visible: new Set(),
  view: "overview",
  page: 0,
  table: "series",
  live: false,
  events: [],
  saved: [],
  session: 0,
  chart: null,
};
let queryController,
  catalogController,
  streamController,
  queryVersion = 0,
  liveTimer,
  refreshTimer,
  cardController,
  cardVersion = 0,
  lastRefresh = 0,
  pendingSharedView = null;
try {
  state.saved = JSON.parse(localStorage.getItem("fluxgate.views.v1") || "[]")
    .filter(
      (view) =>
        view &&
        typeof view.name === "string" &&
        view.name.length <= 80 &&
        view.settings &&
        ["demo", "api"].includes(view.mode),
    )
    .slice(0, 30);
} catch {
  state.saved = [];
}
try {
  document.documentElement.dataset.theme =
    localStorage.getItem("fluxgate.dashboard.theme") || "dark";
} catch {
  /* Storage is optional. */
}

function toast(message) {
  $("#toast").textContent = message;
  $("#toast").classList.add("visible");
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => $("#toast").classList.remove("visible"), 3200);
}
function notice(target, text, error = false) {
  target.textContent = text;
  target.hidden = !text;
  target.classList.toggle("error", error);
}
function openDialog(id) {
  const dialog = $(id);
  if (!dialog.open) dialog.showModal();
}
$$(".close-dialog").forEach((button) =>
  button.addEventListener("click", () => button.closest("dialog").close()),
);
$$("dialog").forEach((dialog) =>
  dialog.addEventListener("click", (event) => {
    if (event.target === dialog) dialog.close();
  }),
);
$("#connection-dialog").addEventListener("close", () => {
  $("#api-key").value = "";
});

function setView(view) {
  state.view = view;
  const titles = {
    overview: [
      "A pulse on your system.",
      "Explore every signal. Follow every change.",
    ],
    explore: [
      "Follow the signal.",
      "Query committed rollups. Inspect every series.",
    ],
    activity: [
      "Watch it happen.",
      "Follow new and corrected windows as they are committed.",
    ],
    saved: [
      "Your next investigation.",
      "Return to the queries that matter to you.",
    ],
  };
  $("#page-title").textContent = titles[view][0];
  $("#page-description").textContent = titles[view][1];
  $("#breadcrumb").textContent = {
    overview: "Overview",
    explore: "Metric explorer",
    activity: "Live activity",
    saved: "Saved views",
  }[view];
  $$("[data-view]").forEach((button) => {
    if (button.dataset.view === view)
      button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  $("#overview-section").hidden = view !== "overview";
  $("#catalog-section").hidden = view !== "overview";
  $("#explorer-section").hidden = !["overview", "explore"].includes(view);
  $("#activity-section").hidden = view !== "activity";
  $("#saved-section").hidden = view !== "saved";
  if (view === "activity" && !state.live) startLive();
  renderSaved();
  renderActivity();
  if (!$("#explorer-section").hidden) renderChart();
}
$$("[data-view]").forEach((button) =>
  button.addEventListener("click", () => setView(button.dataset.view)),
);
$("#theme").addEventListener("click", () => {
  const theme =
    document.documentElement.dataset.theme === "dark" ? "light" : "dark";
  document.documentElement.dataset.theme = theme;
  try {
    localStorage.setItem("fluxgate.dashboard.theme", theme);
  } catch {}
  renderChart();
});

function updateConnection() {
  const demo = state.mode === "demo",
    connected = state.mode === "api";
  $("#demo-banner").hidden = !demo;
  $("#workspace-name").textContent = demo
    ? "Demo workspace"
    : connected
      ? "Connected workspace"
      : "Disconnected";
  $("#workspace-description").textContent = demo
    ? "Synthetic telemetry"
    : connected
      ? new URL(state.client.origin).hostname
      : "Reconnect to read data";
  $("#connection-summary").textContent = demo
    ? "Demo · no server connected"
    : connected
      ? "API connected · tenant scoped"
      : "No connection";
  $("#footer-mode").textContent = demo
    ? "Synthetic demo data"
    : connected
      ? "Your tenant’s API data"
      : "Disconnected";
  $("#connect").textContent = connected ? "Manage connection" : "⊕ Connect API";
  $("#saved-count").textContent = state.saved.length;
}
function showConnection() {
  $("#api-url").value =
    state.mode === "api"
      ? state.client.origin
      : location.hostname.endsWith("vercel.app")
        ? ""
        : location.origin;
  $("#connection-error").textContent = "";
  openDialog("#connection-dialog");
}
["#connect", "#demo-connect", "#workspace-switch"].forEach((id) =>
  $(id).addEventListener("click", showConnection),
);

function resetSession(client, mode) {
  stopLive();
  queryController?.abort();
  catalogController?.abort();
  cardController?.abort();
  if (state.client instanceof APIClient) state.client.token = "";
  state.session++;
  queryVersion++;
  cardVersion++;
  document.body.classList.remove("loading");
  $("#query-form button[type=submit]").disabled = false;
  state.client = client;
  state.mode = mode;
  state.result = null;
  state.metrics = [];
  state.events = [];
  state.visible.clear();
  state.settings.filters = {};
  state.settings.metric = "";
  notice($("#global-notice"), "");
  notice($("#query-notice"), "");
  updateConnection();
  renderChart();
  renderActivity();
  renderCards([]);
  renderCatalog();
}
$("#connection-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("#connection-submit");
  button.disabled = true;
  button.textContent = "Connecting…";
  $("#connection-error").textContent = "";
  let candidate;
  try {
    const origin = endpoint($("#api-url").value.trim(), location.origin),
      token = $("#api-key").value.trim();
    if (!/^fxg_[^\s]+$/.test(token))
      throw new Error("Enter a Fluxgate API key beginning with fxg_.");
    candidate = new APIClient(origin, token);
    const catalog = await candidate.metrics();
    validateCatalog(catalog);
    if (!$("#connection-dialog").open) {
      candidate.token = "";
      return;
    }
    resetSession(candidate, "api");
    $("#connection-dialog").close();
    if (pendingSharedView) {
      state.settings = pendingSharedView;
      pendingSharedView = null;
    }
    applyCatalog(catalog);
    setView("overview");
    await runQuery();
    loadCards();
    toast("Connected. Queries are scoped to your API key’s tenant.");
  } catch (error) {
    if (candidate) candidate.token = "";
    $("#connection-error").textContent =
      error.name === "TypeError"
        ? "Connection failed. Check the URL, TLS, and QUERY_ALLOWED_ORIGINS on your query service."
        : error.message;
  } finally {
    button.disabled = false;
    button.textContent = "Connect workspace →";
    $("#api-key").value = "";
  }
});
$("#use-demo").addEventListener("click", () => {
  $("#connection-dialog").close();
  resetSession(new DemoClient(), "demo");
  loadCatalog();
});

function validateCatalog(data) {
  if (
    !data ||
    !Array.isArray(data.metrics) ||
    data.metrics.length > 1000 ||
    data.metrics.some(
      (item) =>
        typeof item.metric !== "string" ||
        item.metric.length > 200 ||
        !["gauge", "counter", "histogram"].includes(item.kind),
    )
  )
    throw new Error("The query API returned an invalid metric catalog.");
}
function defaultAggregation(metric) {
  return (
    metric?.aggregation ||
    (metric?.kind === "counter"
      ? "sum"
      : metric?.kind === "histogram"
        ? "p95"
        : "last")
  );
}
function applyCatalog(data) {
  validateCatalog(data);
  state.metrics = data.metrics;
  const names = [...new Set(data.metrics.map((metric) => metric.metric))];
  $("#metric").replaceChildren(...names.map((name) => new Option(name, name)));
  if (!names.includes(state.settings.metric)) {
    if (state.settings.metric && (pendingSharedView || state.mode === "api")) {
      $("#metric").add(
        new Option(state.settings.metric, state.settings.metric),
      );
    } else {
      state.settings.metric = names[0] || "";
      state.settings.aggregation = defaultAggregation(data.metrics[0]);
    }
  }
  syncControls();
  renderCatalog();
  if (!names.length)
    notice(
      $("#global-notice"),
      "No retained metrics were found for this tenant. Ingest data, wait for an aggregation checkpoint, then refresh.",
    );
  else if (names.length >= 1000)
    notice(
      $("#global-notice"),
      "Catalog reached the 1,000-entry discovery limit. This is a bounded list, not a complete tenant inventory.",
    );
}
async function loadCatalog() {
  if (!state.client) return showConnection();
  const client = state.client;
  catalogController?.abort();
  catalogController = new AbortController();
  try {
    const data = await client.metrics(catalogController.signal);
    if (client !== state.client) return;
    applyCatalog(data);
    await runQuery();
    loadCards();
  } catch (error) {
    if (error.name !== "AbortError" && client === state.client) {
      rejectKey(error);
      notice(
        $("#global-notice"),
        `Catalog unavailable. ${error.message}`,
        true,
      );
    }
  }
}
function syncControls() {
  $("#metric").value = state.settings.metric;
  $("#aggregation").value = state.settings.aggregation;
  $$("[data-range]").forEach((button) =>
    button.setAttribute(
      "aria-pressed",
      String(button.dataset.range === state.settings.range),
    ),
  );
  $("#custom-range").hidden = state.settings.range !== "custom";
  const localInput = (value) => {
    const date = new Date(value);
    return new Date(+date - date.getTimezoneOffset() * 60000)
      .toISOString()
      .slice(0, 16);
  };
  if (state.settings.from) $("#from").value = localInput(state.settings.from);
  if (state.settings.to) $("#to").value = localInput(state.settings.to);
  $("#filter-chips").innerHTML = Object.entries(state.settings.filters)
    .map(
      ([key, value]) =>
        `<span class="filter-chip">${escape(key)} = ${escape(value)}<button data-remove-filter="${escape(key)}" aria-label="Remove ${escape(key)} filter">×</button></span>`,
    )
    .join("");
}
function settingsFromControls() {
  return {
    ...state.settings,
    metric: $("#metric").value,
    aggregation: $("#aggregation").value,
    from: $("#from").value ? new Date($("#from").value).toISOString() : "",
    to: $("#to").value ? new Date($("#to").value).toISOString() : "",
  };
}
function rejectKey(error) {
  if (error.status !== 401) return false;
  resetSession(null, "disconnected");
  notice(
    $("#global-notice"),
    "Your key was rejected. Reconnect to resume reading data.",
    true,
  );
  return true;
}
async function runQuery() {
  if (!state.client || !state.settings.metric) {
    renderChart();
    return;
  }
  let settings;
  try {
    settings = structuredClone(state.settings);
    queryParams(settings);
  } catch (error) {
    notice($("#query-notice"), error.message, true);
    return;
  }
  queryController?.abort();
  queryController = new AbortController();
  const controller = queryController,
    version = ++queryVersion,
    client = state.client;
  document.body.classList.add("loading");
  $("#query-form button[type=submit]").disabled = true;
  $("#refresh-status").textContent = "Querying…";
  const started = performance.now();
  try {
    const raw = await client.query(settings, controller.signal);
    if (version !== queryVersion || client !== state.client) return;
    const result = normalizeResult(raw);
    const sameMetric = state.result?.metric === result.metric;
    state.result = result;
    state.page = 0;
    if (!sameMetric)
      state.visible = new Set(
        result.series.slice(0, 6).map((series) => series.key),
      );
    else {
      const hadSelection = state.visible.size > 0;
      state.visible = new Set(
        [...state.visible].filter((key) =>
          result.series.some((series) => series.key === key),
        ),
      );
      if (hadSelection && !state.visible.size)
        state.visible = new Set(
          result.series.slice(0, 6).map((series) => series.key),
        );
    }
    const warnings = [
      result.truncated
        ? "Partial result: the API truncated this query. Narrow the time range or add labels before using totals."
        : "",
      ...(Array.isArray(raw.warnings) ? raw.warnings.map(String) : []),
    ].filter(Boolean);
    notice($("#query-notice"), warnings.join(" "));
    $("#query-duration").textContent =
      `${Math.round(performance.now() - started)} ms · ${format(result.pointCount, 0)} points`;
    $("#refresh-status").textContent = `Updated ${shortTime(Date.now())} UTC`;
    lastRefresh = Date.now();
    renderChart();
    renderTable();
  } catch (error) {
    if (
      version !== queryVersion ||
      client !== state.client ||
      error.name === "AbortError"
    )
      return;
    rejectKey(error);
    const message =
      error.name === "TimeoutError"
        ? "Query timed out. Narrow the range and retry."
        : error.name === "TypeError"
          ? "Unable to reach the query API. Check your connection and browser-origin configuration."
          : error.message;
    notice(
      $("#query-notice"),
      `${message}${state.result ? ` Showing the previous result for ${state.result.metric}; it is stale.` : ""}${error.requestId ? ` Request ID: ${error.requestId}` : ""}`,
      true,
    );
    $("#refresh-status").textContent = "Refresh failed";
    if (!state.result) renderChart();
    if (error.status === 429 || error.status === 503) {
      stopLive();
      toast(
        `Automatic refresh paused. ${error.retryAfter ? `Retry-After: ${error.retryAfter} seconds.` : "Retry after the service recovers."}`,
      );
    }
  } finally {
    if (version === queryVersion) {
      document.body.classList.remove("loading");
      $("#query-form button[type=submit]").disabled = false;
    }
  }
}
$("#query-form").addEventListener("submit", (event) => {
  event.preventDefault();
  state.settings = settingsFromControls();
  runQuery();
  restartLive();
});
$("#metric").addEventListener("change", () => {
  state.settings.metric = $("#metric").value;
  state.settings.aggregation = defaultAggregation(
    state.metrics.find((metric) => metric.metric === state.settings.metric),
  );
  state.settings.filters = {};
  syncControls();
  runQuery();
  restartLive();
});
$("#aggregation").addEventListener("change", () => {
  state.settings.aggregation = $("#aggregation").value;
  runQuery();
});
$$("[data-range]").forEach((button) =>
  button.addEventListener("click", () => {
    state.settings.range = button.dataset.range;
    if (state.settings.range === "custom") {
      stopLive();
      state.settings.from ||= new Date(Date.now() - 3600000).toISOString();
      state.settings.to ||= new Date().toISOString();
    }
    syncControls();
    if (state.settings.range !== "custom") runQuery();
  }),
);
$("#refresh").addEventListener("click", () => loadCatalog());

function renderCatalog() {
  $("#catalog-count").textContent = state.metrics.length;
  $("#catalog").innerHTML = state.metrics
    .slice(0, 12)
    .map(
      (metric, i) =>
        `<button class="catalog-card" data-metric-index="${i}"><span class="kind-badge">${escape(metric.kind)}</span><h3>${escape(metric.metric)}</h3><p><span>${format(metric.series_count, 0)} retained series</span><span aria-hidden="true">↗</span></p></button>`,
    )
    .join("");
  $("#catalog-note").textContent =
    state.metrics.length > 12
      ? `Showing 12 of ${state.metrics.length} discovered metric/kind entries. Use Find a metric to explore the rest. Discovery is capped at 1,000 entries.`
      : "Discovery reflects retained data, not process health. Empty windows are not filled with zeroes.";
  $("#catalog")
    .querySelectorAll("[data-metric-index]")
    .forEach((button) =>
      button.addEventListener("click", () =>
        selectMetric(state.metrics[Number(button.dataset.metricIndex)]),
      ),
    );
}
function selectMetric(metric) {
  state.settings.metric = metric.metric;
  state.settings.aggregation = defaultAggregation(metric);
  state.settings.filters = {};
  syncControls();
  setView("explore");
  runQuery();
  restartLive();
}
async function loadCards() {
  const client = state.client,
    version = ++cardVersion;
  if (!client) return;
  cardController?.abort();
  cardController = new AbortController();
  const signal = cardController.signal;
  const cards = [];
  // Sequential reads leave the query service's bounded request slots available
  // to the main chart and other users; cards never fan out unbounded requests.
  for (const metric of state.metrics.slice(0, 4)) {
    try {
      const data = normalizeResult(
        await client.query(
          {
            metric: metric.metric,
            aggregation: defaultAggregation(metric),
            range: "1h",
            filters: {},
          },
          signal,
        ),
      );
      cards.push({ metric, data });
    } catch (error) {
      if (signal.aborted || client !== state.client || rejectKey(error)) return;
      cards.push({
        metric,
        error: error.status === 401 ? "Authentication required" : "Unavailable",
      });
    }
    if (version !== cardVersion || client !== state.client) return;
    renderCards(cards);
  }
}
function renderCards(cards) {
  $("#stat-grid").innerHTML = cards
    .map(({ metric, data, error }, i) => {
      const points = data?.series[0]?.points || [],
        value = points.at(-1)?.v;
      const values = points.map((point) => point.v),
        min = Math.min(...values),
        max = Math.max(...values);
      const coordinates = points.length
        ? decimate(points, 60)
            .map(
              (point, index, list) =>
                `${(index / Math.max(1, list.length - 1)) * 140},${39 - ((point.v - min) / (max - min || 1)) * 32}`,
            )
            .join(" ")
        : "";
      return `<button class="stat-card${state.settings.metric === metric.metric ? " selected" : ""}" data-card="${i}"><span class="stat-top"><span>${escape(metric.title || metric.metric)}</span><span class="stat-icon" aria-hidden="true">${["⌁", "◷", "▤", "↯"][i]}</span></span><strong class="stat-number">${format(value, metric.metric.includes("ratio") ? 3 : 1)}${metric.metric.endsWith("_ms") && Number.isFinite(value) ? "<small> ms</small>" : ""}</strong><span class="stat-meta">${escape(error || `${defaultAggregation(metric)} · latest window`)}</span><span class="stat-kind">${data?.truncated ? "PARTIAL RESULT · " : ""}FIRST SERIES · LAST HOUR</span><svg class="stat-spark" viewBox="0 0 140 45" aria-hidden="true"><polyline points="${coordinates}" fill="none" stroke="${colors[i]}" stroke-width="1.7"/></svg></button>`;
    })
    .join("");
  $("#stat-grid")
    .querySelectorAll("[data-card]")
    .forEach((button) =>
      button.addEventListener("click", () =>
        selectMetric(cards[Number(button.dataset.card)].metric),
      ),
    );
}

function renderChart() {
  const result = state.result,
    chart = $("#chart");
  $("#chart-tooltip").hidden = true;
  if (!result) {
    chart.replaceChildren();
    $("#legend").replaceChildren();
    $("#chart-empty").hidden = false;
    $("#chart-empty").innerHTML =
      "<strong>No query result yet</strong><p>Connect a query API or choose a metric from the demo workspace.</p>";
    $("#selected-value").textContent = "—";
    state.chart = null;
    renderTable();
    return;
  }
  const visible = result.series.filter((series) =>
    state.visible.has(series.key),
  );
  const all = visible.flatMap((series) => series.points);
  const width = Math.max(320, chart.clientWidth),
    height = width < 500 ? 235 : 290;
  const left = width < 500 ? 47 : 62,
    right = width - 18,
    top = 15,
    bottom = height - 36;
  const from = Date.parse(result.from),
    to = Date.parse(result.to);
  let min = 0,
    max = 1;
  if (all.length) {
    min = all.reduce((value, point) => Math.min(value, point.v), Infinity);
    max = all.reduce((value, point) => Math.max(value, point.v), -Infinity);
    const padding = (max - min || Math.abs(max) || 1) * 0.13;
    min = min >= 0 ? Math.max(0, min - padding) : min - padding;
    max += padding;
  }
  const x = (t) => left + ((t - from) / (to - from || 1)) * (right - left),
    y = (v) => bottom - ((v - min) / (max - min || 1)) * (bottom - top);
  chart.setAttribute("viewBox", `0 0 ${width} ${height}`);
  chart.setAttribute(
    "aria-label",
    `${result.metric}, ${result.aggregation}, ${visible.length} visible series. Exact observations are available in the data table.`,
  );
  const grid = Array.from({ length: 5 }, (_, i) => {
    const value = min + ((max - min) * i) / 4,
      py = y(value);
    return `<line class="grid-line" x1="${left}" x2="${right}" y1="${py}" y2="${py}"/><text x="${left - 10}" y="${py + 3}" text-anchor="end">${escape(format(value))}</text>`;
  }).join("");
  const tickCount = width < 500 ? 4 : 7;
  const ticks = Array.from({ length: tickCount }, (_, i) => {
    const t = from + ((to - from) * i) / (tickCount - 1);
    return `<text x="${x(t)}" y="${height - 11}" text-anchor="${i === 0 ? "start" : i === tickCount - 1 ? "end" : "middle"}">${shortTime(t)}</text>`;
  }).join("");
  const paths = visible
    .map((series) => {
      const index = result.series.indexOf(series),
        segments = chartSegments(series.points);
      return segments
        .map((points) =>
          points.length === 1
            ? `<circle cx="${x(points[0].t)}" cy="${y(points[0].v)}" r="3" fill="${colors[index % colors.length]}"/>`
            : `<path class="series-path" stroke="${colors[index % colors.length]}" d="${points.map((point, i) => `${i ? "L" : "M"}${x(point.t).toFixed(2)},${y(point.v).toFixed(2)}`).join(" ")}"/>`,
        )
        .join("");
    })
    .join("");
  chart.innerHTML = `${grid}${ticks}${paths}<line id="crosshair" x1="0" x2="0" y1="${top}" y2="${bottom}" stroke="var(--muted)" stroke-dasharray="3 4" visibility="hidden"/>`;
  $("#chart-empty").hidden = all.length > 0;
  if (!all.length)
    $("#chart-empty").innerHTML = result.series.length
      ? "<strong>No series selected</strong><p>Enable a series in the legend or table to draw it.</p>"
      : "<strong>No observations in this range</strong><p>Try a wider time range, remove label filters, or use an aggregation supported by this metric kind.</p>";
  $("#chart-title").textContent = result.metric;
  $("#metric-kind").textContent = result.kind || "mixed";
  $("#chart-description").textContent =
    `${result.aggregation} · window values · UTC`;
  $("#selected-value").textContent = format(
    visible[0]?.points.at(-1)?.v,
    result.metric.includes("ratio") ? 3 : 1,
  );
  $("#series-summary").textContent =
    `${visible.length} charted / ${result.series.length} returned`;
  $("#chart-footnote").textContent =
    `Up to 12 series charted. ${result.pointCount > 600 ? "Chart simplified; exports retain all returned points. " : ""}Window totals may change.`;
  $("#legend").innerHTML = result.series
    .slice(0, 12)
    .map(
      (series, index) =>
        `<button data-series="${index}" aria-pressed="${state.visible.has(series.key)}"><span class="swatch" style="background:${colors[index % colors.length]}"></span>${escape(labelOf(series))}${result.kind === "mixed" ? ` · ${escape(series.kind)}` : ""}</button>`,
    )
    .join("");
  $("#legend")
    .querySelectorAll("[data-series]")
    .forEach((button) =>
      button.addEventListener("click", () =>
        toggleSeries(Number(button.dataset.series)),
      ),
    );
  state.chart = { x, y, left, right, from, to, width, height, visible };
}
function toggleSeries(index) {
  const series = state.result?.series[index];
  if (!series) return;
  if (state.visible.has(series.key)) state.visible.delete(series.key);
  else {
    if (state.visible.size >= 12)
      return toast("Up to 12 series can be charted at once. Hide one first.");
    state.visible.add(series.key);
  }
  renderChart();
  renderTable();
}
$("#chart").addEventListener("pointermove", (event) => {
  const data = state.chart;
  if (!data || !data.visible.length) return;
  const rect = $("#chart").getBoundingClientRect(),
    px = ((event.clientX - rect.left) / rect.width) * data.width;
  if (px < data.left || px > data.right) {
    $("#chart-tooltip").hidden = true;
    return;
  }
  const target =
    data.from +
    ((px - data.left) / (data.right - data.left)) * (data.to - data.from);
  const nearest = (points) => {
    let lo = 0,
      hi = points.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (points[mid].t < target) lo = mid + 1;
      else hi = mid;
    }
    const before = points[lo - 1],
      after = points[lo];
    return before && (!after || target - before.t < after.t - target)
      ? before
      : after;
  };
  const point = nearest(data.visible[0].points);
  if (!point) return;
  const crosshair = $("#crosshair");
  crosshair.setAttribute("x1", data.x(point.t));
  crosshair.setAttribute("x2", data.x(point.t));
  crosshair.setAttribute("visibility", "visible");
  $("#chart-tooltip").innerHTML =
    `<strong>${escape(new Date(point.t).toISOString())}</strong>${data.visible
      .slice(0, 6)
      .map((series) => {
        const value = series.points.find((item) => item.t === point.t);
        return `<div class="tooltip-row"><span>${escape(series.labels.region || labelOf(series).slice(0, 30))}</span><b>${value ? format(value.v, 3) : "No window"}</b></div>`;
      })
      .join("")}`;
  $("#chart-tooltip").hidden = false;
  const wrap = $("#chart-wrap").getBoundingClientRect();
  $("#chart-tooltip").style.left =
    `${Math.max(8, Math.min(event.clientX - wrap.left + 15, wrap.width - $("#chart-tooltip").offsetWidth - 8))}px`;
  $("#chart-tooltip").style.top = "16px";
});
$("#chart").addEventListener("pointerleave", () => {
  $("#chart-tooltip").hidden = true;
  $("#crosshair")?.setAttribute("visibility", "hidden");
});
new ResizeObserver(() => {
  if (!$("#explorer-section").hidden) renderChart();
}).observe($("#chart-wrap"));

function renderTable() {
  const result = state.result,
    filter = $("#series-search").value.toLowerCase();
  const series = (result?.series || [])
    .map((item, index) => ({ ...item, index }))
    .filter((item) =>
      `${item.kind} ${labelOf(item)}`.toLowerCase().includes(filter),
    );
  const rows =
    state.table === "series"
      ? series
      : series
          .flatMap((item) =>
            item.points.map((point) => ({ series: item, point })),
          )
          .sort((a, b) => b.point.t - a.point.t);
  state.page = Math.max(
    0,
    Math.min(state.page, Math.ceil(rows.length / 25) - 1),
  );
  const page = rows.slice(state.page * 25, (state.page + 1) * 25);
  $("#series-count").textContent = result?.series.length || 0;
  $("#data-head").innerHTML =
    state.table === "series"
      ? '<tr><th scope="col">Chart / labels</th><th scope="col">Kind</th><th scope="col">Latest value</th><th scope="col">Min</th><th scope="col">Max</th><th scope="col">Windows</th></tr>'
      : '<tr><th scope="col">Window start (UTC)</th><th scope="col">Labels</th><th scope="col">Kind</th><th scope="col">Value</th></tr>';
  const labels = (item) =>
    Object.entries(item.labels)
      .map(
        ([k, v]) =>
          `<span class="label-tag" title="${escape(`${k}=${v}`)}">${escape(k)}=${escape(v)}</span>`,
      )
      .join("") || "No labels";
  $("#data-body").innerHTML = page.length
    ? page
        .map((item) =>
          state.table === "series"
            ? `<tr><td><input class="table-checkbox" type="checkbox" data-series="${item.index}" aria-label="Chart ${escape(labelOf(item))} ${escape(item.kind)}"${state.visible.has(item.key) ? " checked" : ""}>${labels(item)}</td><td>${escape(item.kind)}</td><td class="mono">${format(item.points.at(-1)?.v, 3)}</td><td class="mono">${format(
                item.points.reduce((v, p) => Math.min(v, p.v), Infinity),
                3,
              )}</td><td class="mono">${format(
                item.points.reduce((v, p) => Math.max(v, p.v), -Infinity),
                3,
              )}</td><td class="mono">${item.points.length}</td></tr>`
            : `<tr><td class="mono">${escape(new Date(item.point.t).toISOString())}</td><td>${labels(item.series)}</td><td>${escape(item.series.kind)}</td><td class="mono">${format(item.point.v, 6)}</td></tr>`,
        )
        .join("")
    : '<tr><td colspan="6">No matching data. Adjust the query or table filter.</td></tr>';
  $("#data-body")
    .querySelectorAll("[data-series]")
    .forEach((input) =>
      input.addEventListener("change", () =>
        toggleSeries(Number(input.dataset.series)),
      ),
    );
  $("#table-summary").textContent =
    `${rows.length ? state.page * 25 + 1 : 0}–${Math.min((state.page + 1) * 25, rows.length)} of ${format(rows.length, 0)} ${state.table === "series" ? "series" : "observations"}${result?.truncated ? " · partial API result" : ""}`;
  $("#previous").disabled = state.page === 0;
  $("#next").disabled = (state.page + 1) * 25 >= rows.length;
  $("#export").disabled = !result?.pointCount;
}
$("#series-search").addEventListener("input", () => {
  state.page = 0;
  renderTable();
});
$("#previous").addEventListener("click", () => {
  state.page--;
  renderTable();
});
$("#next").addEventListener("click", () => {
  state.page++;
  renderTable();
});
["series", "points"].forEach((type) =>
  $(`#show-${type}`).addEventListener("click", () => {
    state.table = type;
    state.page = 0;
    $("#show-series").setAttribute("aria-pressed", String(type === "series"));
    $("#show-points").setAttribute("aria-pressed", String(type === "points"));
    renderTable();
  }),
);
$("#export").addEventListener("click", () => {
  if (!state.result) return;
  const url = URL.createObjectURL(
    new Blob([csv(state.result)], { type: "text/csv;charset=utf-8" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = `fluxgate-${state.mode}-${state.result.metric.replace(/[^\w.-]/g, "_")}${state.result.truncated ? "-partial" : ""}.csv`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
  toast(
    "Exported all returned points. Chart selection does not limit the export.",
  );
});

$("#add-filter").addEventListener("click", async () => {
  if (!state.client || !state.settings.metric) return;
  $("#filter-key").value = "";
  $("#filter-value").value = "";
  $("#filter-error").textContent = "";
  openDialog("#filter-dialog");
  const client = state.client,
    metric = state.settings.metric;
  try {
    const data = await client.labels(metric);
    if (client !== state.client || metric !== state.settings.metric) return;
    $("#label-keys").replaceChildren(
      ...(data.labels || []).slice(0, 500).map((label) => new Option(label)),
    );
  } catch (error) {
    if (client !== state.client) return;
    if (rejectKey(error)) $("#filter-dialog").close();
    else
      $("#filter-error").textContent =
        "Label suggestions are unavailable. You can enter a key and value manually.";
  }
});
$("#filter-key").addEventListener("change", async () => {
  if (!state.client) return;
  const label = $("#filter-key").value,
    client = state.client,
    metric = state.settings.metric;
  try {
    const data = await client.labels(metric, label);
    if (client !== state.client || label !== $("#filter-key").value) return;
    $("#label-values").replaceChildren(
      ...(data.values || []).slice(0, 1000).map((value) => new Option(value)),
    );
  } catch (error) {
    if (client === state.client && rejectKey(error))
      $("#filter-dialog").close();
  }
});
$("#filter-form").addEventListener("submit", (event) => {
  event.preventDefault();
  const filters = {
    ...state.settings.filters,
    [$("#filter-key").value]: $("#filter-value").value,
  };
  try {
    if (Object.keys(filters).length > 20)
      throw new Error("Use at most 20 label filters.");
    queryParams({ ...state.settings, filters });
    state.settings.filters = filters;
    syncControls();
    $("#filter-dialog").close();
    runQuery();
  } catch (error) {
    $("#filter-error").textContent = error.message;
  }
});
$("#filter-chips").addEventListener("click", (event) => {
  const button = event.target.closest("[data-remove-filter]");
  if (!button) return;
  delete state.settings.filters[button.dataset.removeFilter];
  syncControls();
  runQuery();
});

function liveStatus(message, active = false) {
  $("#stream-status").textContent = message;
  $("#live-dot").style.background = active ? "var(--green)" : "var(--quiet)";
  $("#live-toggle").setAttribute("aria-checked", String(state.live));
  $("#activity-status").textContent = `${state.settings.metric} · ${message}`;
  $("#activity-toggle").textContent = state.live ? "Pause live" : "Resume live";
}
function stopLive() {
  state.live = false;
  clearTimeout(liveTimer);
  clearTimeout(refreshTimer);
  refreshTimer = null;
  streamController?.abort();
  streamController = null;
  liveStatus("Live paused");
}
function restartLive() {
  const running = state.live;
  stopLive();
  if (running) startLive();
}
function scheduleReconcile() {
  if (refreshTimer || !state.live) return;
  refreshTimer = setTimeout(
    () => {
      refreshTimer = null;
      if (state.live) {
        if (document.body.classList.contains("loading")) scheduleReconcile();
        else runQuery();
      }
    },
    Math.max(1000, 5000 - (Date.now() - lastRefresh)),
  );
}
function addEvent(event) {
  if (
    !event ||
    typeof event.metric !== "string" ||
    !["counter", "gauge", "histogram"].includes(event.kind) ||
    !event.labels ||
    typeof event.labels !== "object" ||
    Array.isArray(event.labels) ||
    !Number.isFinite(Date.parse(event.window_start))
  )
    return;
  if (
    event.metric !== state.settings.metric ||
    !Object.entries(state.settings.filters).every(
      ([k, v]) => event.labels[k] === v,
    )
  )
    return;
  state.events.unshift({ ...event, received: Date.now() });
  state.events.length = Math.min(state.events.length, 50);
  renderActivity();
  scheduleReconcile();
}
function startLive() {
  if (!state.client) return showConnection();
  if (state.settings.range === "custom") {
    toast("Live mode uses a rolling time range. Choose 15m, 1h, 6h, or 24h.");
    return;
  }
  if (document.hidden) return;
  state.live = true;
  liveStatus(
    state.mode === "demo" ? "Demo live" : "Connecting…",
    state.mode === "demo",
  );
  if (state.mode === "demo") {
    const session = state.session;
    const tick = async () => {
      if (!state.live || state.mode !== "demo") return;
      const data = await state.client.query(state.settings);
      if (!state.live || state.mode !== "demo" || state.session !== session)
        return;
      const series = data.series[0],
        point = series?.points.at(-1);
      if (point)
        addEvent({
          metric: data.metric,
          kind: series.kind,
          labels: series.labels,
          window_start: point.t,
          count: 240,
          sum: point.v * 240,
          min: point.v * 0.5,
          max: point.v * 1.5,
          last: point.v,
        });
      if (state.live) liveTimer = setTimeout(tick, 5000);
    };
    liveTimer = setTimeout(tick, 1500);
    return;
  }
  streamLoop();
}
async function streamLoop(attempt = 0) {
  if (!state.live || state.mode !== "api") return;
  const client = state.client,
    session = state.session,
    controller = new AbortController();
  streamController = controller;
  let retry = 4000,
    idleTimer;
  const heartbeat = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => controller.abort("idle stream"), 45000);
  };
  heartbeat();
  try {
    const response = await fetch(
      `${client.origin}/v1/stream?${new URLSearchParams({ metric: state.settings.metric })}`,
      {
        headers: {
          Authorization: `Bearer ${client.token}`,
          Accept: "text/event-stream",
        },
        credentials: "omit",
        redirect: "error",
        cache: "no-store",
        referrerPolicy: "no-referrer",
        signal: controller.signal,
      },
    );
    if (
      !state.live ||
      client !== state.client ||
      streamController !== controller
    ) {
      await response.body?.cancel();
      return;
    }
    if (!response.ok) {
      await response.body?.cancel();
      if (response.status === 401) {
        resetSession(null, "disconnected");
        notice(
          $("#global-notice"),
          "Your key was rejected. Reconnect to resume reading data.",
          true,
        );
        return;
      }
      const delay = response.headers.get("Retry-After");
      if (delay && /^\d+$/.test(delay))
        retry = Math.max(1000, Number(delay) * 1000);
      if (retry > 60000) {
        stopLive();
        notice(
          $("#global-notice"),
          `Live paused: server requested a ${delay}-second retry delay. Resume after that interval.`,
        );
        return;
      }
      if (![429, 500, 502, 503, 504].includes(response.status)) {
        stopLive();
        notice(
          $("#global-notice"),
          `Live stream returned HTTP ${response.status}.`,
          true,
        );
        return;
      }
      throw new Error(`HTTP ${response.status}`);
    }
    if (!response.headers.get("Content-Type")?.includes("text/event-stream")) {
      await response.body?.cancel();
      stopLive();
      notice(
        $("#global-notice"),
        "This endpoint did not return a Fluxgate event stream.",
        true,
      );
      return;
    }
    liveStatus("Live connected", true);
    attempt = 0;
    scheduleReconcile();
    const reader = response.body.getReader(),
      decoder = new TextDecoder(),
      parser = new SSEParser(addEvent, (value) => {
        retry = value;
      });
    try {
      while (state.live && session === state.session) {
        const { done, value } = await reader.read();
        if (
          done ||
          !state.live ||
          controller.signal.aborted ||
          session !== state.session ||
          streamController !== controller
        )
          break;
        heartbeat();
        parser.push(decoder.decode(value, { stream: true }));
      }
    } finally {
      await reader.cancel().catch(() => {});
    }
  } catch {
    /* Connection failures are visible and retried with bounded backoff. */
  } finally {
    clearTimeout(idleTimer);
  }
  if (
    state.live &&
    client === state.client &&
    streamController === controller
  ) {
    liveStatus("Reconnecting…");
    scheduleReconcile();
    liveTimer = setTimeout(
      () => streamLoop(attempt + 1),
      Math.max(retry, Math.min(30000, 1000 * 2 ** Math.min(attempt, 5))) +
        Math.random() * 700,
    );
  }
}
["#live-toggle", "#activity-toggle"].forEach((id) =>
  $(id).addEventListener("click", () =>
    state.live ? stopLive() : startLive(),
  ),
);
let resumeLive = false;
document.addEventListener("visibilitychange", () => {
  if (document.hidden) {
    resumeLive = state.live;
    stopLive();
  } else if (resumeLive) {
    resumeLive = false;
    runQuery();
    startLive();
  }
});
window.addEventListener("pagehide", () => {
  stopLive();
  queryController?.abort();
  catalogController?.abort();
  cardController?.abort();
  if (state.client instanceof APIClient) state.client.token = "";
});
function renderActivity() {
  $("#activity-feed").innerHTML = state.events.length
    ? state.events
        .map(
          (event) =>
            `<div class="activity-event"><span class="event-dot" aria-hidden="true"></span><div><h3>${escape(event.metric)} <span class="kind-badge">${escape(event.kind)}</span></h3><p>${escape(labelOf(event))}</p><p>Window ${escape(new Date(event.window_start).toISOString())} · replacement totals</p><div class="event-values"><span>count <b>${format(event.count, 0)}</b></span><span>sum <b>${format(event.sum, 2)}</b></span><span>min <b>${format(event.min, 2)}</b></span><span>max <b>${format(event.max, 2)}</b></span></div></div><time>${time(event.received)} UTC</time></div>`,
        )
        .join("")
    : '<div class="empty-state"><strong>Waiting for the next update</strong><p>Enable live updates to follow the selected metric. Reconnects start from now.</p></div>';
}
$("#activity-clear").addEventListener("click", () => {
  state.events = [];
  renderActivity();
});

function persistViews() {
  try {
    localStorage.setItem("fluxgate.views.v1", JSON.stringify(state.saved));
  } catch {
    toast("Browser storage is unavailable. Views last only for this tab.");
  }
  $("#saved-count").textContent = state.saved.length;
}
$("#save-view").addEventListener("click", () => {
  if (!state.settings.metric) return;
  $("#view-name").value =
    `${state.settings.metric} · ${state.settings.aggregation}`;
  openDialog("#save-dialog");
});
$("#save-form").addEventListener("submit", (event) => {
  event.preventDefault();
  if (state.saved.length >= 30)
    return toast("Keep up to 30 saved views. Remove one to save another.");
  state.saved.unshift({
    id: crypto.randomUUID(),
    name: $("#view-name").value.trim(),
    settings: structuredClone(state.settings),
    mode: state.mode === "demo" ? "demo" : "api",
  });
  persistViews();
  renderSaved();
  $("#save-dialog").close();
  toast("View saved on this browser.");
});
function renderSaved() {
  $("#saved-views").innerHTML = state.saved.length
    ? state.saved
        .map(
          (view, index) =>
            `<article class="saved-card"><span class="kind-badge">${view.mode === "demo" ? "DEMO VIEW" : "API VIEW"}</span><h3>${escape(view.name)}</h3><p class="mono">${escape(view.settings.metric)}</p><p>${escape(view.settings.aggregation)} · ${escape(view.settings.range)} · ${Object.keys(view.settings.filters || {}).length} filters</p><div><button class="button" data-open-view="${index}">Open view →</button><button class="button quiet" data-delete-view="${index}" aria-label="Delete ${escape(view.name)}">Delete</button></div></article>`,
        )
        .join("")
    : '<div class="empty-state"><strong>A good query is worth keeping.</strong><p>Save a view from the metric explorer to find it here.</p></div>';
  $("#saved-views")
    .querySelectorAll("[data-open-view]")
    .forEach((button) =>
      button.addEventListener("click", () => {
        const view = state.saved[Number(button.dataset.openView)];
        if (view.mode !== state.mode)
          return toast(
            `This is a ${view.mode === "api" ? "connected API" : "demo"} view. Switch workspace first.`,
          );
        try {
          queryParams(view.settings);
          state.settings = structuredClone(view.settings);
          if (
            !state.metrics.some(
              (metric) => metric.metric === state.settings.metric,
            )
          ) {
            $("#metric").add(
              new Option(state.settings.metric, state.settings.metric),
            );
          }
          syncControls();
          setView("explore");
          runQuery();
          restartLive();
        } catch {
          toast("This saved view is invalid. Delete it and save a new query.");
        }
      }),
    );
  $("#saved-views")
    .querySelectorAll("[data-delete-view]")
    .forEach((button) =>
      button.addEventListener("click", () => {
        state.saved.splice(Number(button.dataset.deleteView), 1);
        persistViews();
        renderSaved();
      }),
    );
}
$("#share").addEventListener("click", async () => {
  const url = new URL(location.href);
  url.search = "";
  url.hash = new URLSearchParams({
    metric: state.settings.metric,
    agg: state.settings.aggregation,
    range: state.settings.range,
    ...(state.settings.range === "custom"
      ? { from: state.settings.from, to: state.settings.to }
      : {}),
    filters: JSON.stringify(state.settings.filters),
  }).toString();
  try {
    await navigator.clipboard.writeText(url.href);
    toast(
      "Query link copied. It contains filters but no API URL, key, or data.",
    );
  } catch {
    toast("Clipboard unavailable. Save the view on this browser instead.");
  }
});
function applySharedView() {
  if (!location.hash || location.hash.length > 10000) return;
  try {
    const params = new URLSearchParams(location.hash.slice(1)),
      settings = {
        metric: params.get("metric"),
        aggregation: params.get("agg"),
        range: params.get("range"),
        from: params.get("from"),
        to: params.get("to"),
        filters: JSON.parse(params.get("filters") || "{}"),
      };
    queryParams(settings);
    state.settings = settings;
    pendingSharedView = structuredClone(settings);
    toast("Shared query loaded. Connect your API to read its data.");
  } catch {
    toast("The shared query link is invalid. Showing the default workspace.");
  }
}
function renderCommand() {
  const query = $("#command-search").value.toLowerCase();
  const items = state.metrics
    .filter((metric) => metric.metric.toLowerCase().includes(query))
    .slice(0, 30);
  $("#command-results").innerHTML =
    items
      .map(
        (item, i) =>
          `<button data-command="${i}">${escape(item.metric)}<span>${escape(item.kind)}</span></button>`,
      )
      .join("") || '<div class="empty-state">No matching metrics.</div>';
  $("#command-results")
    .querySelectorAll("button")
    .forEach((button) =>
      button.addEventListener("click", () => {
        $("#command-dialog").close();
        selectMetric(items[Number(button.dataset.command)]);
      }),
    );
}
function openCommand() {
  openDialog("#command-dialog");
  $("#command-search").value = "";
  renderCommand();
  $("#command-search").focus();
}
$("#command-button").addEventListener("click", openCommand);
$("#command-search").addEventListener("input", renderCommand);
$("#command-dialog").addEventListener("keydown", (event) => {
  const buttons = [...$("#command-results").querySelectorAll("button")],
    index = buttons.indexOf(document.activeElement);
  if (event.key === "ArrowDown" && buttons.length) {
    event.preventDefault();
    buttons[(index + 1) % buttons.length].focus();
  }
  if (event.key === "ArrowUp" && buttons.length) {
    event.preventDefault();
    buttons[(index - 1 + buttons.length) % buttons.length].focus();
  }
  if (event.key === "Enter" && event.target === $("#command-search")) {
    event.preventDefault();
    buttons[0]?.click();
  }
});
document.addEventListener("keydown", (event) => {
  if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
    event.preventDefault();
    openCommand();
  }
});
applySharedView();
updateConnection();
renderSaved();
renderActivity();
loadCatalog();
