export const colors = [
  "#a994ff",
  "#56d7c0",
  "#f2b86b",
  "#70b6ff",
  "#ed91c8",
  "#c7d76c",
  "#b2bdce",
  "#fa9583",
  "#72ceef",
  "#ce9de8",
  "#85d591",
  "#efc5a2",
];
export const aggregations = [
  "sum",
  "count",
  "avg",
  "min",
  "max",
  "last",
  "p50",
  "p90",
  "p95",
  "p99",
];
export const keyOf = (series) =>
  JSON.stringify([
    series.kind,
    Object.entries(series.labels || {}).sort(([a], [b]) => a.localeCompare(b)),
  ]);
export const format = (value, digits = 1) =>
  Number.isFinite(value)
    ? new Intl.NumberFormat("en-US", {
        maximumFractionDigits: digits,
        notation:
          Math.abs(value) >= 1e12 || (value !== 0 && Math.abs(value) < 0.001)
            ? "scientific"
            : Math.abs(value) >= 10000
              ? "compact"
              : "standard",
      }).format(value)
    : "—";
export const labelOf = (series) =>
  Object.entries(series.labels || {})
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([k, v]) => `${k}=${v}`)
    .join(" · ") || "No labels";

export function endpoint(value, currentOrigin) {
  const url = new URL(value || currentOrigin);
  const same = url.origin === currentOrigin;
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !["http:", "https:"].includes(url.protocol) ||
    (url.pathname !== "/" && url.pathname !== "")
  )
    throw new Error(
      "Use a service origin, such as https://query.example.com, without a path or credentials.",
    );
  if (!same && url.protocol !== "https:")
    throw new Error(
      "Remote connections require HTTPS. For local data, open http://localhost:8082/dashboard/.",
    );
  return url.origin;
}

export function queryParams({
  metric,
  aggregation,
  range = "1h",
  from,
  to,
  filters = {},
}) {
  if (typeof metric !== "string" || !metric || metric.length > 200)
    throw new Error("Choose a metric.");
  if (!aggregations.includes(aggregation))
    throw new Error("Choose a supported aggregation.");
  const params = new URLSearchParams({ metric, agg: aggregation });
  if (range === "custom") {
    const start = new Date(from),
      end = new Date(to);
    if (!Number.isFinite(+start) || !Number.isFinite(+end) || +start >= +end)
      throw new Error("Choose a valid start and end time.");
    if (end - start > 744 * 3600000)
      throw new Error("Keep the range within 31 days.");
    params.set("from", start.toISOString());
    params.set("to", end.toISOString());
  } else {
    if (!["15m", "1h", "6h", "24h"].includes(range))
      throw new Error("Choose a supported time range.");
    params.set("from", `-${range}`);
  }
  if (
    !filters ||
    typeof filters !== "object" ||
    Array.isArray(filters) ||
    Object.keys(filters).length > 20
  )
    throw new Error("Use at most 20 label filters.");
  for (const [key, value] of Object.entries(filters)) {
    if (
      !/^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/.test(key) ||
      key.startsWith("__") ||
      typeof value !== "string" ||
      value.length > 256
    )
      throw new Error("Invalid label filter.");
    params.set(`label.${key}`, value);
  }
  return params;
}

export function normalizeResult(data) {
  if (
    !data ||
    !Array.isArray(data.series) ||
    data.series.length > 500 ||
    !Number.isFinite(Date.parse(data.from)) ||
    !Number.isFinite(Date.parse(data.to))
  )
    throw new Error("The query API returned an invalid result.");
  let count = 0;
  const series = data.series.map((item) => {
    if (
      !["counter", "gauge", "histogram"].includes(item.kind) ||
      !item.labels ||
      typeof item.labels !== "object" ||
      Array.isArray(item.labels) ||
      Object.keys(item.labels).length > 20 ||
      Object.values(item.labels).some((value) => typeof value !== "string") ||
      !Array.isArray(item.points)
    )
      throw new Error("The query API returned an invalid series.");
    count += item.points.length;
    if (count > 50000)
      throw new Error(
        "The response exceeds the dashboard’s 50,000-point limit. Narrow your query.",
      );
    const points = item.points
      .map((point) => {
        const t = Date.parse(point.t);
        if (!Number.isFinite(t) || !Number.isFinite(point.v))
          throw new Error("The query API returned an invalid point.");
        return { t, v: point.v };
      })
      .sort((a, b) => a.t - b.t);
    return { ...item, points, key: keyOf(item) };
  });
  return { ...data, series, pointCount: count };
}

// Keep each bucket's extremes in event-time order. A narrow spike survives
// decimation; the original points remain available to inspection and export.
export function decimate(points, limit = 600) {
  if (points.length <= limit) return points;
  const output = [points[0]],
    width = Math.ceil(
      (points.length - 2) / Math.max(1, Math.floor((limit - 2) / 2)),
    );
  for (let start = 1; start < points.length - 1; start += width) {
    const bucket = points.slice(
      start,
      Math.min(points.length - 1, start + width),
    );
    let min = bucket[0],
      max = bucket[0];
    for (const point of bucket) {
      if (point.v < min.v) min = point;
      if (point.v > max.v) max = point;
    }
    output.push(
      ...(min === max ? [min] : [min, max].sort((a, b) => a.t - b.t)),
    );
  }
  output.push(points.at(-1));
  return output;
}

// Detect gaps before sampling, so removing points never bridges a missing
// interval. The rendering budget applies to the whole series, including gaps.
export function chartSegments(points, limit = 600) {
  const deltas = points
    .slice(1)
    .map((point, i) => point.t - points[i].t)
    .filter((delta) => delta > 0)
    .sort((a, b) => a - b);
  const interval = deltas[Math.floor(deltas.length / 2)] || Infinity;
  let segment = 0;
  const tagged = points.map((point, i) => {
    if (i && point.t - points[i - 1].t > interval * 1.5) segment++;
    return { ...point, segment };
  });
  const groups = [];
  for (const point of decimate(tagged, limit)) {
    if (!groups.length || groups.at(-1)[0].segment !== point.segment)
      groups.push([]);
    groups.at(-1).push(point);
  }
  return groups;
}

export function csv(result) {
  // Spreadsheet formula injection is possible even inside a quoted CSV field.
  const cell = (value) =>
    `"${(typeof value === "string" ? value.replace(/^[=+\-@\t\r]/, (match) => `'${match}`) : String(value)).replaceAll('"', '""')}"`;
  return [
    "metric,kind,labels,window_start,value,truncated",
    ...result.series.flatMap((series) =>
      series.points.map((point) =>
        [
          result.metric,
          series.kind,
          JSON.stringify(series.labels),
          new Date(point.t).toISOString(),
          point.v,
          !!result.truncated,
        ]
          .map(cell)
          .join(","),
      ),
    ),
  ].join("\r\n");
}

export class SSEParser {
  buffer = "";
  data = [];
  type = "";
  size = 0;
  constructor(onEvent, onRetry = () => {}) {
    this.onEvent = onEvent;
    this.onRetry = onRetry;
  }
  push(chunk) {
    this.buffer += chunk;
    if (this.buffer.length > 262144)
      throw new Error("Live event exceeds the dashboard buffer limit.");
    let end;
    while ((end = this.buffer.indexOf("\n")) !== -1) {
      const line = this.buffer.slice(0, end).replace(/\r$/, "");
      this.buffer = this.buffer.slice(end + 1);
      if (!line) {
        if (this.type === "rollup" && this.data.length)
          this.onEvent(JSON.parse(this.data.join("\n")));
        this.data = [];
        this.type = "";
        this.size = 0;
        continue;
      }
      if (line.startsWith(":")) continue;
      const separator = line.indexOf(":");
      const field = separator < 0 ? line : line.slice(0, separator);
      const value =
        separator < 0 ? "" : line.slice(separator + 1).replace(/^ /, "");
      if (field === "event") this.type = value;
      if (field === "data") {
        this.size += value.length;
        if (this.size > 131072)
          throw new Error("Live event exceeds the dashboard buffer limit.");
        this.data.push(value);
      }
      if (field === "retry" && /^\d+$/.test(value))
        this.onRetry(Math.max(1000, Math.min(60000, Number(value))));
    }
  }
}

export async function readJSON(response, limit = 16 * 1024 * 1024) {
  const reader = response.body.getReader(),
    chunks = [];
  let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.length;
      if (size > limit)
        throw new Error("Response too large. Narrow the query.");
      chunks.push(value);
    }
  } finally {
    await reader.cancel().catch(() => {});
  }
  const joined = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.length;
  }
  return JSON.parse(new TextDecoder().decode(joined));
}

export class APIClient {
  constructor(origin, token) {
    this.origin = origin;
    this.token = token;
  }
  async get(path, params = new URLSearchParams(), signal) {
    const combined = signal
      ? AbortSignal.any([signal, AbortSignal.timeout(12000)])
      : AbortSignal.timeout(12000);
    const response = await fetch(`${this.origin}${path}?${params}`, {
      headers: {
        Authorization: `Bearer ${this.token}`,
        Accept: "application/json",
      },
      credentials: "omit",
      redirect: "error",
      cache: "no-store",
      referrerPolicy: "no-referrer",
      signal: combined,
    });
    if (!response.ok) {
      const error = new Error(
        response.status === 401
          ? "Your API key was rejected. Reconnect with a valid tenant key."
          : response.status === 422
            ? "The query exceeds a limit or contains an invalid parameter. Narrow the range or add labels."
            : response.status === 429 || response.status === 503
              ? "The query service is busy. Wait before retrying."
              : `Query service returned HTTP ${response.status}.`,
      );
      error.status = response.status;
      error.retryAfter = response.headers.get("Retry-After");
      error.requestId = response.headers.get("X-Request-Id");
      await response.body?.cancel();
      throw error;
    }
    return readJSON(response);
  }
  metrics(signal) {
    return this.get(
      "/v1/metrics",
      new URLSearchParams({ limit: "1000" }),
      signal,
    );
  }
  labels(metric, label, signal) {
    return this.get(
      "/v1/labels",
      new URLSearchParams({ metric, ...(label ? { label } : {}) }),
      signal,
    );
  }
  query(settings, signal) {
    return this.get("/v1/query", queryParams(settings), signal);
  }
}
