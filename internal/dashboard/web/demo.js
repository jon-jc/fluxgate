// Deterministic synthetic fixtures; no demo value is presented as production.
const metrics = [
  ["http.requests", "counter", "Request volume", "sum"],
  ["http.request.duration_ms", "histogram", "Request latency", "p95"],
  ["queue.depth", "gauge", "Queue depth", "last"],
  ["worker.processing_ms", "histogram", "Processing time", "p95"],
  ["cache.hit_ratio", "gauge", "Cache hit ratio", "avg"],
  ["http.errors", "counter", "Request errors", "sum"],
  ["system.cpu_percent", "gauge", "CPU utilization", "avg"],
  ["db.query.duration_ms", "histogram", "Database latency", "p95"],
];
export const demoMetrics = metrics.map(
  ([metric, kind, title, aggregation]) => ({
    metric,
    kind,
    title,
    aggregation,
    series_count: 3,
  }),
);
const regions = ["us-central1", "us-east1", "europe-west1"];
export class DemoClient {
  async metrics() {
    const now = Math.floor(Date.now() / 60000) * 60000;
    return {
      metrics: demoMetrics.map((metric) => ({
        ...metric,
        oldest_window: new Date(now - 86400000).toISOString(),
        newest_window: new Date(now).toISOString(),
      })),
    };
  }
  async labels(metric, label) {
    return label
      ? { metric, label, values: label === "region" ? regions : ["checkout"] }
      : { metric, labels: ["region", "service"] };
  }
  async query(settings) {
    const end =
      settings.range === "custom" ? +new Date(settings.to) : Date.now();
    const duration =
      { "15m": 900000, "1h": 3600000, "6h": 21600000, "24h": 86400000 }[
        settings.range
      ] || 3600000;
    const start =
      settings.range === "custom" ? +new Date(settings.from) : end - duration;
    const metric = demoMetrics.find((item) => item.metric === settings.metric);
    if (!metric)
      return {
        metric: settings.metric,
        kind: "mixed",
        aggregation: settings.aggregation,
        from: new Date(start).toISOString(),
        to: new Date(end).toISOString(),
        series: [],
        truncated: false,
        warnings: [
          "This metric is not in the synthetic demo. Connect your API to run the shared query.",
        ],
      };
    const step = 60000;
    const series = regions
      .map((region, i) => {
        const points = [];
        for (let t = Math.ceil(start / step) * step; t < end; t += step) {
          const n = Math.floor(t / step),
            wave =
              Math.sin(n / 9 + i) * 0.12 + Math.sin(n / 3.7 + i * 2) * 0.055;
          const burst = Math.max(0, Math.sin(n / 31 + i * 0.3)) ** 16;
          const bases = {
            "http.requests": 38000,
            "http.request.duration_ms": 85,
            "queue.depth": 36,
            "worker.processing_ms": 24,
            "cache.hit_ratio": 0.96,
            "http.errors": 35,
            "system.cpu_percent": 48,
            "db.query.duration_ms": 12,
          };
          let v =
            bases[metric.metric] * (1 + wave + burst * 0.6) * (1 - i * 0.18);
          if (metric.metric === "cache.hit_ratio")
            v = Math.min(0.999, 0.965 + wave * 0.09 - burst * 0.03 - i * 0.004);
          if (
            ["counter"].includes(metric.kind) ||
            metric.metric === "queue.depth"
          )
            v = Math.round(v);
          if (settings.aggregation === "count")
            v = metric.kind === "counter" ? Math.round(v) : 240 + i * 10;
          else if (metric.kind === "histogram")
            v *=
              {
                p50: 0.6,
                p90: 0.9,
                p95: 1,
                p99: 1.3,
                avg: 0.65,
                min: 0.2,
                max: 1.5,
                sum: 156,
                last: 0.8,
              }[settings.aggregation] || 1;
          else if (
            metric.kind === "counter" &&
            ["avg", "min", "max", "last"].includes(settings.aggregation)
          )
            v = 1;
          points.push({ t: new Date(t).toISOString(), v });
        }
        return {
          kind: metric.kind,
          labels: { region, service: "checkout" },
          points,
        };
      })
      .filter((item) =>
        Object.entries(settings.filters || {}).every(
          ([key, value]) => item.labels[key] === value,
        ),
      );
    const percentile = settings.aggregation.startsWith("p");
    return {
      metric: metric.metric,
      kind: metric.kind,
      aggregation: settings.aggregation,
      from: new Date(start).toISOString(),
      to: new Date(end).toISOString(),
      series: percentile && metric.kind !== "histogram" ? [] : series,
      truncated: false,
      warnings:
        percentile && metric.kind !== "histogram"
          ? ["Percentiles require histogram data."]
          : [],
    };
  }
}
