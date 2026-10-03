import { test } from "node:test";
import assert from "node:assert/strict";
import {
  APIClient,
  SSEParser,
  chartSegments,
  csv,
  decimate,
  endpoint,
  keyOf,
  normalizeResult,
  queryParams,
  readJSON,
} from "../../internal/dashboard/web/core.js";

test("credentials can only target explicit safe service origins", () => {
  assert.equal(
    endpoint("http://localhost:8082", "http://localhost:8082"),
    "http://localhost:8082",
  );
  assert.equal(
    endpoint("https://query.example.com/", "https://docs.example.com"),
    "https://query.example.com",
  );
  for (const value of [
    "http://elsewhere.example",
    "https://u:p@host.test",
    "https://host.test/?key=x",
    "https://host.test/path",
    "javascript:alert(1)",
  ])
    assert.throws(() => endpoint(value, "https://docs.example.com"));
});
test("query filters are encoded and historical ranges are validated", () => {
  const params = queryParams({
    metric: "http.requests",
    aggregation: "sum",
    range: "15m",
    filters: { service: "checkout&agg=p99" },
  });
  assert.equal(params.get("label.service"), "checkout&agg=p99");
  assert.equal(params.get("agg"), "sum");
  assert.throws(() =>
    queryParams({
      metric: "x",
      aggregation: "avg",
      range: "custom",
      from: "2026-10-02",
      to: "2026-10-01",
    }),
  );
  assert.throws(() => queryParams({ metric: "x", aggregation: "p100" }));
  assert.equal(
    keyOf({ kind: "gauge", labels: { b: "2", a: "1" } }),
    keyOf({ kind: "gauge", labels: { a: "1", b: "2" } }),
  );
});
test("downsampling preserves a narrow spike and endpoints within a bound", () => {
  const points = Array.from({ length: 12000 }, (_, t) => ({
    t,
    v: t === 5849 ? 10000 : Math.sin(t),
  }));
  const sampled = decimate(points);
  assert(sampled.length <= 600);
  assert(sampled.some((point) => point.v === 10000));
  assert.equal(sampled[0], points[0]);
  assert.equal(sampled.at(-1), points.at(-1));
  assert(sampled.every((point, i) => !i || point.t >= sampled[i - 1].t));
});
test("SSE tolerates chunk boundaries, heartbeats, retry advice, and replacement totals", () => {
  const events = [],
    retries = [],
    parser = new SSEParser(
      (event) => events.push(event),
      (value) => retries.push(value),
    );
  const stream =
    ': keep-alive\r\n\r\nretry: 4000\r\n\r\nevent: rollup\r\ndata: {"sum":2}\r\n\r\nevent: rollup\ndata: {"sum":3}\n\n';
  for (const character of stream) parser.push(character);
  assert.deepEqual(events, [{ sum: 2 }, { sum: 3 }]);
  assert.deepEqual(retries, [4000]);
  assert.throws(() => parser.push("a".repeat(262145)));
});
test("results are bounded and CSV escapes spreadsheet formulas", () => {
  const result = normalizeResult({
    metric: "=EVIL()",
    from: "2026-10-02T12:00:00Z",
    to: "2026-10-02T13:00:00Z",
    truncated: true,
    series: [
      {
        kind: "gauge",
        labels: {},
        points: [{ t: "2026-10-02T12:00:00Z", v: -2 }],
      },
    ],
  });
  const output = csv(result);
  assert(output.includes('"\'=EVIL()"'));
  assert(output.includes('"true"'));
  assert(output.includes('"-2"'));
  assert(!output.includes("'-2"));
  assert.equal(result.pointCount, 1);
  assert.throws(() => normalizeResult({ ...result, metric: null }));
  assert.throws(() => normalizeResult({ ...result, to: result.from }));
  assert.throws(() =>
    normalizeResult({
      ...result,
      series: [{ kind: "gauge", labels: {}, points: [{ t: "invalid", v: 1 }] }],
    }),
  );
});
test("JSON reader aborts oversized responses", async () => {
  await assert.rejects(
    () => readJSON(new Response("x".repeat(100)), 50),
    /too large/,
  );
  assert.deepEqual(await readJSON(new Response('{"series":[]}')), {
    series: [],
  });
});
test("API client omits cookies, refuses redirects, and uses authorization headers only", async () => {
  const original = globalThis.fetch;
  let options, target;
  globalThis.fetch = async (url, init) => {
    target = url;
    options = init;
    return new Response('{"metrics":[]}');
  };
  try {
    await new APIClient(
      "https://query.example.com",
      "fxg_test_secret",
    ).metrics();
    assert.equal(options.credentials, "omit");
    assert.equal(options.redirect, "error");
    assert.equal(options.headers.Authorization, "Bearer fxg_test_secret");
    assert(!target.includes("secret"));
  } finally {
    globalThis.fetch = original;
  }
});

test("chart sampling stays bounded across gaps without joining missing intervals", () => {
  const points = Array.from({ length: 50000 }, (_, i) => ({
    t: i * 1000 + Math.floor(i / 3) * 5000,
    v: i === 1234 ? 100000 : i % 20,
  }));
  const groups = chartSegments(points);
  assert(groups.flat().length <= 600);
  assert(groups.flat().some((point) => point.v === 100000));
  assert(
    groups.every((group) =>
      group.every((point) => point.segment === group[0].segment),
    ),
  );
  assert.deepEqual(chartSegments([]), []);
});
