import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { readFile } from "node:fs/promises";

const key = "fxg_browser_fixture-secret";
const catalog = {
  metrics: [
    { metric: "requests.total", kind: "counter", series_count: 2 },
    { metric: "queue.depth", kind: "gauge", series_count: 2 },
  ],
};
function result(url, extra = {}) {
  const metric = url.searchParams.get("metric");
  const from = Date.now() - 3600000;
  return {
    metric,
    kind: metric === "requests.total" ? "counter" : "gauge",
    aggregation: url.searchParams.get("agg"),
    from: new Date(from).toISOString(),
    to: new Date().toISOString(),
    truncated: false,
    series: ["west", "east"]
      .filter(
        (region) =>
          !url.searchParams.has("label.region") ||
          url.searchParams.get("label.region") === region,
      )
      .map((region, index) => ({
        kind: metric === "requests.total" ? "counter" : "gauge",
        labels: { region, service: "checkout" },
        points: Array.from({ length: 60 }, (_, i) => ({
          t: new Date(from + i * 60000).toISOString(),
          v: i + index * 100,
        })),
      })),
    ...extra,
  };
}
async function apiFixture(page, respond) {
  const requests = [];
  await page.route("**/v1/**", async (route) => {
    const request = route.request(),
      url = new URL(request.url());
    requests.push(request);
    expect(request.headers().authorization).toBe(`Bearer ${key}`);
    expect(request.url()).not.toContain(key);
    expect(request.headers().cookie).toBeUndefined();
    if (respond && (await respond(route, url))) return;
    const data =
      url.pathname === "/v1/metrics"
        ? catalog
        : url.pathname === "/v1/labels"
          ? url.searchParams.has("label")
            ? { values: ["west", "east"] }
            : { labels: ["region", "service"] }
          : result(url);
    await route.fulfill({ json: data });
  });
  return requests;
}
async function connect(page) {
  await page.locator("#connect").click();
  await page
    .getByLabel("Query API URL", { exact: true })
    .fill(new URL(page.url()).origin);
  await page.getByLabel("Tenant API key", { exact: true }).fill(key);
  await page.locator("#connection-submit").click();
  await expect(page.locator("#workspace-name")).toHaveText(
    "Connected workspace",
  );
  await expect(page.locator("#connection-dialog")).not.toBeVisible();
  await expect(page.locator("#refresh-status")).toContainText("Updated");
  await expect(page.locator("#metric")).toHaveValue("requests.total");
}

test("demo exploration, exact filters, exports, saved views, and share links work", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/dashboard/");
  await expect(page.locator(".stat-card")).toHaveCount(4);
  await expect(page.locator("#demo-banner")).toBeVisible();
  await expect(page.locator(".series-path")).toHaveCount(3);
  await page.keyboard.press("Control+k");
  await page.locator("#command-search").fill("queue.depth");
  await page.keyboard.press("Enter");
  await expect(page.locator("#chart-title")).toHaveText("queue.depth");
  await expect(page.locator("#aggregation")).toHaveValue("last");
  await page.locator("#add-filter").click();
  await page.getByLabel("Label key", { exact: true }).fill("region");
  await page.getByLabel("Value", { exact: true }).fill("us-east1");
  await page.getByRole("button", { name: "Apply filter" }).click();
  await expect(page.locator("#series-summary")).toHaveText(
    "1 charted / 1 returned",
  );
  await page.locator("#show-points").click();
  await expect(page.locator("#data-body tr")).toHaveCount(25);
  await page.locator("#next").click();
  await expect(page.locator("#table-summary")).toContainText("26–50");
  const downloadPromise = page.waitForEvent("download");
  await page.locator("#export").click();
  const csv = await readFile(await (await downloadPromise).path(), "utf8");
  expect(csv.split("\r\n")).toHaveLength(61);
  expect(csv).toContain("us-east1");
  await page.locator("#save-view").click();
  await page.getByLabel("View name").fill("Checkout queue");
  await page.locator("#save-form button[type=submit]").click();
  await page.locator("#share").click();
  const link = await page.evaluate(() => navigator.clipboard.readText());
  expect(link).toContain("metric=queue.depth");
  expect(link).not.toContain("fxg_");
  await page.reload();
  await page.locator("[data-view=saved]").click();
  await expect(page.locator(".saved-card")).toContainText("Checkout queue");
  await page.getByRole("button", { name: "Open view" }).click();
  await expect(page.locator("#series-summary")).toHaveText(
    "1 charted / 1 returned",
  );
  await page.goto(link);
  await expect(page.locator("#filter-chips")).toContainText("us-east1");
  await expect(page.locator("#chart-title")).toHaveText("queue.depth");
  expect(errors).toEqual([]);
});

test("API connection preserves a shared query, scopes requests, and clears credentials", async ({
  page,
}) => {
  const requests = await apiFixture(page);
  const shared = new URLSearchParams({
    metric: "requests.total",
    agg: "sum",
    range: "15m",
    filters: JSON.stringify({ region: "east" }),
  });
  await page.goto(`/dashboard/#${shared}`);
  await connect(page);
  await expect(page.locator("#chart-title")).toHaveText("requests.total");
  await expect(page.locator("#series-summary")).toHaveText(
    "1 charted / 1 returned",
  );
  expect(
    requests.some(
      (request) =>
        new URL(request.url()).searchParams.get("label.region") === "east",
    ),
  ).toBe(true);
  await expect(page.locator("#demo-banner")).not.toBeVisible();
  await page.locator("#save-view").click();
  await page.locator("#save-form button[type=submit]").click();
  const storage = await page.evaluate(() =>
    JSON.stringify({ ...localStorage, ...sessionStorage }),
  );
  expect(storage).toContain("requests.total");
  expect(storage).not.toContain(key);
  await expect(page.locator("#api-key")).toHaveValue("");
  await page.locator("#connect").click();
  await page.locator("#use-demo").click();
  await expect(page.locator("#workspace-name")).toHaveText("Demo workspace");
  await expect(page.locator("#chart-title")).not.toHaveText("requests.total");
  await page.reload();
  await expect(page.locator("#workspace-name")).toHaveText("Demo workspace");
});

test("expired credentials clear results and stop live reads", async ({
  page,
}) => {
  let reject = false;
  await apiFixture(page, async (route) => {
    if (!reject) return false;
    await route.fulfill({ status: 401 });
    return true;
  });
  await page.goto("/dashboard/");
  await connect(page);
  reject = true;
  await page.locator("#refresh").click();
  await expect(page.locator("#workspace-name")).toHaveText("Disconnected");
  await expect(page.locator("#data-body")).toContainText("No matching data");
  await expect(page.locator("#selected-value")).toHaveText("—");
  await expect(page.locator("#global-notice")).toContainText("rejected");
  await expect(page.locator("#live-toggle")).toHaveAttribute(
    "aria-checked",
    "false",
  );
  await expect(page.locator("#query-form button[type=submit]")).toBeEnabled();
});

test("partial results and busy failures remain explicit without losing the previous chart", async ({
  page,
}) => {
  let busy = false;
  await apiFixture(page, async (route, url) => {
    if (url.pathname !== "/v1/query") return false;
    await route.fulfill(
      busy
        ? {
            status: 503,
            headers: { "Retry-After": "5", "X-Request-Id": "fixture-overload" },
          }
        : {
            json: result(url, {
              truncated: true,
              warnings: ["Query reached the point budget."],
            }),
          },
    );
    return true;
  });
  await page.goto("/dashboard/");
  await connect(page);
  await expect(page.locator("#query-notice")).toContainText("Partial result");
  await expect(page.locator("#query-notice")).toContainText("point budget");
  const title = await page.locator("#chart-title").textContent();
  busy = true;
  await page.getByRole("button", { name: "Run query" }).click();
  await expect(page.locator("#query-notice")).toContainText("stale");
  await expect(page.locator("#query-notice")).toContainText("fixture-overload");
  await expect(page.locator("#chart-title")).toHaveText(title);
  await expect(page.locator("#live-toggle")).toHaveAttribute(
    "aria-checked",
    "false",
  );
});

test("a newer query cancels an older request and keeps the newest controls and result", async ({
  page,
}) => {
  let delayed = false,
    release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  await apiFixture(page, async (route, url) => {
    if (
      delayed &&
      url.pathname === "/v1/query" &&
      url.searchParams.get("agg") === "avg"
    ) {
      await gate;
      await route.fulfill({ json: result(url) }).catch(() => {});
      return true;
    }
    return false;
  });
  await page.goto("/dashboard/");
  await connect(page);
  delayed = true;
  await page.locator("#aggregation").selectOption("avg");
  await expect(page.locator("#refresh-status")).toHaveText("Querying…");
  await page.locator("#aggregation").selectOption("max");
  await expect(page.locator("#chart-description")).toContainText("max");
  release();
  await expect(page.locator("#chart-description")).toContainText("max");
  await expect(page.locator("#query-form button[type=submit]")).toBeEnabled();
});

test("authenticated live events reconcile through REST and activity can pause", async ({
  page,
}) => {
  let queryCount = 0,
    streams = 0;
  await apiFixture(page, async (route, url) => {
    if (url.pathname === "/v1/query") queryCount++;
    if (url.pathname !== "/v1/stream") return false;
    streams++;
    const event = {
      metric: url.searchParams.get("metric"),
      kind: "counter",
      labels: { region: "west" },
      window_start: new Date().toISOString(),
      count: 7,
      sum: 42,
      min: 1,
      max: 8,
    };
    await route.fulfill({
      contentType: "text/event-stream",
      body: `retry: 1000\r\n\r\n: heartbeat\r\n\r\nevent: rollup\r\ndata: ${JSON.stringify(event)}\r\n\r\n`,
    });
    return true;
  });
  await page.goto("/dashboard/");
  await connect(page);
  await expect(page.locator(".stat-card")).toHaveCount(2);
  const before = queryCount;
  await page.locator("[data-view=activity]").click();
  await expect(page.locator(".activity-event").first()).toContainText("42");
  await expect
    .poll(() => queryCount, { timeout: 10000 })
    .toBeGreaterThan(before);
  expect(streams).toBeGreaterThan(0);
  await page.getByRole("button", { name: "Pause live" }).click();
  await expect(page.locator("#activity-status")).toContainText("Live paused");
  await page.locator("#activity-clear").click();
  await expect(page.locator(".activity-event")).toHaveCount(0);
  await page.locator("[data-view=explore]").click();
  await page.getByRole("button", { name: "Custom", exact: true }).click();
  await page.locator("#live-toggle").click();
  await expect(page.locator("#toast")).toContainText("rolling time range");
});

test("dashboard is accessible in both themes and dialogs", async ({ page }) => {
  await page.goto("/dashboard/");
  await expect(page.locator(".stat-card")).toHaveCount(4);
  for (const theme of ["dark", "light"]) {
    for (const view of ["overview", "activity", "saved"]) {
      await page.locator(`[data-view=${view}]`).click();
      const audit = await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
        .analyze();
      expect(audit.violations, `${theme} ${view}`).toEqual([]);
    }
    await page.locator("#connect").click();
    expect(
      (
        await new AxeBuilder({ page })
          .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
          .analyze()
      ).violations,
    ).toEqual([]);
    await page.getByRole("button", { name: "Close connection dialog" }).click();
    await page.locator("#theme").click();
  }
});

test("maximum point responses render a bounded chart and export every point", async ({
  page,
}) => {
  await apiFixture(page, async (route, url) => {
    if (url.pathname !== "/v1/query") return false;
    const data = result(url);
    const from = Date.parse(data.from);
    data.series = [
      {
        kind: data.kind,
        labels: { region: "west" },
        points: Array.from({ length: 50000 }, (_, i) => ({
          t: new Date(from + i * 70).toISOString(),
          v: i === 12345 ? 9000 : i % 200,
        })),
      },
    ];
    await route.fulfill({ json: data });
    return true;
  });
  await page.goto("/dashboard/");
  await connect(page);
  await expect(page.locator("#query-duration")).toContainText("50K points");
  const commands = await page
    .locator(".series-path")
    .evaluateAll((paths) =>
      paths.reduce(
        (count, path) => count + path.getAttribute("d").match(/[ML]/g).length,
        0,
      ),
    );
  expect(commands).toBeLessThanOrEqual(600);
  await page.locator("#show-points").click();
  await expect(page.locator("#data-body tr")).toHaveCount(25);
  const download = page.waitForEvent("download");
  await page.locator("#export").click();
  const output = await readFile(await (await download).path(), "utf8");
  expect(output.split("\r\n")).toHaveLength(50001);
  expect(output).toContain('"9000"');
});

test("custom shared ranges preserve UTC instants across timezones", async ({
  browser,
  baseURL,
}) => {
  const source = await browser.newContext({
    timezoneId: "UTC",
    permissions: ["clipboard-read", "clipboard-write"],
  });
  const recipient = await browser.newContext({
    timezoneId: "America/Los_Angeles",
  });
  try {
    const page = await source.newPage();
    await page.goto(`${baseURL}/dashboard/`);
    await expect(page.locator(".stat-card")).toHaveCount(4);
    await page.getByRole("button", { name: "Custom", exact: true }).click();
    await page.locator("#from").fill("2026-10-02T12:00");
    await page.locator("#to").fill("2026-10-02T13:00");
    await page.getByRole("button", { name: "Run query" }).click();
    await page.locator("#share").click();
    const link = await page.evaluate(() => navigator.clipboard.readText());
    expect(new URLSearchParams(new URL(link).hash.slice(1)).get("from")).toBe(
      "2026-10-02T12:00:00.000Z",
    );
    const second = await recipient.newPage();
    await second.goto(link);
    await expect(second.locator("#from")).toHaveValue("2026-10-02T05:00");
    await expect(second.locator("#to")).toHaveValue("2026-10-02T06:00");
  } finally {
    await source.close();
    await recipient.close();
  }
});

test("mobile workspace and dialogs fit narrow screens", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/dashboard/");
  await expect(page.locator(".stat-card")).toHaveCount(4);
  for (const view of ["overview", "explore", "activity", "saved"]) {
    await page.locator(`[data-view=${view}]`).click();
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
    await expect(page.locator("#page-title")).toBeVisible();
  }
  await page.locator("#connect").click();
  await expect(page.locator("#connection-dialog")).toBeVisible();
  expect(
    await page
      .locator("#connection-dialog")
      .evaluate((node) => node.scrollWidth <= node.clientWidth),
  ).toBe(true);
});
