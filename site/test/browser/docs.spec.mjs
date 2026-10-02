import { test, expect } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { readFile } from "node:fs/promises";

test("every published page loads without browser errors or missing resources", async ({
  page,
}) => {
  test.setTimeout(120000);
  const manifest = JSON.parse(
    await readFile(
      new URL("../../.build-manifest.json", import.meta.url),
      "utf8",
    ),
  );
  const failures = [];
  page.on("pageerror", (error) => failures.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") failures.push(message.text());
  });
  page.on("response", (response) => {
    if (response.status() >= 400)
      failures.push(`${response.status()} ${response.url()}`);
  });
  for (const item of manifest.pages) {
    const response = await page.goto(item.route);
    expect(response.status(), item.route).toBe(200);
    await page.waitForLoadState("networkidle");
    await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
    expect(failures, item.route).toEqual([]);
  }
});

test("search navigates to a matching section and handles network failure", async ({
  page,
}) => {
  await page.goto("/");
  await page.keyboard.press("Control+k");
  await page.getByRole("searchbox").fill("Idempotency-Key");
  await expect(page.locator(".search-result").first()).toBeVisible();
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Enter");
  await expect(page).not.toHaveURL(/:\d+\/$/);
  await expect(page.getByRole("dialog")).not.toBeVisible();
  await page.reload();
  await page.route("**/search-index.json", (route) => route.abort());
  await page.getByRole("button", { name: /Search documentation/ }).click();
  await page.getByRole("searchbox").fill("recovery");
  await expect(page.locator(".search-status")).toContainText("could not load");
});

test("endpoint examples, schemas, copy, and theme work", async ({
  page,
  context,
}) => {
  await context.grantPermissions(["clipboard-read", "clipboard-write"]);
  await page.goto("/api-reference/ingest-points/");
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Submit metric points",
  );
  await page.getByRole("tab", { name: "JavaScript" }).click();
  await expect(page.getByRole("tabpanel")).toContainText("await fetch");
  await page
    .getByRole("tabpanel")
    .getByRole("button", { name: "Copy code" })
    .click();
  await expect
    .poll(() => page.evaluate(() => navigator.clipboard.readText()))
    .toContain("await fetch");
  await page.getByRole("tab", { name: "JavaScript" }).press("ArrowRight");
  await expect(page.getByRole("tabpanel")).toContainText("urllib.request");
  await page
    .locator("details.response")
    .filter({ hasText: "409" })
    .locator("summary")
    .first()
    .click();
  await expect(
    page.locator("details.response[open]").filter({ hasText: "409" }),
  ).toContainText("conflict");
  const before = await page.locator("html").getAttribute("data-theme");
  await page.getByRole("button", { name: "Switch color theme" }).click();
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute(
    "data-theme",
    before === "dark" ? "light" : "dark",
  );
});

test("mobile navigation and content fit narrow screens", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page
    .getByRole("navigation", { name: "Documentation", exact: true })
    .getByRole("link", { name: "Getting started", exact: true })
    .click();
  await expect(page).toHaveURL(/\/guides\/getting-started\//);
  for (const route of [
    "/",
    "/api-reference/ingest-points/",
    "/guides/configuration/",
    "/guides/architecture/",
  ]) {
    await page.goto(route);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  }
  await page.getByRole("button", { name: "Open navigation" }).click();
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("button", { name: "Open navigation" }),
  ).toHaveAttribute("aria-expanded", "false");
});

test("diagrams render, downloads are served, and unknown routes return 404", async ({
  page,
  request,
}) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/guides/architecture/");
  await expect(page.locator(".mermaid svg")).toHaveCount(2, { timeout: 30000 });
  expect(errors).toEqual([]);
  const contract = await request.get("/openapi.json");
  expect(contract.ok()).toBe(true);
  expect(Object.keys((await contract.json()).paths)).toHaveLength(8);
  expect((await request.get("/openapi.yaml")).ok()).toBe(true);
  for (const [alias, destination] of [
    ["/api/", "/api-reference/"],
    ["/docs/", "/guides/"],
  ]) {
    const redirect = await request.get(alias, { maxRedirects: 0 });
    expect(redirect.status()).toBe(308);
    expect(redirect.headers().location).toBe(destination);
  }
  const response = await page.goto("/this-page-does-not-exist/");
  expect(response.status()).toBe(404);
  await expect(page.getByRole("heading", { level: 1 })).toContainText(
    "back on track",
  );
});

for (const theme of ["light", "dark"])
  test(`key pages pass accessibility checks in ${theme} mode`, async ({
    page,
  }) => {
    for (const route of [
      "/",
      "/api-reference/ingest-points/",
      "/guides/getting-started/",
    ]) {
      await page.goto(route);
      await page.evaluate((theme) => {
        document.documentElement.dataset.theme = theme;
      }, theme);
      const results = await new AxeBuilder({ page })
        .withTags(["wcag2a", "wcag2aa", "wcag21aa"])
        .analyze();
      expect(results.violations).toEqual([]);
    }
  });

test("guides and endpoint contracts remain readable without JavaScript", async ({
  browser,
}) => {
  const context = await browser.newContext({ javaScriptEnabled: false });
  const page = await context.newPage();
  await page.goto(
    (process.env.BASE_URL || "http://127.0.0.1:4173") +
      "/api-reference/query-rollups/",
  );
  await expect(page.getByRole("heading", { level: 1 })).toHaveText(
    "Query rollups",
  );
  await expect(page.locator("article")).toContainText(
    "whole committed window totals",
  );
  await page
    .locator("details.response")
    .filter({ hasText: "422" })
    .locator("summary")
    .first()
    .click();
  await expect(
    page.locator("details.response[open]").filter({ hasText: "422" }),
  ).toContainText("byte budget");
  await context.close();
});
