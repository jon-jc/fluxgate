import assert from "node:assert/strict";
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { load } from "cheerio";
import YAML from "yaml";
import { slug } from "../lib.mjs";

const site = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const root = path.dirname(site);
const out = path.join(site, "dist");
const manifest = JSON.parse(
  await readFile(path.join(site, ".build-manifest.json"), "utf8"),
);
const specText = await readFile(path.join(root, "api/openapi.yaml"), "utf8");
const spec = YAML.parse(specText);
assert.equal(await readFile(path.join(out, "openapi.yaml"), "utf8"), specText);
assert.deepEqual(
  JSON.parse(await readFile(path.join(out, "openapi.json"), "utf8")),
  spec,
);
const operationIds = Object.values(spec.paths).flatMap((item) =>
  Object.values(item)
    .filter((value) => value.operationId)
    .map((value) => value.operationId),
);
assert.deepEqual(
  manifest.pages
    .filter((page) => page.operationId)
    .map((page) => page.operationId)
    .sort(),
  operationIds.sort(),
);
const sources = new Set(manifest.pages.map((page) => page.source));
for (const name of await readdir(path.join(root, "docs"), { recursive: true }))
  if (name.endsWith(".md"))
    assert(
      sources.has(`docs/${name.replaceAll("\\", "/")}`),
      `Guide not published: ${name}`,
    );
const documents = new Map();
for (const page of manifest.pages) {
  const html = await readFile(path.join(out, page.route, "index.html"), "utf8");
  const $ = load(html);
  const ids = $("[id]")
    .map((_, element) => $(element).attr("id"))
    .get();
  assert.equal(new Set(ids).size, ids.length, `Duplicate IDs: ${page.route}`);
  assert.equal($("h1").length, 1, `One h1: ${page.route}`);
  assert($("title").text().includes("Fluxgate"), `Title: ${page.route}`);
  assert(
    $('meta[name="description"]').attr("content")?.length > 10,
    `Description: ${page.route}`,
  );
  assert(
    $('link[rel="canonical"]').attr("href")?.endsWith(page.route),
    `Canonical: ${page.route}`,
  );
  assert.equal(
    $('nav[aria-label="Documentation"] [aria-current="page"]').length,
    1,
    `Active navigation: ${page.route}`,
  );
  documents.set(page.route, { $, ids: new Set(ids) });
}
let links = 0;
for (const [route, { $ }] of documents) {
  for (const link of $("a[href]").toArray()) {
    const href = $(link).attr("href");
    if (!href.startsWith("/") && !href.startsWith("#")) continue;
    const target = new URL(href, `https://docs.invalid${route}`);
    if (documents.has(target.pathname)) {
      if (target.hash)
        assert(
          documents
            .get(target.pathname)
            .ids.has(decodeURIComponent(target.hash.slice(1))),
          `Broken anchor ${route}: ${href}`,
        );
    } else {
      await readFile(
        path.join(
          out,
          target.pathname,
          target.pathname.endsWith("/") ? "index.html" : "",
        ),
      );
    }
    links++;
  }
}
const models = documents.get("/api-reference/schemas/");
for (const name of Object.keys(spec.components.schemas))
  assert(models.ids.has(slug(name)), `Missing model: ${name}`);
for (const [endpoint, item] of Object.entries(spec.paths))
  for (const operation of Object.values(item).filter(
    (value) => value.operationId,
  )) {
    const page = manifest.pages.find(
      (page) => page.operationId === operation.operationId,
    );
    const text = documents.get(page.route).$.text();
    for (const status of Object.keys(operation.responses))
      assert(text.includes(status), `Missing response ${status}: ${endpoint}`);
    for (const parameter of operation.parameters || [])
      assert(
        text.includes(parameter.name),
        `Missing parameter ${parameter.name}: ${endpoint}`,
      );
  }
const config = JSON.parse(
  await readFile(path.join(root, "vercel.json"), "utf8"),
);
const csp = config.headers
  .flatMap((rule) => rule.headers)
  .find((header) => header.key === "Content-Security-Policy").value;
assert(
  csp.includes(`'sha256-${manifest.themeHash}'`),
  "Theme script CSP hash must match",
);
const index = JSON.parse(
  await readFile(path.join(out, "search-index.json"), "utf8"),
);
for (const page of manifest.pages)
  assert(
    index.some((entry) => entry.url === page.route),
    `Unsearchable page: ${page.route}`,
  );
console.log(
  `Verified ${documents.size} pages, ${links} internal links, every guide, all API operations/models, downloads, and security policy.`,
);
