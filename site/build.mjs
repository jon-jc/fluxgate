import { readFile, writeFile, mkdir, readdir, cp, rm } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { Marked, Renderer } from "marked";
import YAML from "yaml";
import { createHighlighter } from "shiki";
import { build } from "esbuild";
import {
  escape as e,
  slug,
  resolveRef,
  rewriteLink,
  schemaType,
  schemaConstraints,
  operationRoute,
  repository,
} from "./lib.mjs";

const site = path.dirname(fileURLToPath(import.meta.url));
const root = path.dirname(site);
const out = path.join(site, "dist");
// Only this builder's fixed output directory is replaced.
if (out !== path.join(root, "site", "dist"))
  throw new Error("Unexpected output directory");
await rm(out, { recursive: true, force: true });
await mkdir(path.join(out, "assets"), { recursive: true });
const specText = await readFile(path.join(root, "api/openapi.yaml"), "utf8");
const spec = YAML.parse(specText);
const origin = (
  process.env.SITE_URL || "https://fluxgate-docs.vercel.app"
).replace(/\/$/, "");
if (!/^https?:\/\/[a-z\d.:-]+$/i.test(origin))
  throw new Error("SITE_URL must be an HTTP(S) origin");
const highlighter = await createHighlighter({
  themes: ["github-light", "github-dark"],
  langs: [
    "bash",
    "powershell",
    "json",
    "yaml",
    "javascript",
    "python",
    "sql",
    "go",
    "hcl",
  ],
});

const guides = [
  [
    "Metrics dashboard",
    "dashboard",
    "Explore metrics, filter series, and follow live changes.",
  ],
  [
    "Getting started",
    "getting-started",
    "Run the pipeline and send your first metric points.",
  ],
  [
    "Client integration",
    "api",
    "Authentication, safe retries, queries, and live streaming.",
  ],
  [
    "Architecture & guarantees",
    "architecture",
    "Follow a point from acceptance through durable aggregation.",
  ],
  [
    "Configuration",
    "configuration",
    "Every runtime setting, default, and resource bound.",
  ],
  [
    "Security & tenants",
    "security",
    "Provision tenants, rotate keys, and isolate access.",
  ],
  [
    "Operations",
    "operations",
    "Monitor health, diagnose failures, and run the pipeline.",
  ],
  [
    "Recovery runbooks",
    "recovery",
    "Restore data and safely recover interrupted processing.",
  ],
  [
    "GCP deployment",
    "deployment",
    "Prepare a release and capture staging acceptance evidence.",
  ],
  [
    "Terraform reference",
    "terraform-reference",
    "Infrastructure inputs, outputs, and dependencies.",
  ],
  [
    "Capacity qualification",
    "capacity",
    "Measure sustainable throughput for your workload.",
  ],
  [
    "Measured results",
    "capacity-results",
    "Recorded workloads, observations, and their limits.",
  ],
  ["Development", "development", "Build, test, and change Fluxgate."],
  [
    "Security scanning",
    "security-scanning",
    "Understand container vulnerability assessment.",
  ],
].map(([title, file, description]) => ({
  title,
  source: `docs/${file}.md`,
  route: `/guides/${file}/`,
  description,
  group: ["getting-started", "api", "architecture", "dashboard"].includes(file)
    ? "Learn"
    : ["development", "security-scanning"].includes(file)
      ? "Contribute"
      : "Operate",
}));
guides.push(
  {
    title: "All guides",
    source: "docs/README.md",
    route: "/guides/",
    description: "Find the right guide for your task.",
    group: "Learn",
  },
  {
    title: "Project overview",
    source: "README.md",
    route: "/guides/project-overview/",
    description: "The Fluxgate project, capabilities, and quickstart.",
    group: "Learn",
  },
  {
    title: "Terraform procedure",
    source: "deploy/terraform/README.md",
    route: "/guides/terraform-procedure/",
    description: "Provision and upgrade the GCP infrastructure.",
    group: "Operate",
  },
  {
    title: "Documentation site",
    source: "site/README.md",
    route: "/guides/documentation-site/",
    description: "Build, verify, publish, and maintain this documentation.",
    group: "Contribute",
  },
  {
    title: "Design decisions",
    source: "docs/adr/README.md",
    route: "/decisions/",
    description: "The tradeoffs behind the architecture.",
    group: "Design decisions",
  },
);
for (const name of (await readdir(path.join(root, "docs/adr")))
  .filter((name) => /^\d.*\.md$/.test(name))
  .sort()) {
  const text = await readFile(path.join(root, "docs/adr", name), "utf8");
  const title = text.match(/^# (.+)/)?.[1] || name;
  guides.push({
    title,
    source: `docs/adr/${name}`,
    route: `/decisions/${name.replace(".md", "")}/`,
    description: `Architecture decision: ${title}`,
    group: "Design decisions",
  });
}
const routes = new Map(guides.map((page) => [page.source, page.route]));
const operations = Object.entries(spec.paths).flatMap(([endpoint, item]) =>
  Object.entries(item)
    .filter(([method]) =>
      ["get", "post", "put", "delete", "patch"].includes(method),
    )
    .map(([method, operation]) => ({
      ...operation,
      method: method.toUpperCase(),
      endpoint,
      servers: operation.servers || item.servers || spec.servers,
      route: operationRoute(operation.operationId),
    })),
);
const titles = {
  ingestPoints: "Submit metric points",
  queryRollups: "Query rollups",
  listMetrics: "List metrics",
  listLabels: "List labels",
  streamRollups: "Stream rollups",
  liveness: "Liveness",
  readiness: "Readiness",
  version: "Build version",
};
const pages = [];
const search = [];
const logo =
  '<svg viewBox="0 0 32 32" fill="none" aria-hidden="true"><path d="M4 6h24v5H10v5h15v5H10v5H4V6Z" fill="currentColor"/><path d="m23 21 5-5v10h-5v-5Z" fill="currentColor" opacity=".45"/></svg>';
const arrow = '<span aria-hidden="true">↗</span>';

function code(text, lang = "text") {
  const actual =
    { sh: "bash", shell: "bash", js: "javascript", yml: "yaml" }[lang] || lang;
  const known = highlighter.getLoadedLanguages().includes(actual)
    ? actual
    : "text";
  const html = highlighter.codeToHtml(text, {
    lang: known,
    themes: { light: "github-light", dark: "github-dark" },
    defaultColor: false,
  });
  return `<div class="code-block"><div class="code-label"><span>${e(lang)}</span><button type="button" class="copy" aria-label="Copy code">Copy</button></div>${html}</div>`;
}

function markdown(text, source, toc = []) {
  const used = new Map();
  const parser = new Marked({ gfm: true });
  parser.use({
    renderer: {
      image({ href, text }) {
        // The GitHub README's live badge is a link here, so documentation
        // never depends on a third-party image request to render correctly.
        if (
          href.startsWith(`${repository}/actions/`) &&
          href.endsWith("/badge.svg")
        ) {
          return `<span>${e(text)} status on GitHub ↗</span>`;
        }
        return `<img src="${e(rewriteLink(href, source, routes))}" alt="${e(text)}" loading="lazy">`;
      },
      heading({ tokens, depth }) {
        const html = this.parser.parseInline(tokens);
        const title = html
          .replace(/<[^>]*>/g, "")
          .replace(/&amp;/g, "&")
          .replace(/&quot;/g, '"')
          .replace(/&#39;/g, "'");
        const base = slug(title);
        const duplicate = used.get(base) || 0;
        used.set(base, duplicate + 1);
        const id = base + (duplicate ? `-${duplicate}` : "");
        if (depth === 2 || depth === 3) toc.push({ title, id, depth });
        return `<h${depth} id="${e(id)}">${html}<a class="anchor" href="#${e(id)}" aria-label="Link to ${e(title)}">#</a></h${depth}>`;
      },
      link({ href, title, tokens }) {
        return `<a href="${e(rewriteLink(href, source, routes))}"${title ? ` title="${e(title)}"` : ""}>${this.parser.parseInline(tokens)}</a>`;
      },
      code({ text, lang }) {
        if (lang === "mermaid")
          return `<figure class="diagram"><div class="mermaid" aria-label="Architecture diagram">${e(text)}</div><details><summary>Diagram source</summary><pre>${e(text)}</pre></details></figure>`;
        return code(text, lang || "text");
      },
      table(token) {
        return `<div class="table-scroll" role="region" aria-label="Reference table" tabindex="0">${Renderer.prototype.table.call(this, token)}</div>`;
      },
    },
  });
  return parser.parse(text);
}
const md = (text) => markdown(text || "", "api/openapi.yaml");

const responseExamples = {
  queryRollups: {
    metric: "http.requests",
    kind: "counter",
    aggregation: "sum",
    from: "2026-10-02T12:00:00Z",
    to: "2026-10-02T12:15:00Z",
    series: [
      {
        kind: "counter",
        labels: { service: "checkout" },
        points: [{ t: "2026-10-02T12:00:00Z", v: 42 }],
      },
    ],
    truncated: false,
  },
  listMetrics: {
    metrics: [
      {
        metric: "http.requests",
        kind: "counter",
        series_count: 1,
        oldest_window: "2026-10-02T12:00:00Z",
        newest_window: "2026-10-02T12:14:00Z",
      },
    ],
  },
  listLabels: { metric: "http.requests", labels: ["service"] },
  liveness: { status: "ok" },
  readiness: { status: "ok", checks: { postgres: "ok" } },
  version: {
    version: "v1.0.0",
    commit: "<commit-sha>",
    build_date: "<build-timestamp>",
    go_version: "go1.27.0",
    platform: "linux/amd64",
  },
};

function schema(node, depth = 0) {
  if (!node) return "";
  const resolved = resolveRef(spec, node);
  const refName = node.$ref?.split("/").at(-1);
  const constraints = schemaConstraints(resolved)
    .map(
      ([label, value]) =>
        `<span><b>${e(label)}</b> <code>${e(value)}</code></span>`,
    )
    .join("");
  const enums = resolved.enum
    ? `<div class="enum">${resolved.enum.map((value) => `<code>${e(JSON.stringify(value))}</code>`).join(" ")}</div>`
    : "";
  const properties = Object.entries(resolved.properties || {})
    .map(
      ([name, child]) =>
        `<div class="property"><div class="property-heading"><code>${e(name)}</code><span class="type">${e(schemaType(child))}</span>${(resolved.required || []).includes(name) ? '<span class="required">required</span>' : '<span class="optional">optional</span>'}</div>${schema(child, depth + 1)}</div>`,
    )
    .join("");
  const nested =
    depth > 0 && properties
      ? `<details class="nested"><summary>Object properties</summary>${properties}</details>`
      : properties;
  const items = resolved.items
    ? `<details class="nested"${depth === 0 ? " open" : ""}><summary>Array items · ${e(schemaType(resolved.items))}</summary>${schema(resolved.items, depth + 1)}</details>`
    : "";
  const additional =
    typeof resolved.additionalProperties === "object"
      ? `<details class="nested"><summary>Additional values · ${e(schemaType(resolved.additionalProperties))}</summary>${schema(resolved.additionalProperties, depth + 1)}</details>`
      : resolved.additionalProperties === false
        ? '<p class="schema-note">Unknown properties are rejected.</p>'
        : "";
  const keys = resolved.propertyNames
    ? `<details class="nested"><summary>Property name constraints</summary>${schema(resolved.propertyNames, depth + 1)}</details>`
    : "";
  const composed = ["oneOf", "anyOf", "allOf"]
    .map((key) =>
      resolved[key]
        ? `<p>${e(key)}</p>${resolved[key].map((child) => schema(child, depth + 1)).join("")}`
        : "",
    )
    .join("");
  return `${refName ? `<a class="schema-link" href="/api-reference/schemas/#${slug(refName)}">${e(refName)} ↗</a>` : ""}${md(resolved.description)}${constraints ? `<div class="constraints">${constraints}</div>` : ""}${enums}${nested}${items}${additional}${keys}${composed}`;
}

function samples(operation) {
  const auth = (operation.security || spec.security).length > 0;
  const body =
    operation.operationId === "ingestPoints"
      ? {
          points: [
            {
              metric: "http.requests",
              kind: "counter",
              value: 1,
              labels: { service: "checkout" },
            },
          ],
        }
      : null;
  const queries = {
    queryRollups: "?metric=http.requests&from=-15m&agg=sum",
    listMetrics: "?limit=100",
    listLabels: "?metric=http.requests",
    streamRollups: "?metric=http.requests",
  };
  const url =
    operation.servers[0].url +
    operation.endpoint +
    (queries[operation.operationId] || "");
  const headers = {
    ...(auth ? { Authorization: "Bearer <YOUR_API_KEY>" } : {}),
    ...(body
      ? {
          "Content-Type": "application/json",
          "Idempotency-Key": "<UNIQUE_BATCH_KEY>",
        }
      : {}),
  };
  const streaming = operation.operationId === "streamRollups";
  const curl = [
    `curl${streaming ? " --no-buffer" : ""} --request ${operation.method} '${url}'`,
    ...Object.entries(headers).map(
      ([key, value]) => `  --header '${key}: ${value}'`,
    ),
    ...(body ? [`  --data '${JSON.stringify(body)}'`] : []),
  ].join(" \\\n");
  const jsHeaders = JSON.stringify(headers, null, 2)
    .split("\n")
    .map((line, i) => (i ? `  ${line}` : line))
    .join("\n");
  const js = `const response = await fetch(${JSON.stringify(url)}, {\n  method: ${JSON.stringify(operation.method)},\n  headers: ${jsHeaders}${body ? `,\n  body: JSON.stringify(${JSON.stringify(body)})` : ""}\n});\nif (!response.ok) throw new Error(await response.text());\n${streaming ? "// These are text chunks; buffer and parse SSE frames before use.\n// Reconnect with backoff and reconcile gaps through /v1/query.\nfor await (const chunk of response.body.pipeThrough(new TextDecoderStream())) {\n  console.log(chunk);\n}" : `const result = await response.json();\n${body ? "// A 202 may contain rejected points. Inspect result.rejected and result.errors.\n" : ""}console.log(result);`}`;
  const pyHeaders = JSON.stringify(headers, null, 4);
  const python = `import json\nfrom urllib.request import Request, urlopen\n\nrequest = Request(\n    ${JSON.stringify(url)},\n    method=${JSON.stringify(operation.method)},\n    headers=${pyHeaders.replace(/\n/g, "\n    ")}${body ? `,\n    data=json.dumps(${JSON.stringify(body)}).encode("utf-8")` : ""}\n)\nwith urlopen(request, timeout=${streaming ? "60" : "30"}) as response:\n${streaming ? '    # Parse SSE frames and reconcile through /v1/query after reconnects.\n    for line in response:\n        print(line.decode("utf-8").rstrip())' : "    result = json.load(response)\n    print(result)"}`;
  return `<div class="samples"><div class="tabs" role="tablist" aria-label="Request language">${["cURL", "JavaScript", "Python"].map((label, i) => `<button type="button" role="tab" id="sample-tab-${i}" aria-controls="sample-panel-${i}" aria-selected="${i === 0}" tabindex="${i === 0 ? "0" : "-1"}">${label}</button>`).join("")}</div>${[
    [curl, "bash"],
    [js, "javascript"],
    [python, "python"],
  ]
    .map(
      ([text, language], i) =>
        `<div role="tabpanel" id="sample-panel-${i}" aria-labelledby="sample-tab-${i}"${i ? " hidden" : ""}>${code(text, language)}</div>`,
    )
    .join("")}</div>`;
}

function operationContent(operation) {
  const auth = (operation.security || spec.security).length > 0;
  const parameters = (operation.parameters || []).map((item) =>
    resolveRef(spec, item),
  );
  let html = `<div class="endpoint-banner"><span class="method ${operation.method.toLowerCase()}">${operation.method}</span><code>${e(operation.endpoint)}</code></div><div class="endpoint-facts"><span>${auth ? "Bearer API key" : "No authentication"}</span><span>${operation.tags[0] === "Operations" ? "All services" : `${e(operation.tags[0])} service`}</span><span>API ${e(spec.info.version)}</span></div>${md(operation.description || operation.summary)}`;
  html +=
    '<h2 id="base-url">Base URL</h2><p>These addresses are for local development. Replace them with your own deployed service URLs.</p>' +
    operation.servers
      .map(
        (server) =>
          `<p><code>${e(server.url)}</code> <span class="muted">${e(server.description)}</span></p>`,
      )
      .join("");
  html += `<h2 id="request-example">Request example</h2><p>Replace credential and batch-key placeholders before running. JavaScript examples use Node.js 24; Python examples use the standard library. Browser clients need a same-origin proxy or suitable CORS configuration. See <a href="/guides/api/#safe-retries">safe retries</a> for production retry handling.</p>${samples(operation)}`;
  if (auth)
    html += `<h2 id="authentication">Authentication</h2>${md(spec.components.securitySchemes.bearerAuth.description)}<p>Credentials select the tenant. See <a href="/guides/security/">tenant provisioning and key rotation</a>.</p>`;
  if (parameters.length)
    html += `<h2 id="parameters">Parameters</h2>${parameters.map((parameter) => `<div class="property"><div class="property-heading"><code>${e(parameter.name)}</code><span class="type">${e(parameter.in)} · ${e(schemaType(parameter.schema || {}))}</span><span class="${parameter.required ? "required" : "optional"}">${parameter.required ? "required" : "optional"}</span></div>${md(parameter.description)}${schema(parameter.schema)}${parameter.example !== undefined ? `<p>Example: <code>${e(String(parameter.example))}</code></p>` : ""}</div>`).join("")}`;
  if (operation.requestBody) {
    const request = resolveRef(spec, operation.requestBody);
    html += `<h2 id="request-body">Request body</h2><p>${request.required ? "Required." : "Optional."} ${e(Object.keys(request.content).join(", "))}</p>`;
    for (const content of Object.values(request.content)) {
      html += schema(content.schema);
      for (const example of Object.values(content.examples || {}))
        html += `<h3>${e(example.summary || "Example")}</h3><p>Illustrative payload. If sending a timestamp, update it to a value within your configured acceptance window.</p>${code(JSON.stringify(example.value, null, 2), "json")}`;
    }
  }
  html +=
    '<h2 id="responses">Responses</h2><p>Expand a response to inspect its headers and complete schema.</p>';
  for (const [status, item] of Object.entries(operation.responses)) {
    const response = resolveRef(spec, item);
    html += `<details class="response"${status.startsWith("2") ? " open" : ""}><summary><span class="status ${status.startsWith("2") ? "success" : ""}">${e(status)}</span><span>${e(response.description?.split("\n")[0] || "Response")}</span></summary><div class="response-content">${md(response.description)}`;
    if (response.headers)
      html += `<h3>Response headers</h3>${Object.entries(response.headers)
        .map(([name, header]) => {
          const resolved = resolveRef(spec, header);
          return `<div class="property"><div class="property-heading"><code>${e(name)}</code><span class="type">${e(schemaType(resolved.schema || {}))}</span></div>${md(resolved.description)}${schema(resolved.schema)}</div>`;
        })
        .join("")}`;
    for (const [mime, content] of Object.entries(response.content || {})) {
      html += `<h3>${e(mime)}</h3>${schema(content.schema)}`;
      if (content.example !== undefined)
        html += code(
          typeof content.example === "string"
            ? content.example
            : JSON.stringify(content.example, null, 2),
          mime === "text/event-stream" ? "text" : "json",
        );
      for (const example of Object.values(content.examples || {}))
        html += `<h4>${e(example.summary || "Example")}</h4>${code(JSON.stringify(example.value, null, 2), "json")}`;
      if (status === "200" && responseExamples[operation.operationId])
        html += `<h4>Illustrative response</h4><p>Values are examples; they are not a response from a hosted Fluxgate service.</p>${code(JSON.stringify(responseExamples[operation.operationId], null, 2), "json")}`;
    }
    html += "</div></details>";
  }
  return html;
}

const operationList = operations
  .map(
    (operation) =>
      `<a class="endpoint-row" href="${operation.route}"><span class="method ${operation.method.toLowerCase()}">${operation.method}</span><code>${e(operation.endpoint)}</code><span>${e(titles[operation.operationId])}</span><span aria-hidden="true">→</span></a>`,
  )
  .join("");
const cards = [
  [
    "01",
    "Send your first points",
    "Start the local pipeline, authenticate, and verify a query.",
    "/guides/getting-started/",
  ],
  [
    "02",
    "Integrate with confidence",
    "Understand partial success, retry identity, and live updates.",
    "/guides/api/",
  ],
  [
    "03",
    "Operate at scale",
    "Set bounds, measure capacity, and prepare a GCP release.",
    "/guides/deployment/",
  ],
];
pages.push({
  route: "/",
  title: "Documentation",
  group: "Fluxgate",
  description:
    "The complete guide to ingesting, aggregating, and querying telemetry with Fluxgate.",
  source: null,
  hero: true,
  body: `<div class="hero"><div class="eyebrow"><span class="dot"></span> FLUXGATE DOCUMENTATION <span class="version">v${e(spec.info.version)}</span></div><h1>Telemetry in.<br><span>Answers out.</span></h1><p>Build on a telemetry pipeline designed for durable ingestion, tenant isolation, and fast access to aggregated metrics.</p><div class="hero-actions"><a class="button primary" href="/guides/getting-started/">Start building <span aria-hidden="true">→</span></a><a class="button secondary" href="/dashboard/">Open dashboard ↗</a></div><div class="pipeline" aria-label="Pipeline: HTTP to Pub/Sub to PostgreSQL to REST and SSE"><span>HTTP</span><i>→</i><span>Pub/Sub</span><i>→</i><span>PostgreSQL</span><i>→</i><span>REST + SSE</span></div></div><section><div class="section-caption">YOUR NEXT STEP</div><div class="start-cards">${cards.map(([number, title, description, url]) => `<a class="start-card" href="${url}"><span class="card-number">${number}</span><h2>${title} <span aria-hidden="true">↗</span></h2><p>${description}</p></a>`).join("")}</div></section><section><div class="section-heading"><div><div class="section-caption">THE CONTRACT</div><h2 id="api">One API. Eight endpoints.</h2></div><a href="/openapi.yaml" download>OpenAPI 3.1 ↓</a></div><p>Separate write and read services. A shared, explicit contract.</p><div class="endpoint-list">${operationList}</div></section><section class="principles"><div><h2>Know what a response means.</h2><p>A successful ingest confirms publication. Queries return committed window totals. Live events replace totals; they don’t add to them.</p><a href="/guides/architecture/">Read the data guarantees →</a></div><div class="callout"><strong>Evidence before production.</strong><p>Explore measured emulator results and use the deployment acceptance record to qualify your own GCP workload.</p><a href="/guides/capacity-results/">View measured results →</a></div></section>`,
});
pages.push({
  route: "/api-reference/",
  title: "API reference",
  group: "API reference",
  description:
    "All Fluxgate endpoints, request parameters, response schemas, and examples.",
  source: "api/openapi.yaml",
  body: `<p class="lead">A small API with an explicit contract. Submit metric points, read windowed aggregates, and follow live changes.</p><div class="callout"><strong>Bring your own service URLs.</strong><p>This site hosts the documentation. The examples target local Fluxgate services; deploy the pipeline to obtain your production API addresses.</p></div><h2 id="endpoints">Endpoints</h2><div class="endpoint-list">${operationList}</div><h2 id="authentication">Authentication</h2>${md(spec.components.securitySchemes.bearerAuth.description)}<p>All data endpoints require a tenant key. Health, readiness, and version endpoints are unauthenticated. <a href="/guides/security/">Manage tenant access →</a></p><h2 id="client-contract">Client contract</h2><div class="link-grid"><a href="/guides/api/#safe-retries">Retry safely →</a><a href="/guides/api/">Handle partial success →</a><a href="/api-reference/schemas/#problem">Understand errors →</a><a href="/api-reference/stream-rollups/">Consume live updates →</a></div><h2 id="openapi">OpenAPI downloads</h2><p>Generated from the same versioned contract as these pages. Import it into your API tooling or generate a client.</p><div class="hero-actions"><a class="button secondary" href="/openapi.yaml" download>Download YAML ↓</a><a class="button secondary" href="/openapi.json" download>Download JSON ↓</a></div>`,
});
for (const operation of operations)
  pages.push({
    route: operation.route,
    title: titles[operation.operationId] || operation.summary,
    group: operation.tags[0],
    description: operation.summary,
    source: "api/openapi.yaml",
    body: operationContent(operation),
    operationId: operation.operationId,
  });
pages.push({
  route: "/api-reference/schemas/",
  title: "Data models",
  group: "API reference",
  description:
    "Every request and response model in the Fluxgate OpenAPI contract.",
  source: "api/openapi.yaml",
  body: `<p class="lead">The complete schema reference, generated from OpenAPI ${e(spec.openapi)}.</p>${Object.entries(
    spec.components.schemas,
  )
    .map(
      ([name, value]) =>
        `<h2 id="${slug(name)}">${e(name)}</h2><p class="type">${e(schemaType(value))}</p>${schema(value)}`,
    )
    .join("")}`,
});
for (const guide of guides) {
  const text = await readFile(path.join(root, guide.source), "utf8");
  const toc = [];
  // Preserve the original h1 fragment for links from other repository guides.
  const originalTitle = text.match(/^# (.+)\r?$/m)?.[1]?.trim();
  const body = markdown(text.replace(/^# .+\r?\n/, ""), guide.source, toc);
  pages.push({
    ...guide,
    body,
    toc,
    headingId: slug(originalTitle || guide.title),
  });
}

function sidebar(current) {
  const item = (url, title, extra = "") =>
    `<a href="${url}"${url === current ? ' aria-current="page"' : ""}>${extra}<span>${e(title)}</span></a>`;
  let html = item("/", "Overview");
  for (const group of [
    "Learn",
    "API reference",
    "Operate",
    "Contribute",
    "Design decisions",
  ]) {
    html += `<div class="nav-group"><div class="nav-label">${group}</div>`;
    if (group === "API reference")
      html +=
        item("/api-reference/", "Introduction") +
        operations
          .map((operation) =>
            item(
              operation.route,
              titles[operation.operationId],
              `<span class="nav-method ${operation.method.toLowerCase()}">${operation.method}</span>`,
            ),
          )
          .join("") +
        item("/api-reference/schemas/", "Data models");
    else
      html += guides
        .filter((page) => page.group === group)
        .map((page) =>
          item(page.route, page.title.replace(/^\d+\.?\s*[:—-]?\s*/, "")),
        )
        .join("");
    html += "</div>";
  }
  return html;
}

await build({
  entryPoints: {
    client: path.join(site, "src/client.js"),
    diagrams: path.join(site, "src/diagrams.js"),
  },
  outdir: path.join(out, "assets"),
  bundle: true,
  splitting: true,
  format: "esm",
  minify: true,
  target: ["es2022"],
  entryNames: "[name]-[hash]",
  chunkNames: "chunk-[hash]",
});
const assets = await readdir(path.join(out, "assets"));
const clientFile = assets.find((name) => name.startsWith("client-"));
const diagramFile = assets.find((name) => name.startsWith("diagrams-"));
let styles = await readFile(path.join(site, "src/styles.css"), "utf8");
const fonts = {};
for (const weight of [400, 500, 600, 700]) {
  const bytes = await readFile(
    path.join(
      site,
      `node_modules/@fontsource/inter/files/inter-latin-${weight}-normal.woff2`,
    ),
  );
  fonts[weight] =
    `inter-${weight}-${createHash("sha256").update(bytes).digest("hex").slice(0, 12)}.woff2`;
  await writeFile(path.join(out, "assets", fonts[weight]), bytes);
  styles = styles.replace(
    `/assets/inter-${weight}.woff2`,
    `/assets/${fonts[weight]}`,
  );
}
const styleFile = `styles-${createHash("sha256").update(styles).digest("hex").slice(0, 12)}.css`;
await writeFile(path.join(out, "assets", styleFile), styles);
await cp(path.join(site, "public"), out, { recursive: true });
await cp(
  path.join(root, "internal/dashboard/web"),
  path.join(out, "dashboard"),
  { recursive: true },
);
await cp(
  path.join(site, "node_modules/@fontsource/inter/LICENSE"),
  path.join(out, "inter-license.txt"),
);
const themeScript =
  "try{const t=localStorage.getItem('theme');document.documentElement.dataset.theme=t||(matchMedia('(prefers-color-scheme:dark)').matches?'dark':'light')}catch{}";
const themeHash = createHash("sha256").update(themeScript).digest("base64");

function layout(page) {
  const toc =
    page.toc ||
    [...page.body.matchAll(/<h([23]) id="([^"]+)">([^<]+)(?:<|$)/g)].map(
      (match) => ({ depth: Number(match[1]), id: match[2], title: match[3] }),
    );
  const index = pages.indexOf(page);
  const neighbor = (other, label) =>
    other
      ? `<a href="${other.route}"><span>${label}</span><strong>${e(other.title)} ${label === "Next" ? "→" : ""}</strong></a>`
      : "<span></span>";
  const canonical = origin + page.route;
  return `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${e(page.title)} · Fluxgate Docs</title><meta name="description" content="${e(page.description)}"><meta name="theme-color" content="#6b49db"><link rel="canonical" href="${canonical}"><meta property="og:type" content="website"><meta property="og:site_name" content="Fluxgate Documentation"><meta property="og:title" content="${e(page.title)} · Fluxgate"><meta property="og:description" content="${e(page.description)}"><meta property="og:url" content="${canonical}"><meta name="twitter:card" content="summary"><link rel="icon" type="image/svg+xml" href="/favicon.svg"><link rel="stylesheet" href="/assets/${styleFile}"><link rel="preload" href="/assets/${fonts[400]}" as="font" type="font/woff2" crossorigin><script>${themeScript}</script><script type="module" src="/assets/${clientFile}"></script>${page.body.includes('class="mermaid"') ? `<script type="module" src="/assets/${diagramFile}"></script>` : ""}</head><body><a class="skip-link" href="#main">Skip to content</a><header class="header"><a class="brand" href="/" aria-label="Fluxgate documentation home">${logo}<span>fluxgate</span><span class="brand-label">docs</span></a><button class="search-trigger" type="button" aria-haspopup="dialog"><svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/></svg><span>Search documentation</span><kbd>Ctrl K</kbd></button><div class="header-actions"><a class="github-link" href="/dashboard/">Dashboard ↗</a><a class="github-link" href="${repository}" aria-label="Fluxgate on GitHub">GitHub ${arrow}</a><button class="theme-toggle icon-button" type="button" aria-label="Switch color theme"><svg viewBox="0 0 24 24" fill="none" aria-hidden="true"><path d="M20 14a8 8 0 0 1-10-10A8 8 0 1 0 20 14Z"/></svg></button><button class="menu-toggle icon-button" type="button" aria-expanded="false" aria-controls="sidebar" aria-label="Open navigation"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 6h16M4 12h16M4 18h16"/></svg></button></div></header><button class="nav-shade" aria-label="Close navigation" hidden></button><aside id="sidebar" class="sidebar"><nav aria-label="Documentation">${sidebar(page.route)}</nav><div class="sidebar-bottom"><span class="dot"></span> OpenAPI ${e(spec.openapi)} <span>v${e(spec.info.version)}</span></div></aside><div class="workspace ${page.hero ? "home" : ""}"><main id="main" tabindex="-1"><noscript><p><a href="/guides/">All guides</a> · <a href="/api-reference/">API reference</a></p></noscript><div class="breadcrumb"><a href="/">Docs</a><span>/</span><span>${e(page.group)}</span></div>${page.hero ? "" : `<h1 id="${e(page.headingId || slug(page.title))}">${e(page.title)}</h1>`}<article class="prose">${page.body}</article>${page.hero ? "" : `<div class="page-source"><span>Something to improve?</span> <a href="${repository}/edit/main/${page.source}">Edit this page ${arrow}</a><a href="${repository}/blob/main/${page.source}">View source ${arrow}</a></div><nav class="page-neighbors" aria-label="Adjacent pages">${neighbor(pages[index - 1], "Previous")}${neighbor(pages[index + 1], "Next")}</nav>`}<footer><a href="${repository}">Fluxgate</a><span>Open source · MIT license</span><a href="/openapi.yaml">OpenAPI</a><a href="/guides/deployment/#release-acceptance-record">Production readiness</a></footer></main>${
    page.hero
      ? ""
      : `<aside class="toc"><nav aria-label="On this page"><div class="nav-label">On this page</div>${toc
          .filter((item) => item.depth === 2)
          .map((item) => `<a href="#${item.id}">${e(item.title)}</a>`)
          .join(
            "",
          )}</nav><a class="toc-download" href="/openapi.yaml" download>Download OpenAPI ↓</a></aside>`
  }</div><dialog class="search-dialog" aria-label="Search documentation"><div class="search-top"><label class="sr-only" for="search-input">Search documentation</label><input id="search-input" type="search" placeholder="Search endpoints, guides, settings…" autocomplete="off"><button type="button" class="close-search" aria-label="Close search">Esc</button></div><p class="search-status" role="status">Search the complete documentation.</p><div class="search-results"></div><div class="search-footer"><span>↑ ↓ to navigate · Enter to open</span><span>Search stays on your device</span></div></dialog><div class="toast" role="status" aria-live="polite"></div></body></html>`;
}

for (const page of pages) {
  const target = path.join(out, page.route, "index.html");
  await mkdir(path.dirname(target), { recursive: true });
  await writeFile(target, layout(page));
  const plain = (value) =>
    value
      .replace(/<[^>]+>/g, " ")
      .replace(
        /&(?:amp|lt|gt|quot|#39);/g,
        (value) =>
          ({
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#39;": "'",
          })[value],
      )
      .replace(/\s+/g, " ")
      .trim();
  search.push({
    title: page.title,
    group: page.group,
    url: page.route,
    text: plain(page.body),
  });
  for (const match of page.body.matchAll(
    /<h2 id="([^"]+)">([\s\S]*?)<\/h2>([\s\S]*?)(?=<h2 |$)/g,
  ))
    search.push({
      title: `${page.title} › ${plain(match[2]).replace(/#$/, "")}`,
      group: page.group,
      url: `${page.route}#${match[1]}`,
      text: plain(match[3]),
    });
}
const notFound = {
  route: "/404/",
  title: "Page not found",
  group: "404",
  description: "Find your way back to the Fluxgate documentation.",
  hero: true,
  body: '<div class="hero"><div class="eyebrow">404 · PAGE NOT FOUND</div><h1>Let’s get you<br>back on track.</h1><p>This address doesn’t match a documentation page. Search the docs or return to the overview.</p><a class="button primary" href="/">Back to documentation →</a></div>',
};
await writeFile(path.join(out, "404.html"), layout(notFound));
await writeFile(path.join(out, "search-index.json"), JSON.stringify(search));
await writeFile(path.join(out, "openapi.yaml"), specText);
await writeFile(path.join(out, "openapi.json"), JSON.stringify(spec, null, 2));
await writeFile(
  path.join(out, "robots.txt"),
  `User-agent: *\nAllow: /\nSitemap: ${origin}/sitemap.xml\n`,
);
await writeFile(
  path.join(out, "sitemap.xml"),
  `<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">${pages.map((page) => `<url><loc>${e(origin + page.route)}</loc></url>`).join("")}</urlset>`,
);
await writeFile(
  path.join(site, ".build-manifest.json"),
  JSON.stringify(
    {
      pages: pages.map(({ route, source, operationId }) => ({
        route,
        source,
        operationId,
      })),
      schemas: Object.keys(spec.components.schemas),
      themeHash,
    },
    null,
    2,
  ),
);
highlighter.dispose();
console.log(
  `Built ${pages.length} pages, ${operations.length} endpoints, ${Object.keys(spec.components.schemas).length} models, and ${search.length} searchable sections.`,
);
