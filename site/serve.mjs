import http from "node:http";
import path from "node:path";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "dist");
const mime = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css",
  ".js": "text/javascript",
  ".json": "application/json",
  ".yaml": "application/yaml",
  ".svg": "image/svg+xml",
  ".woff2": "font/woff2",
  ".txt": "text/plain",
  ".xml": "application/xml",
};
const config = JSON.parse(
  await readFile(path.join(root, "../../vercel.json"), "utf8"),
);
const headers = Object.fromEntries(
  config.headers
    .find((rule) => rule.source === "/(.*)")
    .headers.map(({ key, value }) => [key, value]),
);
const server = http.createServer(async (request, response) => {
  try {
    const pathname = decodeURIComponent(
      new URL(request.url, "http://localhost").pathname,
    );
    const file = path.resolve(
      root,
      `.${pathname}`,
      pathname.endsWith("/") ? "index.html" : "",
    );
    if (!file.startsWith(root + path.sep)) {
      response.writeHead(400).end("Invalid path");
      return;
    }
    if (!["GET", "HEAD"].includes(request.method)) {
      response.writeHead(405, { Allow: "GET, HEAD" }).end();
      return;
    }
    try {
      const data = await readFile(file);
      response.writeHead(200, {
        ...headers,
        "Content-Type": mime[path.extname(file)] || "application/octet-stream",
      });
      response.end(request.method === "HEAD" ? undefined : data);
    } catch {
      response.writeHead(404, { ...headers, "Content-Type": mime[".html"] });
      response.end(
        request.method === "HEAD"
          ? undefined
          : await readFile(path.join(root, "404.html")),
      );
    }
  } catch {
    response.writeHead(400).end("Bad request");
  }
});
server.listen(
  Number(process.env.PORT || 4173),
  process.env.HOST || "127.0.0.1",
  () =>
    console.log(
      `Documentation preview: http://${process.env.HOST || "127.0.0.1"}:${process.env.PORT || 4173}`,
    ),
);
