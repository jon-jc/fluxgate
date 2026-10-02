import path from "node:path";

export const repository = "https://github.com/jon-jc/fluxgate";
export const escape = (value = "") =>
  String(value).replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
export const slug = (value) =>
  value
    .toLowerCase()
    .replace(/<[^>]*>/g, "")
    .replace(/[^\p{L}\p{N}_\-\s]/gu, "")
    .trim()
    .replace(/\s/g, "-");
export const operationRoute = (id) =>
  `/api-reference/${id.replace(/([a-z])([A-Z])/g, "$1-$2").toLowerCase()}/`;

export function resolveRef(spec, value) {
  if (!value?.$ref) return value;
  if (!value.$ref.startsWith("#/"))
    throw new Error(`External OpenAPI reference: ${value.$ref}`);
  const target = value.$ref
    .slice(2)
    .split("/")
    .reduce(
      (node, key) => node?.[key.replace(/~1/g, "/").replace(/~0/g, "~")],
      spec,
    );
  if (!target) throw new Error(`Unresolved OpenAPI reference: ${value.$ref}`);
  const { $ref, ...siblings } = value;
  return { ...resolveRef(spec, target), ...siblings };
}

// Markdown remains useful on GitHub; the published site maps documentation
// links to local pages and implementation links to their repository source.
export function rewriteLink(href, source, routes) {
  if (/^(https?:|mailto:|#)/i.test(href)) return href;
  if (/^[a-z][a-z\d+.-]*:/i.test(href) || href.startsWith("//"))
    throw new Error(`Unsupported link: ${href}`);
  const [pathname, fragment] = href.split("#");
  const normalized = path.posix.normalize(
    path.posix.join(path.posix.dirname(source), decodeURIComponent(pathname)),
  );
  const suffix = fragment ? `#${fragment}` : "";
  if (normalized === "api/openapi.yaml") return `/openapi.yaml${suffix}`;
  if (routes.has(normalized)) return routes.get(normalized) + suffix;
  if (normalized.startsWith("../"))
    throw new Error(`Link escapes the repository: ${source}: ${href}`);
  return `${repository}/blob/main/${normalized}${suffix}`;
}

export function schemaType(schema) {
  if (schema.$ref) return schema.$ref.split("/").at(-1);
  if (schema.items) return `${schemaType(schema.items)}[]`;
  if (schema.oneOf || schema.anyOf)
    return (schema.oneOf || schema.anyOf).map(schemaType).join(" | ");
  return Array.isArray(schema.type)
    ? schema.type.join(" | ")
    : schema.type || (schema.properties ? "object" : "any");
}

export function schemaConstraints(schema) {
  const labels = {
    format: "Format",
    minimum: "Minimum",
    maximum: "Maximum",
    minLength: "Min length",
    maxLength: "Max length",
    minItems: "Min items",
    maxItems: "Max items",
    maxProperties: "Max properties",
    pattern: "Pattern",
    default: "Default",
  };
  return Object.entries(labels)
    .filter(([key]) => schema[key] !== undefined)
    .map(([key, label]) => [label, String(schema[key])]);
}
