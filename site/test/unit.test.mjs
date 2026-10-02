import { test } from "node:test";
import assert from "node:assert/strict";
import {
  rewriteLink,
  resolveRef,
  schemaType,
  slug,
  escape,
  operationRoute,
} from "../lib.mjs";

test("documentation links preserve anchors and implementation links point to source", () => {
  const routes = new Map([
    ["docs/api.md", "/guides/api/"],
    ["docs/README.md", "/guides/"],
  ]);
  assert.equal(
    rewriteLink("../api.md#safe-retries", "docs/adr/README.md", routes),
    "/guides/api/#safe-retries",
  );
  assert.equal(
    rewriteLink("../api/openapi.yaml", "docs/README.md", routes),
    "/openapi.yaml",
  );
  assert.equal(
    rewriteLink("../internal/auth/auth.go#L10", "docs/api.md", routes),
    "https://github.com/jon-jc/fluxgate/blob/main/internal/auth/auth.go#L10",
  );
  assert.equal(rewriteLink("#example", "docs/api.md", routes), "#example");
  assert.throws(() =>
    rewriteLink("javascript:alert(1)", "docs/api.md", routes),
  );
  assert.throws(() => rewriteLink("../../../outside", "docs/api.md", routes));
});
test("OpenAPI 3.1 reference siblings override shared response descriptions", () => {
  const spec = {
    components: {
      responses: { Problem: { description: "Generic", content: { json: {} } } },
    },
  };
  const response = resolveRef(spec, {
    $ref: "#/components/responses/Problem",
    description: "Retry with the same identity",
  });
  assert.equal(response.description, "Retry with the same identity");
  assert.deepEqual(response.content, { json: {} });
  assert.throws(() => resolveRef(spec, { $ref: "#/missing" }));
});
test("schema display retains arrays, model references, and nullability", () => {
  assert.equal(schemaType({ type: ["object", "null"] }), "object | null");
  assert.equal(
    schemaType({
      type: "array",
      items: { $ref: "#/components/schemas/Point" },
    }),
    "Point[]",
  );
  assert.equal(slug("Safe retries & identity"), "safe-retries--identity");
  assert.equal(operationRoute("queryRollups"), "/api-reference/query-rollups/");
  assert.equal(
    escape('<script>"x"</script>'),
    "&lt;script&gt;&quot;x&quot;&lt;/script&gt;",
  );
});
