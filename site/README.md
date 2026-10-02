# Documentation website

The Fluxgate documentation site publishes every guide in `docs/`, the project
overview, the Terraform procedure, and a complete API reference generated from
`api/openapi.yaml`. It is a static site with no runtime secrets or API proxy.

## Local development

Use Node.js 24. From the repository root:

```sh
npm --prefix site ci
npm --prefix site run dev
```

Open `http://localhost:4173`. This builds once and serves the result; after an
edit, rerun `npm --prefix site run build` and refresh the browser. The server
binds to loopback by default. Set `HOST` and `PORT` to change its listening address.

## Content and navigation

- Edit the existing Markdown guides. The build remaps their relative links to
  site pages, preserves heading fragments, and links code references to GitHub.
- Edit `api/openapi.yaml` for endpoint descriptions, parameters, schemas, examples,
  response codes, and headers. The site generates an endpoint page for every
  operation, ten model sections, and YAML/JSON contract downloads.
- Add new guides to the `guides` list in `site/build.mjs`. Architecture decision
  records are discovered automatically. A build check fails if a Markdown guide
  under `docs/` is missing from the site.
- Keep request examples in `samples()` current when adding operations. Examples
  target local services, use placeholder credentials, and do not send requests
  from the documentation website. JavaScript examples target Node.js 24; a browser
  integration also needs an appropriate same-origin proxy or CORS configuration.
- Build-only syntax highlighting avoids shipping the highlighter to readers.
  Mermaid is loaded only on pages containing diagrams; diagram source remains
  available. Search loads its index on demand and runs entirely in the browser.

The site supports keyboard search (Ctrl/Cmd K), mobile navigation, light/dark
themes, copyable code, language tabs, schema expansion, heading links, and a
custom 404. Reading guides and following links work without JavaScript.

## Verification

```sh
npm --prefix site test
cd site
npx playwright install chromium
npm run test:browser
```

The build checks local links and fragments, guide/operation/schema coverage,
duplicate IDs, titles, metadata, and exact OpenAPI downloads. Browser checks
exercise search, navigation, examples, copy controls, mobile layout, diagrams,
accessibility, failure handling, and the 404 response. CI runs both sets.

Tests use the local preview by default. Set `BASE_URL` to test an existing public
deployment without starting the local server. Screenshots and reports are local
build artifacts excluded from Git.

## Vercel deployment

The Vercel project uses the **repository root** as its root directory so the
build can read the canonical guides and OpenAPI file. Root `vercel.json` defines
the install/build commands, `site/dist` output directory, redirects, caching,
and security headers. No backend services are deployed by this project.

The GitHub repository is connected to the `fluxgate-docs` Vercel project.
Pull requests receive preview deployments; merges to `main` deploy production.
Configure the Vercel project to use Node.js 24. Keep deployment protection
enabled for previews and use the authenticated Vercel CLI to inspect them.

To deploy manually from the repository root:

```sh
npx vercel@62.2.0 login
npx vercel@62.2.0 link --project fluxgate-docs
npx vercel@62.2.0 deploy
```

Verify the preview, then deploy production from the reviewed main revision:

```sh
npx vercel@62.2.0 deploy --prod
```

Do not commit `.vercel/`, `.env.local`, tokens, or build output. `.vercelignore`
allows only site inputs into uploads. The public site contains no credentials;
the local Compose key in the guides is an intentional development fixture.

`SITE_URL` optionally sets the public HTTPS origin used for canonical links,
the sitemap, and robots file. It defaults to `https://fluxgate-docs.vercel.app`.
Set it before building if attaching a custom domain. The Content Security Policy
hash allows only the exact inline theme initialization script; changing that
script requires updating the matching hash in `vercel.json` (the build verifies
it). Syntax highlighting and Mermaid require inline styles, but no inline event
handlers or arbitrary inline scripts are permitted.

## Updating and recovery

Review documentation and API changes together. Run verification, inspect the
Vercel preview on desktop and mobile, and merge after CI passes. For a bad
documentation release, use the Vercel dashboard's instant rollback to the
previous ready production deployment, then fix the source in a pull request.
This affects only documentation hosting, not Fluxgate data or GCP services.

The [deployment guide](../docs/deployment.md) describes deployment of the
Fluxgate pipeline itself; that is a separate process with its own acceptance
requirements.
