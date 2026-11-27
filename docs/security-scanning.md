# Release security scans

Every release candidate runs both Go reachable-code analysis and a packaged-image
inventory scan. Image scanning blocks MEDIUM, HIGH and CRITICAL findings without
an ignore list. JSON reports and CycloneDX inventories remain CI artifacts so
LOW/UNKNOWN findings can be reviewed explicitly. A green check is not a claim
that a service has no vulnerabilities.

The September 28, 2026 refresh includes gRPC 1.83.2 and x/crypto 0.56.0. The latter
raises the module's minimum Go version to 1.26; the compiler image and CI use Go
1.27. Both dependencies were refreshed even though the reported gRPC xDS-server
and crypto SSH code paths are absent from Fluxgate's release entry points.

The November 27, 2026 refresh responds to advisories published after the last
green scan, which began failing every pull request:

| Input | Advisories | Resolution |
| --- | --- | --- |
| OpenTelemetry 1.46.0 | [GO-2026-6975](https://pkg.go.dev/vuln/GO-2026-6975), [GO-2026-6974](https://pkg.go.dev/vuln/GO-2026-6974) | 1.47.0, and baggage propagation removed (below) |
| `golang.org/x/net` 0.58.0 | GO-2026-6603, -6610, -6611, -6612, -6617 | 0.60.0 |
| Go standard library 1.27.1 in release images | four HIGH, ten MEDIUM image findings | compiler image pinned to 1.27.4 |

GO-2026-6975 is memory amplification in W3C baggage parsing. The public HTTP
edge was never exposed — it clears baggage and extracts trace context alone —
but the Pub/Sub consumer parsed whatever baggage a message carried, which only
a holder of topic-publish rights can write. Nothing Fluxgate publishes carries
baggage and nothing reads it, so propagation is now W3C trace context only: the
parser is not merely patched but unreachable. `TestBrokerMessagesCarryNoBaggage`
pins both directions.

CI's Go setup resolves the newest 1.27 patch while the image pin is fixed, so
`govulncheck` and the image scan can disagree about the standard library. The
image tag now names the exact patch beside its digest, so the pinned compiler
is legible in review rather than only in a hash.

## Findings retained for review

- [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932) concerns the unmaintained
  `golang.org/x/crypto/openpgp` packages. The broader module is needed for other
  cryptographic primitives. The Linux dependency graph for all four release
  commands excludes OpenPGP, and CI explicitly rejects its introduction. The
  inventory-level finding remains visible rather than being globally ignored.
- Trivy reports `DLA-4792-1` for the pinned Debian 12 runtime's `tzdata`
  2026b package, with severity UNKNOWN and an available 2026c update. Fluxgate
  stores and aggregates UTC instants, so this timezone-data update does not alter
  its window calculations. Review the runtime image digest on each dependency
  refresh and rescan when the upstream base incorporates the update. Do not use
  this assessment to waive unrelated future UNKNOWN findings.

These notes are tied to the scan date and pinned inputs. Keep complete reports,
review newly reported advisories, and rerun both checks after any input changes.
