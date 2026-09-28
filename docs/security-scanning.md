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
