#!/usr/bin/env python3
"""Audit all packaged services, preserving vulnerability reports and SBOMs.

Requires Docker and Python 3. The pinned scanner reads exported image archives;
it receives no Docker socket or application credentials. A current vulnerability
database is downloaded on each run (Trivy reuses a fresh cache when available).
"""

import argparse
import json
from pathlib import Path
import subprocess
import tempfile

SCANNER = "aquasec/trivy@sha256:62b1e65e8869bc4b4c6aa4fa2b21595256c7c2f6018a9d9ad61caf87187c1969"  # 0.74.0
SERVICES = ("ingest-api", "aggregator", "query-api", "migrate")


def run(*args):
    subprocess.run(args, check=True, timeout=900)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image-prefix", default="fluxgate-validation")
    parser.add_argument("--tag", default="local")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cache-dir", type=Path, required=True)
    args = parser.parse_args()
    output = args.output_dir.resolve()
    cache = args.cache_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    cache.mkdir(parents=True, exist_ok=True)
    findings = []
    for service in SERVICES:
        image = f"{args.image_prefix}/{service}:{args.tag}"
        print(f"Auditing {image}", flush=True)
        # Each run owns this temporary directory; cleanup never touches images
        # or another run's archives, reports, or vulnerability database.
        with tempfile.TemporaryDirectory(prefix="fluxgate-image-") as temp:
            archive = Path(temp).resolve()
            run("docker", "save", "--output", str(archive / "image.tar"), image)
            command = ["docker", "run", "--rm", "--cap-drop=ALL", "--security-opt=no-new-privileges",
                       "-v", f"{archive}:/input:ro", "-v", f"{output}:/reports",
                       "-v", f"{cache}:/root/.cache/trivy", SCANNER, "image",
                       "--input", "/input/image.tar", "--scanners", "vuln", "--timeout", "10m", "--quiet"]
            report = output / f"{service}-vulnerabilities.json"
            run(*command, "--format", "json", "--output", f"/reports/{report.name}")
            run(*command, "--format", "cyclonedx", "--output", f"/reports/{service}-sbom.json")
            data = json.loads(report.read_text(encoding="utf-8"))
            for result in data.get("Results", []):
                for vuln in result.get("Vulnerabilities", []):
                    if vuln.get("Severity") in ("HIGH", "CRITICAL"):
                        findings.append(f"{service}: {vuln['VulnerabilityID']} {vuln['PkgName']} "
                                        f"{vuln['InstalledVersion']} ({vuln['Severity']})")
    if findings:
        raise SystemExit("Image audit failed:\n" + "\n".join(findings))
    print("PASS: all four images contain no known HIGH or CRITICAL vulnerabilities.", flush=True)


if __name__ == "__main__":
    main()
