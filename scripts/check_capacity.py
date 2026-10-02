#!/usr/bin/env python3
"""Qualify saved capacity evidence against an explicit workload/latency policy.

No Docker or cloud access is needed. A qualifying local/emulator profile is a
regression check, not certification of a GCP deployment or a production SLO.
"""

import argparse
import datetime as dt
import json
import math
from pathlib import Path
import sys


LIMITS = {
    "min_offered_points_per_second": ("configuration.points_per_second", "min"),
    "min_duration_seconds": ("configuration.duration", "min"),
    "min_active_series": ("active_series", "min"),
    "min_tenants": ("configuration.tenants", "min"),
    "min_aggregators": ("resources.aggregator_replicas", "min"),
    "min_accepted_points_per_second": ("accepted_points_per_second", "min"),
    "max_missed_points": ("generator_missed_points", "max"),
    "max_p95_visibility_seconds": ("accepted_to_visible_p95_seconds", "max"),
    "max_query_p95_ms": ("query_probe_p95_ms", "max"),
    "max_drain_seconds": ("drain_seconds", "max"),
    "max_retryable_responses": ("retryable_responses", "max"),
}


def number(value):
    return (type(value) is int and value >= 0) or (type(value) is float and math.isfinite(value) and value >= 0)


def read_json(path):
    if path.stat().st_size > 16 * 1024**2:
        raise ValueError(f"{path.name}: evidence/policy exceeds 16 MiB")

    def reject_constant(value):
        raise ValueError(f"non-finite JSON number: {value}")

    def unique_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(path.read_text(encoding="utf-8"), parse_constant=reject_constant,
                      object_pairs_hook=unique_keys)


def qualify(report, policy):
    if not isinstance(report, dict) or not isinstance(policy, dict):
        raise ValueError("evidence and policy must be JSON objects")
    if set(policy) - {"name", "limits"} or not isinstance(policy.get("name"), str) or not policy["name"].strip():
        raise ValueError("policy requires a name and limits; unknown policy fields are rejected")
    limits = policy.get("limits")
    if not isinstance(limits, dict) or not limits or set(limits) - LIMITS.keys():
        raise ValueError("policy limits must be non-empty and use supported limit names")
    for name, threshold in limits.items():
        if not number(threshold) or (LIMITS[name][1] == "min" and threshold == 0):
            raise ValueError(f"{name}: expected a finite non-negative number (minimums must be positive)")

    checks = [
        dict(name="correctness", passed=report.get("passed") is True),
        dict(name="no_unresolved_errors", passed=report.get("errors") == [] and not report.get("failure")),
    ]
    offered, accepted, missed = (report.get(key) for key in ("offered_points", "accepted_points", "generator_missed_points"))
    accounted = all(number(v) for v in (offered, accepted, missed)) and accepted > 0 and offered == accepted + missed
    checks.append(dict(name="offered_points_accounted_for", passed=accounted))

    for name, threshold in limits.items():
        path, direction = LIMITS[name]
        observed = report
        if path == "retryable_responses":
            statuses = report.get("http_statuses")
            values = [statuses.get(code, 0) for code in ("429", "503")] if isinstance(statuses, dict) else [None]
            observed = sum(values) if all(number(v) for v in values) else None
        else:
            for key in path.split("."):
                observed = observed.get(key) if isinstance(observed, dict) else None
        valid = number(observed)
        passed = valid and (observed >= threshold if direction == "min" else observed <= threshold)
        checks.append(dict(name=name, observed=observed if valid else None, limit=threshold, passed=passed,
                           reason="within budget" if passed else "missing/invalid measurement" if not valid else "outside budget"))
    return dict(qualified=all(check["passed"] for check in checks), policy=policy,
                evidence_revision=report.get("revision"), evidence_started_at=report.get("started_at"),
                checks=checks)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path)
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    try:
        result = qualify(read_json(args.evidence), read_json(args.policy))
        exit_code = 0 if result["qualified"] else 1
    except (OSError, ValueError, OverflowError) as exc:
        print(f"Capacity qualification input error: {exc}", file=sys.stderr)
        # Replace any earlier success artifact, even when inputs are invalid.
        result = dict(qualified=False, input_error=str(exc))
        exit_code = 2
    result["evaluated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    output = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output, encoding="utf-8")
    print(output, end="")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
