import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from check_capacity import qualify, read_json


class QualificationTests(unittest.TestCase):
    def setUp(self):
        self.report = dict(passed=True, errors=[], offered_points=90000, accepted_points=90000,
                           generator_missed_points=0, accepted_points_per_second=2000,
                           configuration=dict(points_per_second=2000, duration=45, tenants=4),
                           resources=dict(aggregator_replicas=2), active_series=10000,
                           accepted_to_visible_p95_seconds=15, drain_seconds=15,
                           query_probe_p95_ms=30, http_statuses={"202": 180})
        self.policy = read_json(Path(__file__).resolve().parents[1] / "deploy/capacity/ci.json")

    def test_qualifying_profile_and_inclusive_boundaries(self):
        self.assertTrue(qualify(self.report, self.policy)["qualified"])
        self.report["accepted_to_visible_p95_seconds"] = 45
        self.assertTrue(qualify(self.report, self.policy)["qualified"])

    def test_correct_totals_do_not_hide_missed_traffic_or_slow_visibility(self):
        for changes in [dict(generator_missed_points=500, accepted_points=89500),
                        dict(accepted_to_visible_p95_seconds=46), dict(drain_seconds=91),
                        dict(query_probe_p95_ms=1001), dict(accepted_points_per_second=1899),
                        dict(configuration=dict(points_per_second=2000, duration=5, tenants=4)),
                        dict(active_series=9999), dict(resources=dict(aggregator_replicas=1))]:
            with self.subTest(changes=changes):
                self.assertFalse(qualify(self.report | changes, self.policy)["qualified"])

    def test_missing_invalid_and_unresolved_evidence_fails_closed(self):
        for changes in [dict(passed=False), dict(passed="true"), dict(errors=["unknown outcome"]),
                        dict(failure="broker stopped"), dict(accepted_points=89500),
                        dict(accepted_to_visible_p95_seconds=None), dict(query_probe_p95_ms=True),
                        dict(query_probe_p95_ms=float("nan")), dict(query_probe_p95_ms=float("inf")),
                        dict(accepted_points=0, offered_points=0), dict(configuration={})]:
            with self.subTest(changes=changes):
                self.assertFalse(qualify(self.report | changes, self.policy)["qualified"])

    def test_invalid_policy_never_silently_disables_a_gate(self):
        for limits in [{}, {"max_mised_points": 0}, {"max_missed_points": -1},
                       {"max_missed_points": True}, {"min_duration_seconds": 0},
                       {"max_drain_seconds": float("inf")}, {"max_drain_seconds": "45"}]:
            with self.subTest(limits=limits), self.assertRaises(ValueError):
                qualify(self.report, dict(name="invalid", limits=limits))

    def test_retry_budget_counts_both_backpressure_statuses(self):
        policy = dict(name="no backpressure", limits=dict(max_retryable_responses=0))
        self.assertTrue(qualify(self.report, policy)["qualified"])
        for code in ("429", "503"):
            report = self.report | {"http_statuses": {"202": 180, code: 1}}
            self.assertFalse(qualify(report, policy)["qualified"])

    def test_cli_fails_qualification_and_preserves_machine_readable_results(self):
        with tempfile.TemporaryDirectory() as directory:
            evidence, policy, output = (Path(directory) / name for name in ("evidence.json", "policy.json", "result.json"))
            report = copy.deepcopy(self.report)
            report["drain_seconds"] = 91
            evidence.write_text(json.dumps(report), encoding="utf-8")
            policy.write_text(json.dumps(self.policy), encoding="utf-8")
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("check_capacity.py")),
                                     str(evidence), "--policy", str(policy), "--output", str(output)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1, result.stderr)
            self.assertFalse(json.loads(output.read_text(encoding="utf-8"))["qualified"])
            evidence.write_text('{"passed": true, "passed": false}', encoding="utf-8")
            with self.assertRaises(ValueError):
                read_json(evidence)
            output.write_text('{"qualified": true}', encoding="utf-8")
            result = subprocess.run([sys.executable, str(Path(__file__).with_name("check_capacity.py")),
                                     str(evidence), "--policy", str(policy), "--output", str(output)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 2, result.stderr)
            self.assertFalse(json.loads(output.read_text(encoding="utf-8"))["qualified"])
            evidence.write_text('{"latency": NaN}', encoding="utf-8")
            with self.assertRaises(ValueError):
                read_json(evidence)


if __name__ == "__main__":
    unittest.main()
