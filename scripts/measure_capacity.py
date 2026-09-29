#!/usr/bin/env python3
"""Measure sustained local pipeline capacity using disposable Docker resources.

Open-loop scheduling, bounded clients, current event times, multiple tenants,
concurrent reads, exact reconciliation and machine-readable evidence. This is
an emulator benchmark, not a GCP capacity certification. Python 3, Docker and
the four packaged service images are required; build with verify_pipeline.py.
"""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
import math
from pathlib import Path
import threading
import time
import urllib.error
import urllib.request
import uuid

from verify_pipeline import command, require, wait_for


def percentile(values, fraction):
    return round(sorted(values)[math.ceil(len(values) * fraction) - 1], 3) if values else None


def request(url, token=None, body=None, idem=None):
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    if idem:
        headers["Idempotency-Key"] = idem
    if body is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers)
    try:
        response = urllib.request.urlopen(req, timeout=15)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, response.read().decode(), response.headers


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--points-per-second", type=int, default=10000)
    parser.add_argument("--duration", type=int, default=120, help="offered load duration, seconds")
    parser.add_argument("--series", type=int, default=10000, help="series per tenant")
    parser.add_argument("--tenants", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--flush-seconds", type=int, default=15)
    parser.add_argument("--drain-timeout", type=int, default=180)
    parser.add_argument("--image-prefix", default="fluxgate-validation")
    parser.add_argument("--tag", default="local")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for name, low, high in (("points_per_second", 100, 1000000), ("duration", 5, 600),
                            ("series", 1, 100000), ("tenants", 1, 32), ("batch_size", 1, 1000),
                            ("clients", 1, 128), ("window_seconds", 1, 60),
                            ("flush_seconds", 1, 60), ("drain_timeout", 10, 600)):
        if not low <= getattr(args, name) <= high:
            parser.error(f"--{name.replace('_', '-')} must be {low}..{high}")
    if args.flush_seconds > args.window_seconds:
        parser.error("--flush-seconds must not exceed --window-seconds")
    if args.points_per_second * args.duration > 10000000:
        parser.error("at most 10 million offered points per run (bounds client evidence memory)")

    prefix = "fluxgate-capacity-" + uuid.uuid4().hex[:10]
    images = {name: f"{args.image_prefix}/{name}:{args.tag}"
              for name in ("ingest-api", "aggregator", "query-api", "migrate")}
    containers = []
    stopped = threading.Event()
    lock = threading.Lock()
    accepted, first_visible, statuses, errors, probes, samples = {}, {}, Counter(), [], [], []
    secret = "disposable-capacity-secret"
    tokens = [f"fxg_t{i}_{secret}" for i in range(args.tenants)]
    keys = json.dumps([dict(key_id=f"t{i}", tenant_id=f"tenant-{i}",
                           secret_sha256=hashlib.sha256(secret.encode()).hexdigest())
                       for i in range(args.tenants)])
    report = {"configuration": vars(args) | {"output": str(args.output)},
              "revision": command("git", "rev-parse", "HEAD"),
              "dirty": bool(command("git", "status", "--porcelain")),
              "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "environment": json.loads(command("docker", "info", "--format", "{{json .}}")),
              "passed": False}
    # Keep only reproducibility data, not Docker's host paths or proxy settings.
    report["environment"] = {key: report["environment"].get(key)
                             for key in ("NCPU", "MemTotal", "OperatingSystem", "Architecture", "ServerVersion")}
    report["images"] = {name: json.loads(command("docker", "image", "inspect", image))[0]["Id"]
                       for name, image in images.items()}
    command("docker", "network", "create", prefix)

    def start(name, image, env=None, port=8080, memory="512m", cpus="1", extra=()):
        full = prefix + "-" + name
        containers.append(full)
        cmd = ["docker", "run", "-d", "--name", full, "--network", prefix,
               "--memory", memory, "--cpus", cpus]
        if image in images.values():
            cmd += ["--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges"]
        if port:
            cmd += ["-p", f"127.0.0.1::{port}"]
        for key, value in (env or {}).items():
            cmd += ["-e", f"{key}={value}"]
        command(*cmd, image, *extra)
        return full

    def url(name, port=8080):
        return "http://" + command("docker", "port", name, f"{port}/tcp").splitlines()[0]

    monitor = None
    try:
        pg = start("postgres", "postgres:17-alpine", dict(POSTGRES_USER="fluxgate",
                   POSTGRES_PASSWORD="fluxgate", POSTGRES_DB="fluxgate"), port=None, memory="2g", cpus="2")
        broker = start("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators",
                       port=8085, memory="1g", cpus="2", extra=("gcloud", "beta", "emulators", "pubsub", "start",
                       "--host-port=0.0.0.0:8085", "--project=fluxgate-test"))

        def sql(statement):
            return command("docker", "exec", "-e", "PGPASSWORD=fluxgate", pg, "psql", "-h", "127.0.0.1",
                           "-U", "fluxgate", "-d", "fluxgate", "-v", "ON_ERROR_STOP=1", "-tAc", statement)

        wait_for("PostgreSQL", lambda: sql("SELECT 1") == "1")
        wait_for("Pub/Sub", lambda: request(url(broker, 8085) + "/v1/projects/fluxgate-test/topics")[0] == 200)
        for role in ("ingest", "aggregator", "query"):
            sql(f"CREATE ROLE fluxgate_{role} LOGIN PASSWORD 'local-test-password'")
        command("docker", "run", "--rm", "--network", prefix, "-e",
                f"DATABASE_URL=postgres://fluxgate:fluxgate@{pg}/fluxgate?sslmode=disable",
                images["migrate"], "-grant-runtime-roles")

        def env(role):
            return dict(ENVIRONMENT="dev", API_KEYS=keys, GCP_PROJECT_ID="fluxgate-test",
                        PUBSUB_EMULATOR_HOST=broker + ":8085", DATABASE_MIGRATE="false",
                        DATABASE_URL=f"postgres://fluxgate_{role}:local-test-password@{pg}/fluxgate?sslmode=disable",
                        GOMEMLIMIT="350MiB", LOG_LEVEL="warn", SHUTDOWN_GRACE_PERIOD="0s",
                        RATE_LIMIT_POINTS_PER_SECOND="1000000", RATE_LIMIT_BURST="1000000")

        aggregator_env = env("aggregator") | dict(GOMEMLIMIT="700MiB",
                            AGGREGATOR_WINDOW_SIZE=f"{args.window_seconds}s",
                            AGGREGATOR_FLUSH_INTERVAL=f"{args.flush_seconds}s")
        services = {"aggregator": start("aggregator", images["aggregator"], aggregator_env, memory="1g")}
        for name, role in (("ingest-a", "ingest"), ("ingest-b", "ingest"), ("query", "query")):
            services[name] = start(name, images["query-api" if role == "query" else "ingest-api"], env(role))
        bases = {name: url(container) for name, container in services.items()}
        for name, base in bases.items():
            wait_for(name, lambda base=base: request(base + "/readyz")[0] == 200)
        report["resources"] = dict(api_cpu=1, api_memory_mib=512, aggregator_cpu=1, aggregator_memory_mib=1024,
                                   postgres_cpu=2, postgres_memory_mib=2048, broker_cpu=2, broker_memory_mib=1024,
                                   ingest_replicas=2, aggregator_replicas=1, query_replicas=1,
                                   http_max_concurrent=4, subscriber_bytes=16 * 1024**2,
                                   subscriber_messages=1000, tenant_rate_limit_per_replica=1000000)
        began = time.monotonic()

        def probe():
            while not stopped.is_set():
                sample = dict(elapsed_seconds=round(time.monotonic() - began, 3), services={})
                try:
                    for name, base in bases.items():
                        require(request(base + "/healthz")[0] == 200, name + " liveness failed")
                        code, metrics, _ = request(base + "/metrics")
                        require(code == 200, name + " metrics failed")
                        wanted = ("process_resident_memory_bytes", "process_cpu_seconds_total",
                                  "fluxgate_aggregate_tracked_series", "fluxgate_aggregate_buffered_bytes",
                                  "fluxgate_aggregate_pending_messages", "fluxgate_aggregate_pending_encoded_bytes",
                                  "fluxgate_aggregate_flush_duration_seconds_count", "fluxgate_aggregate_flush_duration_seconds_sum")
                        sample["services"][name] = {line.split()[0].split("{")[0]: float(line.split()[1])
                            for line in metrics.splitlines() if line.startswith(wanted)}
                    visible = sql("SELECT batch_id FROM processed_batches GROUP BY batch_id").splitlines()
                    now = time.monotonic()
                    with lock:
                        for batch in visible:
                            first_visible.setdefault(batch, now)
                        sample["accepted_points"] = len(accepted) * args.batch_size
                        sample["visible_batches"] = len(visible)
                    query_started = time.monotonic()
                    code, payload, _ = request(bases["query"] + "/v1/query?metric=capacity.gauge&from=-1h&agg=count&label.shard=0", tokens[0])
                    result = json.loads(payload)
                    probes.append(dict(milliseconds=(time.monotonic() - query_started) * 1000, status=code))
                    require(code == 200 and not result.get("truncated"), "concurrent query failed or truncated")
                    samples.append(sample)
                except Exception as exc:
                    errors.append("probe: " + str(exc))
                stopped.wait(2)

        monitor = threading.Thread(target=probe, daemon=True)
        monitor.start()
        slots = threading.BoundedSemaphore(args.clients)
        attempts = []

        def send(index):
            try:
                tenant = index % args.tenants
                timestamp = dt.datetime.now(dt.timezone.utc)
                window = int(timestamp.timestamp()) // args.window_seconds * args.window_seconds
                hosts = [(index // args.tenants * args.batch_size + j) % args.series for j in range(args.batch_size)]
                points = [dict(metric="capacity.gauge", kind="gauge", value=host % 17 + 1,
                               timestamp=timestamp.isoformat(),
                               labels=dict(host=str(host), shard=str(host // 100), detail="x" * 128)) for host in hosts]
                body = json.dumps(dict(points=points), separators=(",", ":")).encode()
                initial = time.monotonic()
                while True:
                    started = time.monotonic()
                    code, payload, headers = request(bases["ingest-a" if index % 2 else "ingest-b"] + "/v1/ingest",
                                                      tokens[tenant], body, f"capacity-{index}")
                    with lock:
                        statuses[code] += 1
                    if code == 202:
                        result = json.loads(payload)
                        require(result["accepted"] == args.batch_size and result["rejected"] == 0, "partial batch acceptance")
                        with lock:
                            require(result["batch_id"] not in accepted, "distinct requests shared batch identity")
                            accepted[result["batch_id"]] = (tenant, hosts, window, time.monotonic(), initial)
                            attempts.append((time.monotonic() - started) * 1000)
                        break
                    require(code in (429, 503) and time.monotonic() - initial < 30,
                            f"unresolved HTTP outcome {code}: {payload}")
                    time.sleep(min(5, max(.1, float(headers.get("Retry-After", "1")))))
            except Exception as exc:
                # Unknown HTTP outcomes cannot be silently excluded from exact totals.
                errors.append("send: " + str(exc))
            finally:
                slots.release()

        batches = args.points_per_second * args.duration // args.batch_size
        missed = 0
        with ThreadPoolExecutor(max_workers=args.clients) as pool:
            for index in range(batches):
                due = began + index * args.batch_size / args.points_per_second
                delay = due - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                # No unbounded queue and no catch-up burst that hides generator lag.
                if time.monotonic() - due > max(.1, args.batch_size / args.points_per_second) or not slots.acquire(blocking=False):
                    missed += 1
                    continue
                pool.submit(send, index)
        submit_seconds = time.monotonic() - began
        report["offered_points"] = batches * args.batch_size
        report["generator_missed_points"] = missed * args.batch_size
        report["accepted_points"] = len(accepted) * args.batch_size
        report["submit_seconds"] = round(submit_seconds, 3)
        report["accepted_points_per_second"] = round(len(accepted) * args.batch_size / max(args.duration, submit_seconds), 1)
        report["http_statuses"] = dict(statuses)
        report["accepted_attempt_p95_ms"] = percentile(attempts, .95)
        report["batch_accept_p95_ms"] = percentile([(row[3] - row[4]) * 1000 for row in accepted.values()], .95)
        print("Load submitted: " + json.dumps({k: report[k] for k in ("accepted_points", "generator_missed_points", "accepted_points_per_second")}), flush=True)

        def drained():
            with lock:
                return bool(accepted) and accepted.keys() <= first_visible.keys()

        wait_for("all accepted batches visible in committed delivery ledger", drained, timeout=args.drain_timeout)
        stopped.set()
        monitor.join(timeout=60)
        require(not monitor.is_alive(), "monitor did not stop")
        report["drain_seconds"] = round(time.monotonic() - began - submit_seconds, 3)
        report["accepted_to_visible_p95_seconds"] = percentile([max(0, first_visible[k] - row[3]) for k, row in accepted.items()], .95)
        report["accepted_to_visible_max_seconds"] = round(max(max(0, first_visible[k] - row[3]) for k, row in accepted.items()), 3)

        expected = Counter()
        for tenant, hosts, window, _, _ in accepted.values():
            expected.update((f"tenant-{tenant}", str(host), str(window)) for host in hosts)
        actual = {}
        rows = sql("SELECT tenant_id, labels->>'host', extract(epoch FROM window_start)::bigint, count, sum, min, max, last "
                   "FROM rollups WHERE metric='capacity.gauge'")
        for line in rows.splitlines():
            tenant, host, window, count, total, minimum, maximum, last = line.split("|")
            key = tenant, host, window
            value = int(host) % 17 + 1
            require(key not in actual, "duplicate series-window row")
            actual[key] = int(count)
            require(float(total) == int(count) * value and all(float(v) == value for v in (minimum, maximum, last)),
                    f"aggregate mismatch for {key}")
        require(actual == expected, "per-series/window counts differ from accepted points")
        report["reconciled_series_windows"] = len(actual)
        report["active_series"] = len({(tenant, host) for tenant, host, _ in actual})
        report["query_probe_p95_ms"] = percentile([p["milliseconds"] for p in probes], .95)
        for container in containers:
            info = json.loads(command("docker", "inspect", container))[0]
            require(info["State"]["Running"] and not info["State"]["OOMKilled"] and info["RestartCount"] == 0,
                    container + " stopped, restarted or exceeded memory")
        require(not errors, str(errors))
        report["passed"] = True
        print("PASS: all accepted points reconciled by tenant, series, window and aggregate", flush=True)
    except Exception as exc:
        report["failure"] = str(exc)
        raise
    finally:
        stopped.set()
        if monitor:
            monitor.join(timeout=60)
        report["errors"] = errors
        report["samples"] = samples
        report["query_probes"] = probes
        report["sampled_peak_rss_mib"] = {name: round(max(s["services"].get(name, {}).get("process_resident_memory_bytes", 0)
            for s in samples) / 1024**2, 2) for name in ("ingest-a", "ingest-b", "aggregator", "query")} if samples else {}
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        for container in reversed(containers):
            if not report["passed"]:
                print(command("docker", "logs", "--tail", "10", container, check=False))
            command("docker", "rm", "-fv", container, check=False)
        command("docker", "network", "rm", prefix, check=False)


if __name__ == "__main__":
    main()
