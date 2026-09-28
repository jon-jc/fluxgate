#!/usr/bin/env python3
"""Verify the packaged pipeline with disposable Docker resources; no cloud access.

Requires Docker and Python 3. Uses unique container names, loopback-only random
ports and local test credentials. Removes only resources created by this run.
"""

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def command(*args, input_text=None, check=True, timeout=120):
    result = subprocess.run(args, input=input_text, text=True, capture_output=True,
                            cwd=ROOT, timeout=timeout, encoding="utf-8")
    if check and result.returncode:
        raise RuntimeError(f"{args[0]} failed: {result.stderr or result.stdout}")
    return (result.stdout + (result.stderr if not check else "")).strip()


def wait_for(description, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, ValueError, RuntimeError) as exc:
            last = exc
        time.sleep(0.5)
    raise RuntimeError(f"Timed out: {description}; last error: {last}")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--image-prefix", default="fluxgate-validation")
    parser.add_argument("--tag", default="local")
    args = parser.parse_args()
    images = {service: f"{args.image_prefix}/{service}:{args.tag}"
              for service in ("ingest-api", "aggregator", "query-api", "migrate")}
    if not args.skip_build:
        revision = command("git", "rev-parse", "HEAD")
        if command("git", "status", "--porcelain"):
            revision += "-dirty"
        for service, image in images.items():
            print(f"Building {service}", flush=True)
            command("docker", "build", "-f", "build/docker/Dockerfile", "--build-arg",
                    f"SERVICE={service}", "--build-arg", f"COMMIT={revision}", "-t", image, ".", timeout=600)

    prefix = "fluxgate-check-" + uuid.uuid4().hex[:10]
    containers = []
    command("docker", "network", "create", prefix)

    def start(suffix, image, env=None, port=None, extra=()):
        name = prefix + "-" + suffix
        containers.append(name)
        cmd = ["docker", "run", "-d", "--name", name, "--network", prefix]
        if port:
            cmd += ["-p", f"127.0.0.1::{port}"]
        for key, value in (env or {}).items():
            cmd += ["-e", f"{key}={value}"]
        command(*cmd, image, *extra)
        return name

    def base_url(name, port=8080):
        mapping = command("docker", "port", name, f"{port}/tcp").splitlines()[0]
        return "http://" + mapping

    secret = "isolated-pipeline-test-secret"
    keys = json.dumps([dict(key_id=key, tenant_id=tenant,
                           secret_sha256=hashlib.sha256(secret.encode()).hexdigest())
                       for key, tenant in (("a", "tenant-a"), ("b", "tenant-b"))])

    def request(url, body=None, key="a", idem=None, raw=False):
        headers = {}
        if key:
            headers["Authorization"] = f"Bearer fxg_{key}_{secret}"
        if idem:
            headers["Idempotency-Key"] = idem
        if body is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(body, separators=(",", ":")).encode()
        req = urllib.request.Request(url, data=body, headers=headers)
        try:
            response = urllib.request.urlopen(req, timeout=15)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            data = response.read().decode()
            return response.status, data if raw else json.loads(data), response.headers

    def ready(name):
        wait_for(name + " readiness", lambda: request(base_url(name) + "/readyz")[0] == 200)

    try:
        postgres = start("postgres", "postgres:17-alpine", {
            "POSTGRES_USER": "fluxgate", "POSTGRES_PASSWORD": "fluxgate", "POSTGRES_DB": "fluxgate"})
        broker = start("pubsub", "gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators", port=8085,
                       extra=("gcloud", "beta", "emulators", "pubsub", "start", "--host-port=0.0.0.0:8085", "--project=fluxgate-test"))

        def sql(statement):
            # The image's temporary initialization server accepts Unix sockets
            # before the final TCP server is ready for the application.
            return command("docker", "exec", "-i", "-e", "PGPASSWORD=fluxgate", postgres,
                           "psql", "-h", "127.0.0.1", "-U", "fluxgate", "-d", "fluxgate",
                           "-v", "ON_ERROR_STOP=1", "-tAc", statement)

        wait_for("PostgreSQL", lambda: sql("SELECT 1") == "1")
        wait_for("Pub/Sub", lambda: request(base_url(broker, 8085) + "/v1/projects/fluxgate-test/topics")[0] == 200)
        for role in ("ingest", "aggregator", "query"):
            sql(f"CREATE ROLE fluxgate_{role} LOGIN PASSWORD 'local-test-password'")
        owner_dsn = f"postgres://fluxgate:fluxgate@{postgres}/fluxgate?sslmode=disable"
        command("docker", "run", "--rm", "--network", prefix, "-e", "DATABASE_URL=" + owner_dsn,
                images["migrate"], "-grant-runtime-roles")

        def runtime_env(role):
            return dict(ENVIRONMENT="dev", API_KEYS=keys, GCP_PROJECT_ID="fluxgate-test",
                        PUBSUB_EMULATOR_HOST=broker + ":8085", DATABASE_MIGRATE="false",
                        DATABASE_URL=f"postgres://fluxgate_{role}:local-test-password@{postgres}/fluxgate?sslmode=disable",
                        SHUTDOWN_GRACE_PERIOD="0s", SHUTDOWN_DRAIN_TIMEOUT="3s",
                        HTTP_MAX_CONCURRENT="8", HTTP_HANDLER_TIMEOUT="3s", HTTP_WRITE_TIMEOUT="5s", PUBSUB_PUBLISH_TIMEOUT="2s")

        aggregator_env = runtime_env("aggregator")
        aggregator_env.update(AGGREGATOR_WINDOW_SIZE="5s", AGGREGATOR_ALLOWED_LATENESS="1s",
                              AGGREGATOR_FLUSH_INTERVAL="1s", AGGREGATOR_IDLE_TIMEOUT="1s", PRUNE_INTERVAL="1s")
        aggregator = start("aggregator", images["aggregator"], aggregator_env, port=8080)
        first = start("ingest-a", images["ingest-api"], runtime_env("ingest"), port=8080)
        second = start("ingest-b", images["ingest-api"], runtime_env("ingest"), port=8080)
        reader = start("query", images["query-api"], runtime_env("query"), port=8080)
        for name in (aggregator, first, second, reader):
            ready(name)
        ingest_urls = [base_url(name) + "/v1/ingest" for name in (first, second)]
        query_url = base_url(reader) + "/v1/query?from=-1h&agg=sum&metric="
        require(request(query_url + "verify.load", key=None)[0] == 401, "unauthenticated query accepted")
        require(request(ingest_urls[0], {"points": []}, key=None)[0] == 401, "unauthenticated ingest accepted")
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2)).isoformat()
        body = {"points": [dict(metric="verify.load", kind="counter", value=1, timestamp=old)]}

        def retry_pair(index):
            one = request(ingest_urls[index % 2], body, idem=f"load-{index}")
            two = request(ingest_urls[(index + 1) % 2], body, idem=f"load-{index}")
            require(one[0] == two[0] == 202 and one[1]["batch_id"] == two[1]["batch_id"], "cross-replica retry changed identity")
            return one[1]["batch_id"]

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            batch_ids = list(pool.map(retry_pair, range(40)))
        require(len(set(batch_ids)) == 40, "distinct requests shared batch identities")
        command("docker", "restart", "-t", "5", first)
        ready(first)
        # Docker can choose a new ephemeral host port after restart.
        ingest_urls[0] = base_url(first) + "/v1/ingest"
        replay = request(ingest_urls[0], body, idem="load-0")
        require(replay[0] == 202 and replay[1]["batch_id"] == batch_ids[0], "restart lost durable retry record")
        different = {"points": [dict(metric="verify.load", kind="counter", value=99, timestamp=old)]}
        require(request(ingest_urls[1], different, idem="load-0")[0] == 409, "conflicting retry accepted")

        def total(metric, key="a"):
            code, result, _ = request(query_url + metric, key=key)
            require(code == 200, "query failed")
            return sum(point["v"] for series in result["series"] for point in series["points"])

        wait_for("40 unique points counted once", lambda: total("verify.load") == 40)
        require(total("verify.load", "b") == 0, "tenant isolation failed")
        print("PASS: auth, 40 concurrent requests, cross-replica/restart retries, exact totals and tenant isolation", flush=True)

        # A future window cannot flush before the process is killed. Confirm the
        # message entered the engine, then kill it without a graceful final flush.
        metric_url = base_url(aggregator) + "/metrics"
        def consumed():
            metrics = request(metric_url, raw=True)[1]
            return sum(float(line.rsplit(" ", 1)[1]) for line in metrics.splitlines()
                       if line.startswith("fluxgate_consume_messages_total{") and 'outcome="ok"' in line)
        before = consumed()
        future = dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=2)
        crash_body = {"points": [dict(metric="verify.crash", kind="gauge", value=7, timestamp=future.isoformat())]}
        require(request(ingest_urls[0], crash_body, idem="crash")[0] == 202, "crash test publish failed")
        wait_for("aggregator accepted uncommitted message", lambda: consumed() > before)
        require(sql("SELECT count(*) FROM rollups WHERE metric='verify.crash'") == "0", "crash fixture flushed early")
        command("docker", "kill", "--signal=KILL", aggregator)
        command("docker", "start", aggregator)
        ready(aggregator)
        marker = {"points": [dict(metric="verify.marker", kind="gauge", value=0,
                                   timestamp=(future + dt.timedelta(minutes=1)).isoformat())]}
        require(request(ingest_urls[0], marker)[0] == 202, "watermark marker failed")
        wait_for("redelivery after hard termination", lambda: sql("SELECT COALESCE(sum(sum),0) FROM rollups WHERE metric='verify.crash'") == "7", timeout=120)
        require(total("verify.load") == 40, "recovery double-counted earlier data")
        print("PASS: forced aggregator termination before commit and broker redelivery", flush=True)

        command("docker", "stop", "-t", "5", postgres)
        failed = request(ingest_urls[0], body, idem="database-outage")
        require(failed[0] == 503, "database outage was acknowledged as accepted")
        command("docker", "start", postgres)
        wait_for("database recovery", lambda: sql("SELECT 1") == "1")
        ready(first)
        require(request(ingest_urls[1], body, idem="database-outage")[0] == 202, "retry after DB recovery failed")
        wait_for("recovered write counted once", lambda: total("verify.load") == 41)

        # More than one cleanup chunk, exercised by the actual restricted role.
        sql("INSERT INTO ingest_requests (tenant_id,idempotency_key,fingerprint,status,response,batch,expires_at) "
            "SELECT 'expired','key-'||i,'fp',202,'{}'::bytea,'{}'::jsonb,now()-interval '1 hour' FROM generate_series(1,10050) i")
        wait_for("retention drains multiple chunks", lambda: sql("SELECT count(*) FROM ingest_requests WHERE tenant_id='expired'") == "0")
        print("PASS: database outage/recovery and retention under restricted runtime permissions", flush=True)
        print("Pipeline verification passed. No GCP resources were used.", flush=True)
    except BaseException:
        for name in containers:
            print(f"--- {name} ---\n" + command("docker", "logs", "--tail", "30", name, check=False), flush=True)
        raise
    finally:
        for name in reversed(containers):
            command("docker", "rm", "-f", "-v", name, check=False)
        command("docker", "network", "rm", prefix, check=False)


if __name__ == "__main__":
    main()
