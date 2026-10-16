#!/usr/bin/env python3
"""Required local DQ acceptance: real processes, broker, storage and canonical PG.

No existing database or production endpoint is accepted. All credentials and
fixtures are generated for new Testcontainers and destroyed on exit.
"""
import argparse
import base64
import contextlib
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import yaml
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import ExecWaitStrategy

ROOT = Path(__file__).resolve().parents[4]
SERVICE = ROOT / "services/dq"
SCOPE = {"installation_id": "dq-local-test", "namespace": "beta"}
POLICY = "acceptance-v1"
QUERY = "?" + urllib.parse.urlencode(SCOPE)
TOPICS = {"dq.commands.v1": ("delete", 7), "dq.worker-reports.v1": ("delete", 7),
          "dq.events.v1": ("delete", 30), "dq.dlq.v1": ("delete", 30),
          "dq.registry.v1": ("compact", 0), "dq.audit.v1": ("delete", 180)}


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def local_environment(values):
    # A developer's shell can contain production endpoints or secret-file
    # overrides. Only this harness supplies platform settings to its processes.
    prefixes = ("POSTGRES_", "OCTOPUS_", "DQ_", "MINIO_", "SYNC_", "OTEL_", "COORDINATOR_")
    environment = {key: value for key, value in os.environ.items() if not key.startswith(prefixes)}
    environment.update({key: str(value) for key, value in values.items()})
    environment["OTEL_SDK_DISABLED"] = "true"
    return environment


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def database_host(container_host):
    # Octopus forbids a loopback database hostname. The generated container
    # publishes only its isolated test port; connect to that port through an
    # address bound to this computer. UDP connect chooses an interface without
    # transmitting any packets to the documentation-only destination.
    if container_host not in {"localhost", "127.0.0.1", "::1"}:
        return container_host
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("192.0.2.1", 80))
        address = sock.getsockname()[0]
    check(not address.startswith("127."), "a local non-loopback interface is required for Octopus acceptance")
    return address


def check(condition, description):
    if not condition:
        raise AssertionError(description)


class TransientHTTPResponse(Exception):
    pass


def eventually(fn, description, timeout=45):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = fn()
            if last:
                return last
        except (urllib.error.URLError, ConnectionError, TimeoutError, TransientHTTPResponse):
            pass
        time.sleep(0.1)
    raise AssertionError(f"timed out: {description}; last={last!r}")


class Stack:
    def __init__(self, args, directory):
        self.args, self.directory = args, directory
        self.cleanup = contextlib.ExitStack()
        self.processes, self.logs = [], []
        self.control_token, self.worker_token = secrets.token_hex(32), secrets.token_hex(32)
        self.user_token, self.viewer_token = secrets.token_hex(32), secrets.token_hex(32)
        self.storage_access, self.storage_secret = secrets.token_hex(12), secrets.token_hex(32)
        self.database_password = secrets.token_hex(32)
        self.runtime_password = secrets.token_hex(32)
        self.search_password = secrets.token_hex(32)
        self.admission_ms, self.runs, self.managers, self.workers = [], [], [], []
        self.scenarios = []
        self.load_results = []

    def close(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        for log in self.logs:
            log.close()
        self.cleanup.close()

    def execute(self, argv, *, env=None, stdin=None, timeout=60, cwd=ROOT):
        result = subprocess.run(argv, cwd=cwd, env=env, input=stdin, text=True,
                                capture_output=True, timeout=timeout)
        if result.returncode:
            # Never include a credential-bearing command line or environment.
            raise RuntimeError(f"{Path(argv[0]).name} failed: {result.stderr[-3000:]}")
        return result.stdout

    def launch(self, name, argv, env):
        log = (self.directory / f"{name}.log").open("w")
        process = subprocess.Popen(argv, cwd=ROOT, env=local_environment(env),
                                   stdout=log, stderr=subprocess.STDOUT)
        self.logs.append(log)
        self.processes.append(process)
        return process

    def sql(self, query, role="postgres", password=None):
        password = password or self.database_password
        result = self.pg.exec(["env", f"PGPASSWORD={password}", "psql", "-X", "-h", "127.0.0.1",
                               "-U", role, "-d", "sync", "-v", "ON_ERROR_STOP=1", "-Atc", query])
        if result.exit_code:
            raise AssertionError(result.output.decode(errors="replace")[-4000:])
        return result.output.decode().strip()

    def query_json(self, query):
        return json.loads(self.sql(query))

    def probe(self, action, value=None, address=None, token=None):
        environment = local_environment(dict(MINIO_ENDPOINT=self.minio_address,
                           MINIO_ACCESS_KEY=self.storage_access, MINIO_SECRET_KEY=self.storage_secret,
                           DQ_BROKERS=self.broker_address, DQ_WORKER_TOKEN=token or self.worker_token,
                           DQ_PROBE_WORKER=address or self.worker_addresses[0]))
        output = self.execute([str(self.directory / "probe"), action], env=environment,
                              stdin=json.dumps(value) if value is not None else None)
        return json.loads(output)

    def http(self, base, path, body=None, *, token=None, expected=200, actor=False):
        headers = {"Authorization": "Bearer " + (token or self.user_token)}
        if actor:
            headers.update({"X-DQ-Actor": hashlib.sha256(b"local-acceptance").hexdigest(),
                            "X-DQ-Roles": "admin,operator,viewer", "X-DQ-Scopes": "north,south"})
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(base + path, data=canonical(body) if body is not None else None,
                                         headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=15)
        except urllib.error.HTTPError as error:
            if error.code >= 500:
                error.close()
                raise TransientHTTPResponse(f"HTTP {error.code} from {path}")
            response = error
        with response:
            raw = response.read()
            check(response.status == expected,
                  f"HTTP {path.split('?')[0]} expected {expected}, got {response.status}: {raw[:300]!r}")
            return json.loads(raw) if raw else None

    def internal(self, path, body=None, expected=200, actor=False):
        return self.http(self.octopus_url + "/internal/dq/v1", path, body,
                         token=self.control_token, expected=expected, actor=actor)

    def public(self, path, body=None, expected=200, token=None, manager=0):
        return self.http(self.manager_urls[manager] + "/v1", path, body, expected=expected, token=token)

    def admit(self, key, scope="north", maximum=20, operation="prepare_candidates", candidate=None, internal=False):
        parameters = {"authorized_scope": scope, "policy_version": POLICY, "max_candidates": maximum}
        if candidate:
            parameters["candidate_id"] = candidate
        request = dict(SCOPE, processor_id="atheros.dedup", operation=operation,
                       parameters=parameters, idempotency_key=key)
        start = time.perf_counter()
        run = (self.internal("/runs", request, expected=202, actor=True) if internal
               else self.public("/runs", request, expected=202, manager=len(self.runs) % 2))
        self.admission_ms.append((time.perf_counter() - start) * 1000)
        self.runs.append(run["run_id"])
        return request, run

    def status(self, run_id):
        return self.internal("/runs/" + run_id + QUERY)

    def terminal(self, run_id, success=True, timeout=60):
        def done():
            run = self.status(run_id)
            return run if run["status"] in {"succeeded", "failed", "cancelled"} else None
        try:
            run = eventually(done, "run terminal state", timeout)
        except AssertionError:
            evidence = self.query_json(
                "SELECT jsonb_build_object('status',r.status,'error_code',r.error_code,"
                "'attempt_count',r.attempt_count,'run_age_seconds',extract(epoch FROM (now()-r.created_at)),"
                "'attempts',(SELECT jsonb_agg(jsonb_build_object('status',a.status,'fence',a.fence,"
                "'report_sequence',a.report_sequence,'lease_remaining_seconds',extract(epoch FROM (a.lease_expires_at-now())))) "
                "FROM octopus_core.dq_attempts a WHERE a.run_id=r.run_id),"
                "'reports',(SELECT jsonb_agg(jsonb_build_object('event_type',e.envelope->>'event_type',"
                "'sequence',e.envelope->>'sequence','quarantine_reason',e.quarantine_reason)) "
                "FROM octopus_core.dq_receipts e WHERE e.envelope->>'run_id'=r.run_id)) "
                f"FROM octopus_core.dq_runs r WHERE r.run_id='{run_id}'")
            (self.directory / "terminal-failure.json").write_text(json.dumps(evidence, indent=2) + "\n")
            raise AssertionError(f"run did not finish: {json.dumps(evidence)}") from None
        if success:
            check(run["status"] == "succeeded", f"run failed: {run.get('error_code', run['status'])}")
        return run

    def pause_managers(self):
        for process in self.managers:
            if process.poll() is None:
                process.send_signal(signal.SIGSTOP)

    def resume_managers(self):
        for process in self.managers:
            if process.poll() is None:
                process.send_signal(signal.SIGCONT)

    def setup(self):
        print("Building fresh acceptance binaries", flush=True)
        for name, package in (("manager", "./cmd/manager"), ("worker", "./cmd/worker"),
                              ("probe", "./tests/acceptance/probe")):
            self.execute(["go", "build", "-o", str(self.directory / name), package], cwd=SERVICE, timeout=180)
        implementation = "sha256:" + hashlib.sha256((self.directory / "worker").read_bytes()).hexdigest()
        self.implementation = implementation
        jar = self.args.octopus_jar.resolve()
        self.jar = jar
        check(jar.is_file(), "a rebuilt Octopus assembly is required")
        self.octopus_assembly_sha256 = hashlib.sha256(jar.read_bytes()).hexdigest()
        latest = max(path.stat().st_mtime for path in (ROOT / "services/octopus/src/main").rglob("*") if path.is_file())
        check(jar.stat().st_mtime >= latest, "Octopus assembly is stale: run sbt assembly first")
        print("Starting isolated Testcontainers dependencies", flush=True)
        self.pg = self.cleanup.enter_context(
            DockerContainer(self.args.postgres_image).with_env("POSTGRES_PASSWORD", self.database_password)
            .with_env("POSTGRES_DB", "sync").with_exposed_ports(5432)
            .waiting_for(ExecWaitStrategy(["env", f"PGPASSWORD={self.database_password}", "PGCONNECT_TIMEOUT=2",
                                         "psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "sync", "-Atc", "SELECT 1"])
                         .with_startup_timeout(60)))
        broker_port = port()
        self.redpanda = self.cleanup.enter_context(
            DockerContainer(self.args.redpanda_image).with_bind_ports(19092, broker_port)
            .with_command(f"redpanda start --mode dev-container --smp 1 --memory 512M --reserve-memory 0M "
                          f"--overprovisioned --node-id 0 --check=false "
                          f"--kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 "
                          f"--advertise-kafka-addr internal://127.0.0.1:9092,external://127.0.0.1:{broker_port}")
            .waiting_for(ExecWaitStrategy(["rpk", "cluster", "info", "--brokers", "127.0.0.1:9092"])
                         .with_startup_timeout(90)))
        self.broker_address = f"127.0.0.1:{broker_port}"
        kms_key = "acceptance:" + base64.b64encode(secrets.token_bytes(32)).decode()
        self.minio = self.cleanup.enter_context(
            DockerContainer(self.args.minio_image).with_env("MINIO_ROOT_USER", self.storage_access)
            .with_env("MINIO_ROOT_PASSWORD", self.storage_secret).with_env("MINIO_KMS_SECRET_KEY", kms_key)
            .with_exposed_ports(9000).with_command("server /data --address :9000"))
        self.minio_address = f"127.0.0.1:{self.minio.get_exposed_port(9000)}"
        eventually(lambda: urllib.request.urlopen(f"http://{self.minio_address}/minio/health/ready", timeout=2).status == 200,
                   "MinIO readiness")
        self.worker_addresses = [f"127.0.0.1:{port()}" for _ in range(2)]
        self.probe("setup-storage")
        for topic, (cleanup, days) in TOPICS.items():
            command = ["rpk", "topic", "create", topic, "--brokers", "127.0.0.1:9092", "--partitions", "1" if cleanup == "compact" else "3",
                       "--replicas", "1", "--topic-config", f"cleanup.policy={cleanup}",
                       "--topic-config", "max.message.bytes=65536"]
            if days:
                command += ["--topic-config", f"retention.ms={days * 86400000}"]
            result = self.redpanda.exec(command)
            check(result.exit_code == 0, f"topic setup failed: {topic}: {result.output.decode(errors='replace')[:300]}")
        self.provision()
        self.fixture("north", 1000, 1)
        self.fixture("south", 2, 2001)
        self.manifest = self.make_manifest(implementation)
        manifest_file = self.directory / "manifest.json"
        manifest_file.write_bytes(canonical(self.manifest))
        octopus_port = port()
        self.octopus_url = f"http://127.0.0.1:{octopus_port}"
        octopus_env = {
            "POSTGRES_ENABLED": "true", "POSTGRES_HOST": database_host(self.pg.get_container_host_ip()),
            "POSTGRES_PORT": self.pg.get_exposed_port(5432), "POSTGRES_DATABASE": "sync",
            "POSTGRES_USER": "dq_octopus_test", "POSTGRES_PASSWORD": self.runtime_password,
            "POSTGRES_SSL_MODE": "disable", "POSTGRES_SCHEMA_MANIFEST_SHA256": self.manifests["octopus_core"]["manifest_sha256"],
            "OCTOPUS_ENVIRONMENT": "test", "OCTOPUS_PROCESSORS_ENABLED": "false", "OCTOPUS_CONSUMERS_ENABLED": "false",
            "COORDINATOR_HTTP_PORT": str(octopus_port),
            "SYNC_REDPANDA_BOOTSTRAP_SERVERS": self.broker_address, "SYNC_REDPANDA_TOPIC_PARTITIONS": "3",
            "SYNC_REDPANDA_TOPIC_REPLICATION_FACTOR": "1", "OCTOPUS_DQ_ENABLED": "true",
            "OCTOPUS_DQ_INSTALLATION_ID": SCOPE["installation_id"], "OCTOPUS_DQ_NAMESPACE": SCOPE["namespace"],
            "OCTOPUS_DQ_SCOPE_MODES_JSON": json.dumps({"north": "shadow", "south": "enabled", "load": "shadow"}),
            "OCTOPUS_DQ_POLICY_VERSION": POLICY, "OCTOPUS_DQ_APPROVED_DIGESTS_JSON": json.dumps([implementation]),
            "OCTOPUS_DQ_CONTROL_TOKEN": self.control_token, "OCTOPUS_DQ_POLL_INTERVAL_MS": "100",
            "OCTOPUS_DQ_LEASE_SECONDS": "60", "MINIO_ENDPOINT": "http://" + self.minio_address,
            "MINIO_ACCESS_KEY_ID": self.storage_access, "MINIO_SECRET_ACCESS_KEY": self.storage_secret,
            "MINIO_REGION": "us-east-1", "SYNC_OUTBOX_DIR": str(self.directory / "outbox")}
        self.octopus_env = octopus_env
        self.octopus = self.launch("octopus", ["java", "-XX:ActiveProcessorCount=4", "-Xmx768m", "-jar", str(jar)], octopus_env)
        eventually(lambda: self.internal("/processors" + QUERY).get("items") == [], "Octopus authenticated readiness", 90)
        self.internal("/processor-versions", self.manifest, expected=201, actor=True)
        common = {"DQ_INSECURE_DEVELOPMENT": "true", "DQ_BROKER_TLS": "false", "MINIO_SECURE": "false",
                  "MINIO_ENDPOINT": self.minio_address, "MINIO_ACCESS_KEY": self.storage_access,
                  "MINIO_SECRET_KEY": self.storage_secret, "DQ_BROKERS": self.broker_address,
                  "DQ_WORKER_TOKEN": self.worker_token, "DQ_WORKER_MANIFEST_FILE": str(manifest_file),
                  "DQ_WORKER_IMPLEMENTATION_DIGEST": implementation}
        for index, address in enumerate(self.worker_addresses):
            health_port = port()
            environment = dict(common, DQ_WORKER_LISTEN=address, DQ_WORKER_HTTP_LISTEN=f"127.0.0.1:{health_port}",
                               DQ_WORKER_INSTANCE_PREFIX=f"acceptance-{index}")
            self.workers.append(self.launch(f"worker-{index}", [str(self.directory / "worker")], environment))
            eventually(lambda: urllib.request.urlopen(f"http://127.0.0.1:{health_port}/readyz", timeout=2).status == 200,
                       "worker readiness")
        identities = [dict(SCOPE, id="acceptance-admin", token=self.user_token, roles=["admin"], namespaces=["beta"], scopes=["north", "south", "load"]),
                      dict(SCOPE, id="acceptance-viewer", token=self.viewer_token, roles=["viewer"], namespaces=["beta"], scopes=["north"])]
        workers = [dict(address=address, processor_id="atheros.dedup", processor_version="0.1.0",
                        implementation_digest=implementation, worker_service="acceptance-dedup") for address in self.worker_addresses]
        self.manager_urls = []
        for index in range(2):
            address, remote = port(), port()
            self.manager_urls.append(f"http://127.0.0.1:{address}")
            environment = dict(common, DQ_CONTROL_URL=self.octopus_url, DQ_CONTROL_TOKEN=self.control_token,
                               DQ_INSTALLATION_ID=SCOPE["installation_id"], DQ_NAMESPACE=SCOPE["namespace"],
                               DQ_HTTP_ADDR=f"127.0.0.1:{address}", DQ_REMOTE_HTTP_ADDR=f"127.0.0.1:{remote}",
                               DQ_IDENTITIES_JSON=json.dumps(identities), DQ_WORKERS_JSON=json.dumps(workers),
                               DQ_BETA_ENABLED="true", DQ_POLL_INTERVAL="100ms", DQ_DISPATCH_LIMIT="32",
                               DQ_CONSUMER_GROUP="dq-acceptance-manager-v1")
            self.managers.append(self.launch(f"manager-{index}", [str(self.directory / "manager")], environment))
            eventually(lambda: self.public("/processors" + QUERY, manager=index).get("items"), "manager readiness")
        eventually(lambda: self.sql("SELECT count(*) FROM octopus_core.dq_workers WHERE ready") == "2", "two worker heartbeats")

    def provision(self):
        self.sql("CREATE EXTENSION vector")
        self.manifests = {}
        for domain in ("octopus_core", "atheros_search"):
            directory = ROOT / "sql/postgres" / domain
            manifest = yaml.safe_load((directory / "manifest.yaml").read_text())
            checksum = hashlib.sha256()
            for relative in manifest["apply_order"]:
                data = (directory / relative).read_bytes()
                checksum.update(relative.encode() + b"\0" + data + b"\0")
                self.sql(data.decode())
            check(checksum.hexdigest() == manifest["manifest_sha256"], f"canonical {domain} checksum")
            self.manifests[domain] = manifest
            version, sha = manifest["schema_version"], manifest["manifest_sha256"]
            self.sql(f"UPDATE {domain}.schema_readiness SET required_version='{version}',applied_version='{version}',"
                     f"required_checksum='{sha}',applied_checksum='{sha}',ready=true")
        self.sql(f"CREATE ROLE dq_octopus_test LOGIN PASSWORD '{self.runtime_password}'; "
                 f"CREATE ROLE dq_search_test LOGIN PASSWORD '{self.search_password}'; "
                 "ALTER ROLE dq_octopus_test SET search_path=octopus_core,public; "
                 "ALTER ROLE dq_search_test SET search_path=atheros_search,public; "
                 "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        for domain in self.manifests:
            template = (ROOT / "sql/postgres" / domain / "grants/least_privilege.sql.tmpl").read_text()
            self.sql(template.replace("{{OCTOPUS_ACCOUNT}}", "dq_octopus_test")
                     .replace("{{ATHEROS_SEARCH_ACCOUNT}}", "dq_search_test"))
        check(self.sql("SELECT ready AND required_checksum=applied_checksum FROM octopus_core.schema_readiness",
                       "dq_octopus_test", self.runtime_password) == "t", "real runtime role readiness")
        check(self.sql("SELECT has_schema_privilege(current_user,'octopus_core','CREATE')",
                       "dq_octopus_test", self.runtime_password) == "f", "runtime role cannot apply DDL")
        self.scenarios.append("canonical_schema_checksums_and_runtime_grants")

    def make_manifest(self, implementation):
        schema = json.loads((SERVICE / "schemas/atheros.dedup.parameters.v1.json").read_text())
        def binding(name, direction, suffix, schema_id):
            return {"name": name, "direction": direction, "transport": "minio", "bucket": "dq-artifacts",
                    "allowed_prefix": f"{SCOPE['installation_id']}/beta/{suffix}", "schema_id": schema_id,
                    "required": True, "max_items": 1, "max_bytes": 8 * 1024 * 1024, "semantics": "artifact_reference"}
        manifest = dict(SCOPE, processor_id="atheros.dedup", version="0.1.0", owner_team="wireless",
                        implementation_digest=implementation, worker_service="acceptance-dedup", protocol_version="dq.v1",
                        parameter_schema=schema, parameter_schema_sha256=digest(schema),
                        inputs=[binding("snapshot", "input", "inputs", "atheros.dedup.snapshot.v1")],
                        outputs=[binding("proposal", "output", "runs", "atheros.dedup.proposal.v1")],
                        resources={"concurrency": 2, "timeout_seconds": 45, "max_input_bytes": 8 * 1024 * 1024,
                                   "max_output_bytes": 8 * 1024 * 1024}, policy_version=POLICY)
        manifest["manifest_sha256"] = digest(manifest)
        return manifest

    def fixture(self, scope, count, start):
        statements = []
        for pair in range(start, start + count):
            left, right, identity = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
            macs = [f"02:00:{(number >> 24) & 255:02x}:{(number >> 16) & 255:02x}:{(number >> 8) & 255:02x}:{number & 255:02x}"
                    for number in (pair * 2, pair * 2 + 1)]
            for document, mac in zip((left, right), macs):
                statements.append(f"INSERT INTO atheros_search.devices(mac,location_id,first_seen,last_seen) "
                                  f"VALUES('{mac}','{scope}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP);")
                statements.append(f"INSERT INTO atheros_search.search_documents(document_id,source_kind,source_id,source_key,source_table,source_mac,"
                                  f"location_id,normalized_text,normalized_sha256,search_vector) VALUES('{document}','device',"
                                  f"'{mac}','{mac}','devices','{mac}','{scope}','synthetic','{'0' * 64}',to_tsvector('synthetic'));")
            statements.append(f"INSERT INTO atheros_search.similarity_pairs(pair_id,pair_kind,embedding_model,embedding_kind,"
                              "left_document_id,right_document_id,left_source_table,left_source_key,left_source_mac,"
                              "right_source_table,right_source_key,right_source_mac,cosine_distance,cosine_similarity,evidence,"
                              f"computed_at,projection_run_id) VALUES('{identity}','device_device','synthetic','device','{left}',"
                              f"'{right}','devices','{macs[0]}','{macs[0]}','devices','{macs[1]}','{macs[1]}',0.01,0.99,'{{}}',"
                              f"CURRENT_TIMESTAMP,'{uuid.uuid4()}');")
        for index in range(0, len(statements), 200):
            self.sql("\n".join(statements[index:index + 200]))

    def exercise(self):
        print("Exercising shadow/enabled runs and admission identity", flush=True)
        request, first = self.admit("duplicate-invocation", maximum=1000)
        duplicate = self.public("/runs", request, expected=202, manager=1)
        check(first["run_id"] == duplicate["run_id"], "duplicate admission resolves identical durable run")
        changed = json.loads(json.dumps(request))
        changed["parameters"]["max_candidates"] = 999
        self.public("/runs", changed, expected=409)
        self.public("/runs", request, token=self.viewer_token, expected=403)
        forbidden = dict(request, installation_id="other-installation")
        self.public("/runs", forbidden, expected=403)
        admission_started = time.perf_counter()
        for index in range(self.args.samples):
            self.admit(f"latency-{index}")
        self.sample_admission_seconds = time.perf_counter() - admission_started
        shadow = self.terminal(first["run_id"])
        check(shadow["execution_mode"] == "shadow" and not shadow["projection_applied"], "shadow success does not apply")
        check(self.sql("SELECT count(*) FROM atheros_search.merge_candidates c JOIN atheros_search.devices d ON d.mac=c.mac_a WHERE d.location_id='north'") == "0",
              "shadow does not mutate candidates")
        bytes_verified = [self.probe("read-artifact", ref) for ref in shadow["input_refs"] + shadow["output_refs"]]
        check(bytes_verified[0]["size_bytes"] > 65536, "large input crosses broker record limit through MinIO")
        check(bytes_verified[1]["size_bytes"] > 65536, "large proposal crosses broker record limit through MinIO")
        for run_id in list(self.runs):
            self.terminal(run_id)
        _, enabled = self.admit("enabled-explicit", "south")
        enabled = self.terminal(enabled["run_id"])
        check(enabled["projection_applied"], "enabled completion proves projection commit")
        eventually(lambda: self.sql("SELECT count(*) FROM atheros_search.merge_candidates c JOIN atheros_search.devices d ON d.mac=c.mac_a WHERE d.location_id='south'") == "2",
                   "enabled candidate application")
        self.scenarios.extend(["two_managers_two_workers", "duplicate_invocation_and_conflict", "scope_and_viewer_authorization",
                               "shadow_comparison_without_projection", "enabled_projection_application", "large_exact_version_artifact_integrity"])
        self.streaming = bytes_verified
        artifact = enabled["output_refs"][0]
        access = self.public("/artifacts/" + artifact["artifact_id"] + "/access", SCOPE)
        self.public("/artifacts/" + artifact["artifact_id"] + "/access", SCOPE,
                    token=self.viewer_token, expected=403)
        signed = urllib.parse.urlparse(access["url"])
        signed_query = urllib.parse.parse_qs(signed.query)
        check(signed_query.get("versionId") == [artifact["version_id"]], "signed artifact access pins exact version")
        check(int(signed_query["X-Amz-Expires"][0]) <= 60, "signed artifact access expires within one minute")
        with urllib.request.urlopen(access["url"], timeout=10) as response:
            body = response.read(artifact["size_bytes"] + 1)
        check(len(body) == artifact["size_bytes"] and hashlib.sha256(body).hexdigest() == artifact["sha256"],
              "authorized signed URL returns exact artifact bytes")
        self.scenarios.append("scoped_short_lived_exact_version_artifact_access")
        altered = dict(artifact, sha256="0" * 64)
        try:
            self.probe("read-artifact", altered)
        except RuntimeError as error:
            check("artifact byte integrity mismatch" in str(error), "altered artifact checksum is rejected")
        else:
            raise AssertionError("altered artifact checksum unexpectedly accepted")
        try:
            self.probe("health", token="invalid-local-worker-token")
        except RuntimeError as error:
            check("Unauthenticated" in str(error), "worker rejects unauthorized gRPC client")
        else:
            raise AssertionError("worker unexpectedly accepted invalid credential")
        self.scenarios.extend(["artifact_checksum_tampering_rejected", "worker_grpc_authorization"])
        self.recovery_scenarios()
        # Verify actual expiry, not merely the advertised URL duration.
        expires = datetime.fromisoformat(access["expires_at"].replace("Z", "+00:00")).timestamp()
        while (remaining := expires + 2 - datetime.now(timezone.utc).timestamp()) > 0:
            time.sleep(min(remaining, 5))
        try:
            urllib.request.urlopen(access["url"], timeout=10).close()
        except urllib.error.HTTPError as error:
            check(error.code == 403, "expired artifact access must be forbidden by object storage")
            error.close()
        else:
            raise AssertionError("expired artifact access unexpectedly succeeded")
        self.scenarios.append("artifact_signed_access_expires_at_storage")

    def prepared(self, key):
        _, run = self.admit(key, internal=True)
        return eventually(lambda: (lambda value: value if value["input_refs"] else None)(self.status(run["run_id"])), "prepared input")

    def recovery_scenarios(self):
        print("Exercising decision split, redelivery, stale fences and cancellation", flush=True)
        self.pause_managers()
        candidate = self.sql("SELECT candidate_id FROM atheros_search.merge_candidates ORDER BY candidate_id LIMIT 1")
        self.sql(f"BEGIN; INSERT INTO atheros_search.merge_decisions(candidate_id,decision,decided_by) VALUES('{candidate}','merge','synthetic-reviewer'); "
                 f"UPDATE atheros_search.merge_candidates SET status='approved',updated_at=CURRENT_TIMESTAMP WHERE candidate_id='{candidate}'; COMMIT;",
                 "dq_search_test", self.search_password)
        check(self.sql(f"SELECT status FROM atheros_search.merge_candidates WHERE candidate_id='{candidate}'") == "approved",
              "decision recording precedes projection application")
        reconciliation_id = eventually(lambda: self.sql(f"SELECT run_id FROM octopus_core.dq_decision_sources WHERE candidate_id='{candidate}' LIMIT 1"),
                                       "durable decision discovery without manager")
        self.resume_managers()
        decision = self.terminal(reconciliation_id)
        check(decision["projection_applied"], "recorded decision eventually projected")
        check(self.sql(f"SELECT status FROM atheros_search.merge_candidates WHERE candidate_id='{candidate}'") == "confirmed",
              "explicit human confirmation remains authoritative")
        check(self.sql(f"SELECT confirmation_source FROM atheros_search.merge_candidates WHERE candidate_id='{candidate}'") == "human",
              "human decision owns confirmed identity edge provenance")
        check(self.sql(f"SELECT count(*) FROM atheros_search.merge_candidates candidate "
                       "JOIN atheros_search.identity_cluster_members left_member ON left_member.mac=candidate.mac_a "
                       "JOIN atheros_search.identity_cluster_members right_member ON right_member.mac=candidate.mac_b "
                       "AND right_member.cluster_id=left_member.cluster_id "
                       "JOIN atheros_search.identity_clusters cluster ON cluster.cluster_id=left_member.cluster_id "
                       f"WHERE candidate.candidate_id='{candidate}' AND cluster.status='active' AND cluster.cluster_size=2") == "1",
              "confirmed exact MAC pair belongs to one active canonical identity cluster with both memberships")
        self.scenarios.append("decision_recorded_then_completed_and_projection_applied")
        self.scenarios.append("human_confirmed_identity_edge_and_canonical_cluster_membership")
        self.pause_managers()
        run = self.prepared("lost-ack-and-source-change")
        health = self.probe("health")
        reservation_body = dict(SCOPE, worker_instance_id=health["worker_instance_id"], worker_slot=0)
        reserved = self.internal(f"/runs/{run['run_id']}/attempts/reserve", reservation_body)
        repeated = self.internal(f"/runs/{run['run_id']}/attempts/reserve", reservation_body)
        check(reserved["attempt"] == repeated["attempt"], "lost reservation acknowledgment retains exact attempt")
        a, r = reserved["attempt"], reserved["run"]
        invocation = {key: r[key] for key in ("installation_id", "namespace", "run_id", "processor_id", "processor_version", "implementation_digest",
                                             "operation", "parameters", "input_refs", "policy_version", "source_revision_digest", "deadline_at")}
        invocation.update({key: a[key] for key in ("attempt_id", "fence", "worker_instance_id", "worker_slot", "lease_expires_at")})
        before_digest = r["source_revision_digest"]
        self.sql("UPDATE atheros_search.search_documents SET normalized_text='changed-synthetic' WHERE location_id='north'")
        first = self.probe("run", invocation)
        repeated = self.probe("run", invocation)
        check(first["attempt_id"] == repeated["attempt_id"] and first["fence"] == repeated["fence"], "worker redelivery returns same execution")
        self.resume_managers()
        new = self.terminal(r["run_id"], success=False)
        check(new["source_revision_digest"] != before_digest or new.get("error_code") == "source_revision_changed",
              "changed source cannot apply stale output")
        check(self.sql(f"SELECT count(*) FROM octopus_core.dq_attempts WHERE run_id='{r['run_id']}' AND attempt_id='{a['attempt_id']}'") == "1",
              "duplicate worker request retains one durable attempt")
        lease = dict(SCOPE, run_id=r["run_id"], worker_instance_id=a["worker_instance_id"], lease_token=a["lease_token"], fence=a["fence"])
        self.internal(f"/attempts/{a['attempt_id']}/renew", lease, expected=409)
        self.scenarios.extend(["lost_reservation_ack_replays_same_attempt", "worker_run_redelivery", "source_revision_change_fences_output", "stale_lease_rejected"])
        self.pause_managers()
        cancelled = self.prepared("cancel-before-dispatch")
        self.internal(f"/runs/{cancelled['run_id']}/cancel", SCOPE, actor=True)
        self.resume_managers()
        check(self.terminal(cancelled["run_id"], success=False)["status"] == "cancelled", "cancelled work remains cancelled")
        self.scenarios.append("cancellation_fences_prepared_work")
        produced = self.probe("produce", {"topic": "dq.worker-reports.v1", "key": "synthetic-malformed", "payload": "{bad json"})
        where = f"topic='dq.worker-reports.v1' AND partition_id={produced['partition']} AND offset_id={produced['offset']}"
        eventually(lambda: self.sql(f"SELECT count(*) FROM octopus_core.dq_receipts WHERE {where} AND quarantine_reason IS NOT NULL") != "0",
                   "malformed broker record durably quarantined")
        self.scenarios.append("malformed_record_durable_quarantine")
        self.worker_failure()

    def worker_failure(self):
        self.pause_managers()
        run = self.prepared("expired-worker")
        health = self.probe("health")
        reservation = self.internal(f"/runs/{run['run_id']}/attempts/reserve",
                                    dict(SCOPE, worker_instance_id=health["worker_instance_id"], worker_slot=0))
        attempt = reservation["attempt"]
        self.workers[0].terminate()
        self.workers[0].wait(timeout=10)
        # Only the isolated provisioning account performs deliberate fault injection.
        self.sql(f"UPDATE octopus_core.dq_attempts SET lease_expires_at=CURRENT_TIMESTAMP-INTERVAL '1 second' WHERE attempt_id='{attempt['attempt_id']}'")
        eventually(lambda: self.sql(f"SELECT status FROM octopus_core.dq_attempts WHERE attempt_id='{attempt['attempt_id']}'") == "expired",
                   "dead worker attempt expired and fenced")
        self.resume_managers()
        # A process paused inside a tick may resume with the old worker health
        # and acquire one more reservation before refreshing it. That attempt
        # must expire under the real lease; it cannot report an accepted result.
        completed = self.terminal(run["run_id"], timeout=150)
        check(completed["current_attempt"]["worker_instance_id"] != health["worker_instance_id"], "remaining live worker recovers run")
        self.scenarios.append("worker_termination_expired_lease_recovery")
        self.managers[0].terminate()
        self.managers[0].wait(timeout=10)
        _, run = self.admit("surviving-manager", internal=True)
        self.terminal(run["run_id"])
        self.scenarios.append("manager_termination_surviving_replica")
        self.broker_failure()

    def broker_failure(self):
        # This pause affects only the container created by this harness.
        container = self.redpanda.get_wrapped_container()
        container.pause()
        try:
            _, run = self.admit("broker-outage", internal=True)
            eventually(lambda: self.sql("SELECT count(*) FROM octopus_core.dq_outbox "
                                        f"WHERE payload->>'run_id'='{run['run_id']}' AND status<>'published'") != "0",
                       "durable unpublished outbox during broker outage")
            check(self.status(run["run_id"])["status"] != "succeeded", "broker outage cannot manufacture success")
        finally:
            container.unpause()
        self.terminal(run["run_id"], timeout=90)
        eventually(lambda: self.sql("SELECT count(*) FROM octopus_core.dq_outbox "
                                    f"WHERE payload->>'run_id'='{run['run_id']}' AND status<>'published'") == "0",
                   "durable outbox drained after broker recovery")
        self.scenarios.append("broker_pause_durable_outbox_and_recovery")
        self.storage_failure()

    def storage_failure(self):
        print("Exercising object storage outage, bounded timeout and recovery", flush=True)
        def readiness():
            try:
                with urllib.request.urlopen(self.octopus_url + "/ready", timeout=3) as response:
                    return response.status
            except urllib.error.HTTPError as error:
                status = error.code
                error.close()
                return status
        eventually(lambda: readiness() == 200, "healthy coordinator before storage failure")
        container = self.minio.get_wrapped_container()
        container.pause()
        try:
            _, run = self.admit("storage-outage", internal=True)
            eventually(lambda: readiness() == 503, "bounded storage timeout makes coordinator unready", 45)
            failed = self.status(run["run_id"])
            # Bucket preflight may fail before preparation claims the run;
            # either path keeps its durable admission available for recovery.
            check(failed["status"] in {"queued", "preparing", "retrying"} and not failed["input_refs"],
                  "storage outage retains retryable work without publishing an input reference")
            # Status remains available while a storage retry is waiting. This
            # exercises the actual control path rather than a mocked client.
            started = time.perf_counter()
            self.status(run["run_id"])
            check(time.perf_counter() - started < 5, "storage outage does not block control queries")
        finally:
            container.unpause()
        self.terminal(run["run_id"], timeout=90)
        self.scenarios.append("storage_pause_bounded_timeout_and_durable_recovery")

    def admission_load(self):
        # The isolated load scope has no source rows. Requests still traverse
        # the real manager authentication, schema checks and durable admission.
        # These bounded waves measure capacity; they do not assert a target.
        print("Measuring bounded 1,000 and 10,000 admission waves", flush=True)
        check(all(process.poll() is None for process in self.managers + self.workers),
              "load measurement requires two healthy managers and workers")

        def submit(index, prefix):
            request = dict(SCOPE, processor_id="atheros.dedup", operation="prepare_candidates",
                           parameters={"authorized_scope": "load", "policy_version": POLICY, "max_candidates": 1},
                           idempotency_key=prefix + str(index))
            headers = {"Authorization": "Bearer " + self.user_token, "Content-Type": "application/json"}
            req = urllib.request.Request(self.manager_urls[index % 2] + "/v1/runs", data=canonical(request), headers=headers)
            started = time.perf_counter()
            try:
                response = urllib.request.urlopen(req, timeout=15)
            except urllib.error.HTTPError as error:
                response = error
            except (urllib.error.URLError, TimeoutError):
                return {"status": "transport_error", "elapsed_ms": (time.perf_counter() - started) * 1000}
            with response:
                payload = response.read(65537)
                outcome = {"status": response.status, "elapsed_ms": (time.perf_counter() - started) * 1000}
                if response.status == 202:
                    outcome["run_id"] = json.loads(payload)["run_id"]
                return outcome

        def cancel(run_id):
            self.public("/runs/" + run_id + "/cancel", SCOPE)

        for count in (1000, 10000):
            prefix = f"load-{count}-"
            started = time.perf_counter()
            deadline = started + self.args.load_window_seconds
            outcomes, submitted = [], 0
            with ThreadPoolExecutor(max_workers=self.args.load_concurrency) as pool:
                pending = set()
                while submitted < count or pending:
                    while (submitted < count and len(pending) < self.args.load_concurrency
                           and time.perf_counter() < deadline):
                        pending.add(pool.submit(submit, submitted, prefix))
                        submitted += 1
                    if not pending:
                        break
                    completed, pending = wait(pending, return_when=FIRST_COMPLETED)
                    outcomes.extend(future.result() for future in completed)
            elapsed = time.perf_counter() - started
            accepted = [value for value in outcomes if value["status"] == 202]
            statuses = {}
            for value in outcomes:
                code = str(value["status"])
                statuses[code] = statuses.get(code, 0) + 1
            timings = sorted(value["elapsed_ms"] for value in accepted)
            self.load_results.append({"requested_requests": count, "sent_requests": submitted,
                                      "client_backlog_cancelled": count - submitted,
                                      "arrival_window_seconds": self.args.load_window_seconds,
                                      "accepted_requests": len(accepted), "manager_replicas": 2, "worker_replicas": 2,
                                      "client_concurrency": self.args.load_concurrency,
                                      "elapsed_seconds": round(elapsed, 3), "response_counts": statuses,
                                      "accepted_requests_per_second": round(len(accepted) / elapsed, 3),
                                      "accepted_p99_ms": round(timings[max(0, math.ceil(len(timings) * 0.99) - 1)], 3) if timings else None,
                                      "target_certified": False})
            # Cancellation uses the public API. The containing Testcontainers
            # are also destroyed, including all cancelled evidence and objects.
            with ThreadPoolExecutor(max_workers=self.args.load_concurrency) as pool:
                list(pool.map(cancel, (value["run_id"] for value in accepted)))
            self.load_results[-1]["cancelled_requests"] = len(accepted)
            self.load_results[-1]["pending_outbox_after_cancellation"] = int(self.sql(
                "SELECT count(*) FROM octopus_core.dq_outbox WHERE status<>'published'"))
            print(f"Admission wave {count}: sent={submitted}, accepted={len(accepted)}, "
                  f"unsent={count - submitted}, elapsed={elapsed:.3f}s", flush=True)
        self.scenarios.append("bounded_admission_load_and_public_cancellation_cleanup")

    def rollback(self):
        print("Exercising disabled-beta startup rollback and legacy scope restoration", flush=True)
        self.pause_managers()
        pending = self.prepared("rollback-reserved-work")
        alive = next(index for index, process in enumerate(self.workers) if process.poll() is None)
        health = self.probe("health", address=self.worker_addresses[alive])
        reservation = self.internal(f"/runs/{pending['run_id']}/attempts/reserve",
                                    dict(SCOPE, worker_instance_id=health["worker_instance_id"], worker_slot=0))
        attempt = reservation["attempt"]
        self.octopus.terminate()
        try:
            self.octopus.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.octopus.kill()
            self.octopus.wait(timeout=5)
        environment = dict(self.octopus_env, OCTOPUS_DQ_ENABLED="false")
        self.octopus = self.launch("octopus-rollback", ["java", "-XX:ActiveProcessorCount=4", "-Xmx768m", "-jar", str(self.jar)], environment)
        eventually(lambda: self.sql("SELECT count(*) FROM octopus_core.dq_scope_policies WHERE mode<>'disabled'") == "0",
                   "persisted beta scope policies disabled at startup")
        check(self.sql(f"SELECT status FROM octopus_core.dq_runs WHERE run_id='{pending['run_id']}'") == "cancelled",
              "rollback cancels outstanding beta work")
        check(self.sql(f"SELECT status FROM octopus_core.dq_attempts WHERE attempt_id='{attempt['attempt_id']}'") == "cancelled",
              "rollback fences the reserved worker attempt")
        def control_removed():
            request = urllib.request.Request(self.octopus_url + "/internal/dq/v1/processors" + QUERY,
                                             headers={"Authorization": "Bearer " + self.control_token})
            try:
                urllib.request.urlopen(request, timeout=5).close()
                return False
            except urllib.error.HTTPError as error:
                removed = error.code == 404
                error.close()
                return removed
        eventually(control_removed, "disabled beta control routes removed")
        # Legacy processors remain disabled in this test. Their persisted scope
        # exclusion predicate is nevertheless released by the disabled policy.
        check(self.sql("SELECT NOT EXISTS(SELECT 1 FROM octopus_core.dq_scope_policies WHERE mode='enabled')") == "t",
              "legacy scope gate is restored")
        self.scenarios.append("disabled_beta_restart_fences_work_and_restores_legacy_scope")

    def failure_logs(self):
        if not hasattr(self, "pg"):
            return
        try:
            raw = self.pg.get_wrapped_container().logs(tail=1000).decode(errors="replace")
            for value in (self.control_token, self.worker_token, self.user_token, self.viewer_token,
                          self.storage_access, self.storage_secret, self.database_password,
                          self.runtime_password, self.search_password):
                raw = raw.replace(value, "[redacted]")
            (self.directory / "postgres-failure.log").write_text(raw)
        except Exception:
            pass

    def result(self):
        dispatch = self.query_json("SELECT coalesce(json_agg(ms), '[]') FROM (SELECT extract(epoch FROM "
                                   "(a.started_at - x.received_at))*1000 AS ms FROM octopus_core.dq_attempts a "
                                   "JOIN LATERAL (SELECT min(received_at) received_at FROM octopus_core.dq_receipts "
                                   "WHERE envelope->>'run_id'=a.run_id AND envelope->>'event_type'='invoke') x ON true "
                                   "WHERE a.started_at IS NOT NULL AND x.received_at IS NOT NULL "
                                   "AND a.started_at>=x.received_at) samples")
        def p99(values):
            check(values, "latency samples are required")
            return round(sorted(float(value) for value in values)[max(0, math.ceil(len(values) * 0.99) - 1)], 3)
        check(dispatch, "actual eligible-receipt to worker-start dispatch samples required")
        return {"status": "passed", "scenarios": self.scenarios, "manager_replicas": 2, "worker_replicas": 2,
                "octopus_assembly_sha256": self.octopus_assembly_sha256,
                "implementation_digest": self.implementation, "canonical_manifests": {name: value["manifest_sha256"] for name, value in self.manifests.items()},
                "admission": {"samples": len(self.admission_ms), "p99_ms": p99(self.admission_ms),
                              "sequential_requests_per_second": round(self.args.samples / self.sample_admission_seconds, 3)},
                "dispatch_receipt_to_worker_started": {"samples": len(dispatch), "p99_ms": p99(dispatch)},
                "streamed_artifacts": self.streaming, "performance_target_certified": False,
                "bounded_admission_load": self.load_results,
                "limitations": ["Local synthetic acceptance is not sustained 1000 requests per second certification.",
                                "Dispatch includes configured polling and contention; production warm free-capacity target needs a dedicated load environment.",
                                "Load waves use two healthy managers and workers, bounded client concurrency and scope admission quotas; unsent client backlog is reported separately.",
                                "Manager processing overhead excluding dependencies and sustained production artifact streaming are not isolated by this harness.",
                                "Local transport is explicitly plaintext development; production mTLS and broker/object ACL deployment remain prerequisites.",
                                "Atheros decision recording uses its exact runtime role and canonical table contract; browser and HTTP integration have separate tests."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--octopus-jar", type=Path, default=ROOT / "services/octopus/target/scala-3.3.8/octopus.jar")
    parser.add_argument("--result", type=Path, default=Path("/tmp/dq-acceptance-result.json"))
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--load", action="store_true", help="measure bounded admission waves in a second fresh stack after recovery checks")
    parser.add_argument("--load-concurrency", type=int, default=32)
    parser.add_argument("--load-window-seconds", type=int, default=60)
    parser.add_argument("--postgres-image", default="pgvector/pgvector:0.8.6-pg16-bookworm")
    parser.add_argument("--redpanda-image", default="redpandadata/redpanda:v24.2.7@sha256:82a69763bef8d8b55ea5a520fa1b38f993908ef68946819ca1aed43541824c48")
    parser.add_argument("--minio-image", default="elestio/minio@sha256:25348a257f1ece1b192f25f6cd9854618fa86422ac87b494b5d4e629c556d4bd")
    args = parser.parse_args()
    check(args.samples >= 1, "at least one admission sample required")
    check(1 <= args.load_concurrency <= 256, "load concurrency must be 1..256")
    check(1 <= args.load_window_seconds <= 60, "load arrival window must be 1..60 seconds")
    directory = Path(tempfile.mkdtemp(prefix="dq-acceptance-"))
    stack = Stack(args, directory)
    completed_scenarios = []
    started = time.monotonic()
    result = {"status": "interrupted", "completed_scenarios": stack.scenarios,
              "logs": str(directory), "performance_target_certified": False}
    try:
        stack.setup()
        stack.exercise()
        stack.rollback()
        result = stack.result()
        if args.load:
            # Performance backlog must not contaminate the bounded recovery
            # checks or their latency samples. Keep its actual backlog visible
            # in the load result instead of waiting indefinitely for it to drain.
            stack.close()
            load_directory = directory / "load"
            load_directory.mkdir()
            load_stack = Stack(args, load_directory)
            completed_scenarios = stack.scenarios
            stack = load_stack
            stack.setup()
            stack.admission_load()
            result["bounded_admission_load"] = stack.load_results
            result["load_environment"] = "separate freshly provisioned two-manager/two-worker stack"
            result["limitations"].append("Load outbox backlog is reported after public cancellation; draining that backlog within a production recovery budget is not certified.")
        result["scenarios"] = completed_scenarios + stack.scenarios
    except Exception as error:
        stack.failure_logs()
        result = {"status": "failed", "error": str(error), "completed_scenarios": completed_scenarios + stack.scenarios,
                  "logs": str(directory), "bounded_admission_load": stack.load_results,
                  "performance_target_certified": False}
        raise
    finally:
        stack.close()
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        result["logs"] = str(directory)
        args.result.write_text(json.dumps(result, indent=2) + "\n")
        print(f"Acceptance {result['status']}; results: {args.result}; logs: {directory}", flush=True)


if __name__ == "__main__":
    main()
