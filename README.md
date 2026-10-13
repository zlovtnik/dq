# Delip Queve

DQ dispatches immutable processor versions through warm Go workers. Redpanda
carries control messages and MinIO carries artifact bytes. The first processor,
`atheros.dedup`, prepares candidate proposals and reconciles recorded decisions.

[Octopus](../octopus/README.md) owns the registry, durable run ledger, ingestion
receipts, leases, retry timing, outbox, and projection application.
[Atheros Search](../../apps/integration-console/atheros-search/README.md) owns
human decisions and embedding execution. DQ managers and workers have no
PostgreSQL clients or database credentials.

## Project

- `cmd/manager`: authenticated catalog, invocation, status, dispatch and shell.
- `cmd/worker`: isolated `atheros.dedup` version deployment.
- `pkg/contract`: bounded cross-language control messages.
- `ui`: SolidJS shell and independently built processor remote.
- [Wire contract](contracts/README.md): endpoints, messages and lifecycle.
- [Local acceptance](tests/acceptance.md): isolated full-stack checks and measurements.
- [Verification report](tests/verification.md): completed checks, measurements and promotion gates.
- [Container build](Dockerfile): separate manager and worker build targets.
- [Kubernetes base](../../cyber-stack/base/delip-queve): unpromoted workloads.

The manager resolves manifests through Octopus and dispatches reserved attempts
over persistent gRPC connections. It records broker receipts before offset
commits and polls durable dispatchable work independently, so a manager crash
after a commit cannot strand a run. Each attempt pins a worker process identity,
slot, token and fence. Computation can overlap after a network partition, but
Octopus accepts only the current authorized result.

Workers stream exact, versioned artifact objects through supplied clients,
verify SHA-256 and size, and publish references. Artifact ETags are not hashes.
Control records are limited to 64 KiB. Credentials and signed URLs never enter
broker records. A successful run is separate from a human review decision.

## Local development

Use Go, Bun, JDK 21 and sbt. Docker is required for the repository's ephemeral
PostgreSQL database checks. Production credentials must not be used for local
tests. Build the binaries with `make build`; verify Go with `make check`, UI
with `make ui-check`, and local benchmarks with `make benchmark`.

The manager requires scoped identities, an authenticated Octopus control URL,
approved worker addresses and broker access. Configure these with `DQ_*`
variables. `DQ_INSECURE_DEVELOPMENT=true` permits local HTTP and insecure gRPC;
use it only with synthetic fixtures on an isolated development network.
`DQ_BETA_ENABLED` defaults to false.

Octopus's `OCTOPUS_DQ_ENABLED` defaults to false. Activation also requires
installation and namespace, scope modes JSON, approved implementation digests,
policy version, control token, and versioned MinIO access. Scope modes select
`shadow` or `enabled`; omission denies admission. Shadow proposals record
comparisons without changing maintained projections. Enabled scopes require
exclusive candidate preparation and fenced application.

Atheros's additive `POST /v1/dedup/runs` and `GET /v1/dedup/runs/{run_id}` use
`ATHSEARCH_DQ_ENABLED`, manager URL, service token, installation ID, namespace
and a bounded timeout. The existing merge decision endpoint keeps its contract.
The Atheros UI's `VITE_DQ_BETA_ENABLED` controls visibility; it grants no backend
permission.

## Deployment and beta gates

Desired state is under [cyber-stack](../../cyber-stack/README.md). The DQ base
is deliberately excluded from environment slices until built images have real
approved registry digests and the prerequisites below are provisioned. Add the
promoted manager and worker to `app-stack` in a reviewed Git change; reconcile
through Argo CD. Do not fabricate image digests or deploy interactively.

Provision scoped control identities, worker mutual TLS certificates, broker
credentials and topic ACLs, and separate object-prefix permissions. Enable
bucket versioning and encryption before registration. Workers may write only
their reserved attempt prefixes. Registration must pin the implementation
digest, parameter schema checksum and optional immutable remote bundle digest.

Only the existing provisioning executor applies the append-only
[Octopus schema manifest](../../sql/postgres/octopus_core/manifest.yaml).
Runtime schema checksums must match the recorded manifest before DQ is enabled.
Run metadata and request identities retain 90 days; referenced artifacts retain
through open review/replay and for 90 days after closure. Uncommitted outputs
need a safety window and reference check before removal.

Octopus drains expired object versions in bounded batches every five minutes
and prunes metadata after object deletion. Hourly orphan scans wait seven days
and require a durable
reference check. Pending review evidence stays retained; decision-discovery
tombstones remain after run metadata expires. Its object credentials therefore
need version listing and deletion rights within the installation's artifact prefix.

The v1 coordinator prepares and applies `atheros.dedup` only; registration
rejects unsupported processor domains. Its manager, SDK and worker lifecycle
are generic, while a new projection-owning processor also needs an Octopus
snapshot/application adapter. Parameter schemas use the bounded vocabulary
described in the [wire contract](contracts/README.md).

Manager metrics include dispatch latency buckets and scoped broker observations.
Octopus samples queue age, retrying and applying runs, eligible application age,
minute admission/completion counts, expired fences, pending outbox/DLQ work and
expired artifact backlog. Kafka lag is observed per assigned partition, and
sample timestamps expose stale observations. Use the existing broker metrics
for retained topic depth; an outbox backlog is distinct from broker lag.

The [DQ configuration placeholders](../../cyber-stack/base/delip-queve/configmaps.yaml)
must be replaced by approved worker manifests and configured identities before
activation. `DQ_CONTROL_ALLOW_PLAINTEXT` defaults to false; the current Octopus
internal HTTP endpoint needs an explicit trusted-network setting or a configured
TLS endpoint. The schema executor image must include the canonical DQ additions
before the new Octopus runtime checksum is promoted. Shell and remote origins,
artifact browser access and object-store CORS also need environment configuration.

Before enabled beta, verify duplicate invocation and conflicts, redelivery,
lost acknowledgements, two managers and two workers, worker termination, lease
expiry, stale output, decision and source races, storage outages, partial
uploads, malformed records, scoped access, remote fallback and rollback.
Required stack checks include Atheros quality/fixtures/UI and mandatory database
contracts, Octopus tests, canonical schema validation, desired-state validation
and `python3 scripts/check-docs.py`.

Admission has a global queue cap and an equal reserved quota per configured
scope, preventing one scope from taking all available capacity. Repeated
idempotent requests recover their original run even when the admission quota
is full. Worker report and command offsets advance only after durable receipts.

Measure admission, dispatch, preparation, queue wait, computation, application
and artifact throughput separately. The design targets (1000 runs/sec sustained,
admission and warm dispatch p99 below 50 ms, and 100-500 MiB/sec artifact
streaming) are not evidence. Production activation requires representative
shadow comparisons and measured recovery and access-isolation results.

Rollback is a reviewed Git change: disable admission and dispatch, fence active
attempts, and restore the previous candidate preparation path. Recorded human
decisions remain durable.
