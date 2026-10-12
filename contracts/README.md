# DQ v1 wire contract

The Go manager and isolated workers use this contract with Octopus, which owns
all durable state. All JSON fields use snake_case; times use RFC3339 UTC.
Control records are limited to 65536 bytes and contain artifact references only.

## Control API

Internal routes start at `/internal/dq/v1`. Public manager routes start at
`/v1`. Every internal request requires service authentication. Reads use
`installation_id` and `namespace` query parameters; mutations carry them in
their JSON body. Authorization and beta policy are checked by Octopus.

- `GET /processors`: processor catalog.
- `POST /processor-versions`: immutable approved registration.
- `GET /processor-versions/{processor_id}/{version}`: exact manifest.
- `POST /runs`: admission; returns the durable run with HTTP 202.
- `GET /runs/{run_id}`: authoritative status.
- `POST /runs/{run_id}/cancel`: cancellation and fencing.
- `POST /runs/{run_id}/replay`: explicit audited new run.
- `POST /receipts`: persist valid or quarantined broker receipt.
- `POST /workers/{worker_instance_id}/heartbeat`: readiness and credits.
- `GET /dispatches`: durable received work and unaccepted reserved attempts.
- `POST /runs/{run_id}/attempts/reserve`: atomic worker and slot reservation.
- `POST /attempts/{attempt_id}/accepted`: record dispatch acknowledgement.
- `POST /attempts/{attempt_id}/renew`: renew an exact fenced lease.

## Shared data

`ArtifactRef`: artifact_id, installation_id, namespace, uri, version_id, etag,
sha256, schema_id, media_type, size_bytes, producer_run_id?, producer_attempt_id?.

`Manifest`: installation_id, namespace, processor_id, version, owner_team,
implementation_digest, worker_service, protocol_version, manifest_sha256,
parameter_schema (JSON Schema 2020-12 object), parameter_schema_sha256,
inputs[], outputs[], resources, policy_version, ui_remote?. Resources contain
concurrency, timeout_seconds, max_input_bytes, max_output_bytes. Bindings contain
name, direction, transport, topic?, bucket?, allowed_prefix?, schema_id,
required, max_items, max_bytes, semantics.

`AdmitRequest`: installation_id, namespace, processor_id, processor_version?,
operation, parameters, idempotency_key, deadline_at?, traceparent?, tracestate?.
Execution mode is derived from scope policy, never caller-supplied.

`Run`: admission identity, run_id, pinned processor_version,
implementation_digest, parameters_sha256, policy_version, input_refs[],
output_refs[], source_revision_digest, release_channel, execution_mode, status,
state_revision, current_attempt?, created_at, deadline_at, completed_at?,
projection_applied, error_code?, sanitized_error?.

`Attempt`: attempt_id, run_id, attempt_number, worker_instance_id, worker_slot,
lease_token, fence, lease_expires_at, status, started_at?, finished_at?.
Reserve body adds worker_instance_id and worker_slot to the installation and
namespace. Reserve response is `{run, attempt, manifest}`. Dispatch response is
`{items: [{run, manifest, attempt?}]}`. Repeated reservations return the same
current attempt, including the reserved worker.

Heartbeat body: installation_id, namespace, processor_id, processor_version,
implementation_digest, worker_instance_id, address, concurrency, ready.
Lease mutation: installation_id, namespace, run_id, worker_instance_id,
lease_token, fence.

Receipt body: installation_id, namespace, consumer_group, topic, partition,
offset, event_id, payload_sha256, envelope?, quarantine_reason?. Envelope:
schema_version, event_id, event_type, installation_id, namespace, run_id,
attempt_id?, processor_id, processor_version, fence?, state_revision?,
occurred_at, correlation_id, causation_id?, traceparent?, tracestate?,
parameters?, artifacts[], source_revision_digest?, sequence?, summary?,
error_code?, error_class?.

## Dedup processor

`atheros.dedup` version `0.1.0` supports `prepare_candidates` and
`reconcile_decision`. Parameters: authorized_scope, snapshot_revision?,
policy_version, max_candidates (1..1000), candidate_id?. Scope is `*` or one
authorized location; scoped pairs require both endpoints in that location.

Snapshot schema `atheros.dedup.snapshot.v1`: installation_id, namespace,
authorized_scope, operation, source_revision_digest, policy_version, pairs[],
devices[], candidates[], decisions[], candidate_id?. Pairs contain pair_id,
mac_a, mac_b, confidence, left_revision, right_revision, pair_revision. Devices
contain mac, registered, registered_device_id, revision. Candidates contain
candidate_id, mac_a, mac_b, confidence, status, revision. Decisions contain
candidate_id, decision, decided_at.

Proposal schema `atheros.dedup.proposal.v1`: run_id, attempt_id, fence,
operation, source_revision_digest, policy_version, candidates[] containing
mac_a, mac_b, confidence, pair_id; reconciliations[] containing candidate_id,
decision; summary containing candidate_count and reconciliation_count.
Octopus validates and applies proposals using existing candidate IDs and
confirmation constraints. Workers never confirm identities themselves.

Lifecycle gRPC calls are Init, Run, Cancel, Health. Run returns promptly with
attempt_id, fence, and status; computation and broker reporting are asynchronous.
Immutable repeated attempt requests return the cached status. Workers use
boot-specific identities; restarted workers cannot accept a previous boot's
reserved attempt. Output-ready reports contain one proposal reference.
