# DQ verification

> **Status: Final local acceptance in progress.** Production beta remains disabled.

DQ lives in [services/dq](../README.md). The Go manager, isolated worker, SolidJS
shell and independent remote are implemented, with durable coordination and
projection application in [Octopus](../../octopus/README.md). The additive
[Atheros Search integration](../../../apps/integration-console/atheros-search/README.md)
preserves the existing human-decision API and embedding worker ownership.

## Checks completed

Validation used local synthetic fixtures on October 12-13, 2026.

| Check | Evidence |
|---|---|
| DQ Go | Formatting, vet, race tests and both binary builds passed |
| DQ UI | 12 tests, TypeScript checks, shell build and independent remote packaging passed |
| Atheros Search | Quality checks, both lint configurations, race tests, protocol compatibility, fixtures and architecture boundaries passed |
| Atheros UI | 157 tests and production build passed |
| Required Atheros database contracts | Canonical ephemeral PostgreSQL checks passed with no skips; final run took 21.466 seconds |
| Octopus DQ | 31 tests passed with no skips, including real PostgreSQL registry, fencing, projection and retention tests |
| Full Octopus suite | 443 passed; one existing opt-in live hydration probe skipped; zero failures |
| Schema migrator | Full suite: 222 passed, zero failures |
| MinIO integration | Real encrypted, versioned input and 6 MiB multipart output passed; interrupted upload published neither a reference nor an object version |
| Repository contracts | Canonical schema validation, DQ topic tests, dependency boundaries and GitOps validation passed |
| Containers | Manager and worker Docker build targets passed; images remain local |
| Documentation | Repository documentation checker passed |

The append-only [019 retention indexes](../../../sql/postgres/octopus_core/01_tables/019_dq_retention_indexes.sql)
follow the existing 018 DQ schema without rewriting it. The final
[Octopus manifest](../../../sql/postgres/octopus_core/manifest.yaml) is version
`2026101202`, checksum
`625ab5479655353e71dfab2a6164644f1baae47be42b60ecdbdb0ec217181489`.

## Full-chain acceptance and capacity

The [acceptance procedure](acceptance.md) runs actual Octopus, two managers,
two workers, Redpanda, encrypted versioned MinIO, and canonical PostgreSQL
with least privilege runtime roles. Final results and measurements are pending.

The independent MinIO test also checks partial-upload cancellation against a
real server. Coordinator tests cover application retries with retained outputs,
current human decisions, source revisions, scope isolation and retained review
evidence. UI tests cover failed or incompatible remotes and the sandbox message
boundary.

## Limits and promotion gates

Local acceptance does not certify production capacity. The capacity probe uses
32 clients and bounded 60-second submission windows. It reports unsent client
backlog separately and does not generate the full sustained 1,000 requests per
second or 10,000-request burst. Outbox backlog after load must be included in
production recovery measurements. Manager overhead excluding dependencies and
sustained 100-500 MiB/sec streaming were not isolated by these checks.

The first supported coordinator adapter is `atheros.dedup`. Adding another
projection-owning processor requires an Octopus snapshot/application adapter.
Registration supports the bounded JSON Schema vocabulary in the
[wire contract](../contracts/README.md), rather than every JSON Schema keyword.

The [Kubernetes base](../../../cyber-stack/base/delip-queve) remains excluded
from environment application slices. Activation needs reviewed digest-pinned
images, approved manifests and identities, worker mutual TLS, broker credentials
and ACLs, object-prefix permissions, versioning/encryption, and configured shell,
remote and artifact browser origins. The schema executor image must include the
canonical DQ additions before the coordinator checksum is promoted. Representative
shadow comparisons and measured performance/recovery gates must pass first.

Local validation generated Go binaries, an Octopus assembly, UI bundles and
Docker images. Acceptance creates and removes its own containers and processes;
its temporary logs contain local test data. Production was not deployed, and
changes were not committed or pushed by this task.
