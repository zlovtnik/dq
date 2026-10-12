# Local acceptance

The [acceptance harness](acceptance/run.py) starts two manager processes, two
worker processes, a freshly assembled [Octopus](../../octopus/README.md), and
isolated PostgreSQL, Redpanda, and MinIO containers. It provisions the canonical
schemas and least privilege runtime grants. No existing database is accepted.

From the repository root, install [test requirements](../../../scripts/requirements-test.txt)
into a temporary Python environment, rebuild Octopus, and run the harness:

```sh
python3 -m venv /tmp/dq-acceptance-env
/tmp/dq-acceptance-env/bin/python -m pip install -r scripts/requirements-test.txt
(cd services/octopus && sbt assembly)
GOCACHE=/tmp/dq-go-cache /tmp/dq-acceptance-env/bin/python services/dq/tests/acceptance/run.py
```

Docker Desktop or another working local Docker engine is required. Missing
dependencies, stale assemblies, and failed checks cause the run to fail.
The harness builds its Go binaries and generates temporary credentials, a
synthetic manifest, an ephemeral MinIO encryption key, and synthetic fixtures.
It destroys its own containers and processes when it exits; temporary logs and
the JSON result remain available for diagnosis.

The default MinIO image is a pinned local test image. Production uses its
separately approved platform image. Local transport explicitly enables
development plaintext, while object versioning and server-side encryption
remain required. This check does not certify production TLS, credentials,
object ACLs, or network policy.

The scenarios cover duplicate admissions and conflicting retries, scope and
role authorization, shadow and enabled application, large artifacts with exact
version and checksum verification, short-lived scoped artifact access, human
decision recording followed by projection application, repeated reservations
and worker requests, source revision fences, stale leases, cancellation,
malformed record quarantine, worker and manager termination, and durable
outbox recovery after a real broker pause.

The JSON result defaults to `/tmp/dq-acceptance-result.json`. It reports admission
p99, eligible command receipt to recorded worker start p99, and verified MinIO
download throughput. These are local synthetic measurements. They do not
establish sustained 1,000 requests per second, a 10,000 requests per second
burst, the production 50 ms latency goals, or full production streaming capacity.

The separate real MinIO multipart check can be run from `services/dq`:

```sh
DQ_TEST_MINIO_IMAGE=elestio/minio@sha256:25348a257f1ece1b192f25f6cd9854618fa86422ac87b494b5d4e629c556d4bd \
GOCACHE=/tmp/dq-go-cache go test -tags=integration -run TestMinIOExactVersionEncryptedStreaming -count=1 ./internal/artifact
```

See the [service README](../README.md) for deployment prerequisites and the
[wire contract](../contracts/README.md) for control messages.
