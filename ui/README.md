# DQ stable shell and immutable remote

The SolidJS/TypeScript shell uses the [DQ v1 contract](../contracts/README.md)
through the manager public API. Credentials stay in host memory. The shell
provides the processor catalog, pinned invocation schema, run inspector,
artifact metadata and authorized exact-version download, and Atheros beta view.
Loading and errors reflect manager responses; no successful runs are simulated.

## Development and checks

```sh
bun install --frozen-lockfile
bun run test
bun run build
bun run dev
```

The build writes generated `dist/`, `remote-build/` and `remotes-dist/` directories.
`bun run build:remote` independently releases the extension without rebuilding
the shell. Generated `remotes-dist/release.json` contains its bundle SHA-256 and
immutable path. Register the approved remote origin plus this path with the
pinned processor manifest. The manager verifies `remote.js` against that digest.
The HTML import map precedes module loading. Serve remote assets on a separate
origin using the manager remote listener. Browser module loading requires CORS
for the opaque sandbox origin; manager serving supplies it.

## Environment

- `VITE_DQ_API_BASE_URL`: manager origin; empty uses the shell origin.
- `VITE_DQ_INSTALLATION_ID`, `VITE_DQ_NAMESPACE`: optional connection form defaults.
- `VITE_DQ_APPROVED_REMOTE_ORIGIN`: approved separate origin; empty uses generic views.
- `VITE_DQ_BETA_ENABLED`: literal `true` enables dedup invocation visibility;
  default is disabled. Backend entitlement remains authoritative.
- `VITE_DQ_ATHEROS_REVIEW_URL`: optional link to the existing Atheros review queue.

These values contain no secrets. Enter the authorized bearer token at runtime.
For development, serve the shell and manager through the same origin or configure
the manager's allowed UI origin; never embed a bearer token in a build variable.

## Extension boundary and fallback

An exact registered processor/version/digest URL loads inside an iframe with
`sandbox="allow-scripts"`. Its origin is opaque. The host checks the selected
frame's window, origin `null`, random channel, protocol version, request ID,
operation allowlist and 64 KiB bound. The remote checks the parent window and
host origin. Remotes can supply parameters for host review and inspect only the
current authorized run or artifact metadata. Credentials, lease tokens and
download capabilities never cross the channel. The host keeps submission,
navigation, cancellation, replay and download authorization.

Missing or incompatible remotes retain generic views. Failed frames are removed;
the ready handshake times out after five seconds. The generic form checks common
scalar schema constraints; the manager independently validates the full pinned
JSON Schema. Historical runs resolve their exact processor version and remote.

Run success, projection application and human decisions are displayed separately.
The [existing Atheros UI](../../../apps/integration-console/atheros-search-ui/README.md)
retains human decisions and adds an opt-in processing panel without automatic
invocation on page load or decision submission.
