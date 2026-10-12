# Delip Queve conventions

- The manager resolves approved manifests and dispatches existing Octopus
  reservations. Add processors through manifests and the lifecycle protocol.
- `pkg/contract/` defines the snake_case control contract; update producer and
  consumer tests together when changing it.
- Worker identity is unique per process boot. Repeated attempt requests are
  immutable; a restart must not inherit an old reservation.
- Workers consume supplied artifact and report interfaces. Artifact references
  must identify exact object versions, SHA-256 checksums, and bounded sizes.
- Keep the shell and processor remotes independently buildable. Host tokens
  never cross the sandboxed frame boundary.
- Verify Go code with `make check` and UI code with `make ui-check`.
