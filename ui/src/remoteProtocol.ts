import type { Manifest, RemoteManifest } from "./types";
export const hostAPIVersion = "dq.host.v1";
export type RemoteOperation =
  | "ready"
  | "set_parameters"
  | "inspect_run"
  | "artifact_metadata"
  | "failed";
export interface RemoteRequest {
  version: string;
  channel: string;
  request_id: string;
  kind: "request";
  operation: RemoteOperation;
  payload: unknown;
}
const operations = new Set([
  "ready",
  "set_parameters",
  "inspect_run",
  "artifact_metadata",
  "failed",
]);
export const maxMessageBytes = 65536;

export function pinnedRemoteURL(
  manifest: Manifest,
  approvedOrigin: string,
  hostOrigin: string,
): URL | undefined {
  const remote: RemoteManifest | undefined = manifest.ui_remote;
  if (
    !remote ||
    remote.host_api_version !== hostAPIVersion ||
    !/^[a-f0-9]{64}$/.test(remote.bundle_digest)
  )
    return;
  try {
    const url = new URL(remote.immutable_url);
    if (
      url.origin !== approvedOrigin ||
      url.origin === hostOrigin ||
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      !["http:", "https:"].includes(url.protocol)
    )
      return;
    const expected = `/${encodeURIComponent(manifest.processor_id)}/${encodeURIComponent(manifest.version)}/${remote.bundle_digest}/index.html`;
    if (url.pathname !== expected) return;
    return url;
  } catch {
    return;
  }
}

export function validateRemoteRequest(
  event: Pick<MessageEvent, "origin" | "source" | "data">,
  frameWindow: Window | null,
  channel: string,
): RemoteRequest | undefined {
  // allow-scripts without allow-same-origin produces the opaque origin "null".
  // Source identity and the unguessable frame channel bind that origin to this pinned frame.
  if (!frameWindow || event.source !== frameWindow || event.origin !== "null")
    return;
  const data = event.data;
  if (!data || typeof data !== "object" || Array.isArray(data)) return;
  try {
    if (
      new TextEncoder().encode(JSON.stringify(data)).byteLength >
      maxMessageBytes
    )
      return;
  } catch {
    return;
  }
  if (
    data.version !== hostAPIVersion ||
    data.channel !== channel ||
    data.kind !== "request" ||
    typeof data.request_id !== "string" ||
    !/^[a-zA-Z0-9_-]{1,80}$/.test(data.request_id) ||
    !operations.has(data.operation)
  )
    return;
  if (
    Object.keys(data).some(
      (key) =>
        ![
          "version",
          "channel",
          "kind",
          "request_id",
          "operation",
          "payload",
        ].includes(key),
    )
  )
    return;
  return data as RemoteRequest;
}
