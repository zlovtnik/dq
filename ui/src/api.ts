import { accessToken, scope } from "./session";
import { embedded, embeddedAPI } from './embeddedBridge';
import type { ArtifactRef, Manifest, Run } from "./types";
const apiBase = (import.meta.env.VITE_DQ_API_BASE_URL || "").replace(/\/$/, "");
const maxBytes = 65536;

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function request<T>(
  path: string,
  method = "GET",
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  if (embedded) return embeddedAPI<T>(path,method,body,signal);
  if (!accessToken())
    throw new ApiError(401, "Sign in to inspect this installation.");
  const controller = new AbortController();
  const abort = () => controller.abort();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) controller.abort();
  const timer = window.setTimeout(abort, 10000);
  try {
    const query =
      method === "GET"
        ? "?" + new URLSearchParams({ ...scope() }).toString()
        : "";
    const encoded =
      body === undefined
        ? undefined
        : JSON.stringify({ ...(body as object), ...scope() });
    if (encoded && new TextEncoder().encode(encoded).byteLength > maxBytes)
      throw new ApiError(413, "Control request exceeds 64 KiB.");
    const response = await fetch(apiBase + path + query, {
      method,
      headers: {
        "Content-Type": "application/json",
        Authorization: "Bearer " + accessToken(),
      },
      body: encoded,
      signal: controller.signal,
      redirect: "error",
      credentials: "omit",
    });
    const reader = response.body?.getReader();
    if (!reader)
      throw new ApiError(502, "The manager returned an empty response.");
    const chunks: Uint8Array[] = [];
    let size = 0;
    try {
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        size += next.value.byteLength;
        if (size > maxBytes) {
          await reader.cancel();
          throw new ApiError(502, "The manager response exceeds 64 KiB.");
        }
        chunks.push(next.value);
      }
    } finally {
      reader.releaseLock();
    }
    const bytes = new Uint8Array(size);
    let offset = 0;
    for (const chunk of chunks) {
      bytes.set(chunk, offset);
      offset += chunk.byteLength;
    }
    if (!response.ok) {
      const messages: Record<number, string> = {
        400: "The parameters were rejected.",
        401: "The session is invalid or expired.",
        403: "This action is not authorized for the installation or scope.",
        404: "The requested run, artifact or processor version was not found.",
        409: "The request conflicts with an existing invocation.",
        429: "Admission capacity is exhausted. Retry later.",
      };
      throw new ApiError(
        response.status,
        messages[response.status] || "The manager is unavailable. Retry later.",
      );
    }
    try {
      return JSON.parse(new TextDecoder().decode(bytes)) as T;
    } catch {
      throw new ApiError(502, "The manager returned invalid metadata.");
    }
  } finally {
    window.clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}

const segment = (value: string) => encodeURIComponent(value);
export const api = {
  processors: (signal?: AbortSignal) =>
    request<{ items: Manifest[] }>("/v1/processors", "GET", undefined, signal),
  manifest: (id: string, version: string, signal?: AbortSignal) =>
    request<Manifest>(
      `/v1/processor-versions/${segment(id)}/${segment(version)}`,
      "GET",
      undefined,
      signal,
    ),
  admit: (
    manifest: Manifest,
    operation: string,
    parameters: Record<string, unknown>,
    idempotencyKey: string,
    signal?: AbortSignal,
  ) =>
    request<Run>(
      "/v1/runs",
      "POST",
      {
        processor_id: manifest.processor_id,
        processor_version: manifest.version,
        operation,
        parameters,
        idempotency_key: idempotencyKey,
      },
      signal,
    ),
  run: (id: string, signal?: AbortSignal) =>
    request<Run>(`/v1/runs/${segment(id)}`, "GET", undefined, signal),
  cancel: (id: string) =>
    request<Run>(`/v1/runs/${segment(id)}/cancel`, "POST", {}),
  replay: (id: string, idempotencyKey: string) =>
    request<Run>(`/v1/runs/${segment(id)}/replay`, "POST", {
      idempotency_key: idempotencyKey,
    }),
  artifact: (id: string, signal?: AbortSignal) =>
    request<ArtifactRef>(
      `/v1/artifacts/${segment(id)}`,
      "GET",
      undefined,
      signal,
    ),
  artifactAccess: (id: string) =>
    request<{ download_url?: string; url?: string; expires_at?: string }>(
      `/v1/artifacts/${segment(id)}/access`,
      "POST",
      {},
    ),
};
