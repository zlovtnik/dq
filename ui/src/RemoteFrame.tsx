import { createEffect, createSignal, onCleanup, Show } from "solid-js";
import { approvedRemoteOrigin } from "./session";
import { embedded } from './embeddedBridge';
import { checkParameters } from "./schema";
import {
  hostAPIVersion,
  pinnedRemoteURL,
  validateRemoteRequest,
} from "./remoteProtocol";
import type { ArtifactRef, Manifest, Run } from "./types";

export function RemoteFrame(props: {
  manifest: Manifest;
  mode: "parameter_form" | "run_inspector" | "artifact_preview";
  run?: Run;
  artifact?: ArtifactRef;
  onParameters?: (parameters: Record<string, unknown>) => void;
}) {
  const [failed, setFailed] = createSignal("");
  const [ready, setReady] = createSignal(false);
  const [src, setSrc] = createSignal("");
  let frame: HTMLIFrameElement | undefined;

  createEffect(() => {
    const manifest = props.manifest;
    const mode = props.mode;
    const remote = manifest.ui_remote;
    setReady(false);
    setFailed("");
    if (!remote?.[mode]) {
      setSrc("");
      return;
    }
    const url = pinnedRemoteURL(
      manifest,
      approvedRemoteOrigin,
      window.location.origin,
    );
    if (!url) {
      setSrc("");
      setFailed(
        "This version has no compatible approved extension. The generic view remains available.",
      );
      return;
    }
    const channel = crypto.randomUUID();
    const seen = new Set<string>();
    url.searchParams.set("channel", channel);
    url.searchParams.set("host_origin", embedded ? 'null' : window.location.origin);
    setSrc(url.toString());
    const timer = window.setTimeout(() => {
      setFailed("The processor extension timed out. Use the generic view.");
      setSrc("");
    }, 5000);

    function fail(message: string) {
      window.clearTimeout(timer);
      setFailed(message);
      setSrc("");
    }
    function receive(event: MessageEvent) {
      const request = validateRemoteRequest(
        event,
        frame?.contentWindow || null,
        channel,
      );
      if (!request) return;
      if (seen.has(request.request_id)) return;
      if (seen.size >= 128) {
        fail(
          "The processor extension exceeded its message limit. Use the generic view.",
        );
        return;
      }
      seen.add(request.request_id);
      let payload: unknown;
      let error: string | undefined;
      if (request.operation === "ready") {
        window.clearTimeout(timer);
        setReady(true);
        payload = {
          mode,
          processor_id: manifest.processor_id,
          version: manifest.version,
          schema: manifest.parameter_schema,
          run_id: props.run?.run_id,
          artifact_id: props.artifact?.artifact_id,
        };
      } else if (!ready()) return;
      else if (
        request.operation === "set_parameters" &&
        mode === "parameter_form" &&
        props.onParameters
      ) {
        if (
          !request.payload ||
          typeof request.payload !== "object" ||
          Array.isArray(request.payload)
        )
          error = "Parameters must be an object.";
        else {
          const parameters = request.payload as Record<string, unknown>;
          const issues = checkParameters(manifest.parameter_schema, parameters);
          if (issues.length) error = issues.join(" ");
          else {
            props.onParameters(parameters);
            payload = { accepted: true, submitted: false };
          }
        }
      } else if (
        request.operation === "inspect_run" &&
        mode === "run_inspector" &&
        props.run &&
        request.payload &&
        typeof request.payload === "object" &&
        Object.keys(request.payload).length === 1 &&
        (request.payload as { run_id?: string }).run_id === props.run.run_id
      ) {
        // Only the current authorized run is exposed. Lease tokens stay outside extension data.
        const { current_attempt: attempt, ...run } = props.run;
        payload = {
          ...run,
          current_attempt: attempt && {
            attempt_id: attempt.attempt_id,
            attempt_number: attempt.attempt_number,
            status: attempt.status,
            lease_expires_at: attempt.lease_expires_at,
          },
        };
      } else if (
        request.operation === "artifact_metadata" &&
        mode === "artifact_preview" &&
        props.artifact &&
        request.payload &&
        typeof request.payload === "object" &&
        Object.keys(request.payload).length === 1 &&
        (request.payload as { artifact_id?: string }).artifact_id ===
          props.artifact.artifact_id
      )
        payload = props.artifact;
      else if (request.operation === "failed") {
        fail("The processor extension failed. Use the generic view.");
        return;
      } else error = "This action is outside the extension scope.";
      const response = {
        version: hostAPIVersion,
        channel,
        request_id: request.request_id,
        kind: "response",
        operation: request.operation,
        payload,
        error,
      };
      if (
        new TextEncoder().encode(JSON.stringify(response)).byteLength > 65536
      ) {
        fail(
          "The extension response exceeded its metadata limit. Use the generic view.",
        );
        return;
      }
      // The sandbox origin is opaque, so targetOrigin must be *. No credentials cross this channel.
      frame?.contentWindow?.postMessage(response, "*");
    }
    window.addEventListener("message", receive);
    onCleanup(() => {
      window.clearTimeout(timer);
      window.removeEventListener("message", receive);
      setSrc("");
    });
  });

  return (
    <section class="extension" aria-label="Processor extension">
      <Show when={src()}>
        <p role="status">
          {ready()
            ? "Processor extension ready"
            : "Loading processor extension..."}
        </p>
        <iframe
          ref={frame}
          title={`${props.manifest.processor_id} ${props.mode.replaceAll("_", " ")}`}
          src={src()}
          sandbox="allow-scripts"
          referrerPolicy="no-referrer"
          onError={() => {
            setFailed(
              "The processor extension could not load. Use the generic view.",
            );
            setSrc("");
          }}
        />
      </Show>
      <Show when={failed()}>
        <p role="status">{failed()}</p>
      </Show>
    </section>
  );
}
