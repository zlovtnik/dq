import { createSignal, Show } from "solid-js";
import { render } from "solid-js/web";
import { hostAPIVersion } from "../src/remoteProtocol";

const query = new URLSearchParams(window.location.search);
const channel = query.get("channel") || "";
const hostOrigin = query.get("host_origin") || "";
const pending = new Map<string, string>();
const [mode, setMode] = createSignal("");
const [parameters, setParameters] = createSignal("{}");
const [data, setData] = createSignal<unknown>();
const [notice, setNotice] = createSignal("Connecting to the host...");
const [busy, setBusy] = createSignal(false);
let sequence = 0;

function request(operation: string, payload: unknown) {
  const requestID = "remote_" + ++sequence;
  pending.set(requestID, operation);
  window.parent.postMessage(
    {
      version: hostAPIVersion,
      channel,
      kind: "request",
      request_id: requestID,
      operation,
      payload,
    },
    hostOrigin,
  );
  window.setTimeout(() => {
    if (pending.delete(requestID)) {
      setBusy(false);
      setNotice("The host did not respond. Use the generic view.");
    }
  }, 5000);
}

function receive(event: MessageEvent) {
  if (
    event.source !== window.parent ||
    event.origin !== hostOrigin ||
    !event.data ||
    typeof event.data !== "object"
  )
    return;
  const response = event.data;
  try {
    if (new TextEncoder().encode(JSON.stringify(response)).byteLength > 65536)
      return;
  } catch {
    return;
  }
  if (
    response.version !== hostAPIVersion ||
    response.channel !== channel ||
    response.kind !== "response" ||
    pending.get(response.request_id) !== response.operation
  )
    return;
  pending.delete(response.request_id);
  setBusy(false);
  if (response.error) {
    setNotice(String(response.error));
    return;
  }
  if (response.operation === "ready") {
    setMode(response.payload.mode);
    setNotice("Scoped extension connected.");
    if (response.payload.mode === "run_inspector")
      request("inspect_run", { run_id: response.payload.run_id });
    if (response.payload.mode === "artifact_preview")
      request("artifact_metadata", {
        artifact_id: response.payload.artifact_id,
      });
  } else if (response.operation === "set_parameters")
    setNotice(
      "Parameters copied to the host form. Review and submit in the host.",
    );
  else setData(response.payload);
}

window.addEventListener("message", receive);
const root = document.getElementById("remote-root");
if (
  root &&
  /^[a-f0-9-]{36}$/.test(channel) &&
  /^https?:\/\//.test(hostOrigin) &&
  window.parent !== window
) {
  render(
    () => (
      <main>
        <h2>Atheros dedup extension</h2>
        <p role="status">{notice()}</p>
        <Show when={mode() === "parameter_form"}>
          <p>
            Paste candidate preparation or reconciliation parameters. The host
            validates them and keeps submission controls.
          </p>
          <label>
            Parameters JSON
            <textarea
              rows={7}
              value={parameters()}
              onInput={(event) => setParameters(event.currentTarget.value)}
            />
          </label>
          <button
            type="button"
            disabled={busy()}
            onClick={() => {
              try {
                const value = JSON.parse(parameters());
                setBusy(true);
                request("set_parameters", value);
              } catch {
                setNotice("Enter valid JSON parameters.");
              }
            }}
          >
            Copy parameters to host
          </button>
        </Show>
        <Show when={data()}>
          <pre>{JSON.stringify(data(), null, 2)}</pre>
        </Show>
      </main>
    ),
    root,
  );
  request("ready", {});
} else if (root)
  root.textContent = "Open this extension through the authorized DQ shell.";
