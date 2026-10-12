import {
  createEffect,
  createResource,
  createSignal,
  For,
  Match,
  onCleanup,
  Show,
  Switch,
} from "solid-js";
import { api } from "./api";
import {
  accessToken,
  betaEnabled,
  reviewURL,
  scope,
  setAccessToken,
  setScope,
  signOut,
} from "./session";
import { SchemaForm } from "./SchemaForm";
import { RemoteFrame } from "./RemoteFrame";
import { checkParameters, defaultParameters } from "./schema";
import { terminal } from "./types";
import type { ArtifactRef, Manifest, Run } from "./types";

type Outcome<T> =
  | { value: T; error?: never }
  | { value?: never; error: string };
async function load<T>(fetcher: () => Promise<T>): Promise<Outcome<T>> {
  try {
    return { value: await fetcher() };
  } catch (error) {
    return {
      error:
        error instanceof Error
          ? error.message
          : "The requested data is unavailable.",
    };
  }
}

export function parseRoute(path: string) {
  const parts = path.split("/").filter(Boolean);
  try {
    if (parts[0] !== "dq")
      return { kind: path === "/" ? "processors" : "missing" } as const;
    if (parts.length === 2 && parts[1] === "processors")
      return { kind: "processors" } as const;
    if (
      parts.length === 5 &&
      parts[1] === "processors" &&
      parts[4] === "invoke"
    )
      return {
        kind: "invoke",
        id: decodeURIComponent(parts[2]!),
        version: decodeURIComponent(parts[3]!),
      } as const;
    if (parts.length === 3 && parts[1] === "runs")
      return { kind: "run", id: decodeURIComponent(parts[2]!) } as const;
    if (parts.length === 3 && parts[1] === "artifacts")
      return { kind: "artifact", id: decodeURIComponent(parts[2]!) } as const;
    if (parts.length === 3 && parts[1] === "atheros" && parts[2] === "dedup")
      return { kind: "dedup" } as const;
  } catch {
    /* Invalid encoded paths have a normal not-found view. */
  }
  return { kind: "missing" } as const;
}

const processorPath = (manifest: Manifest) =>
  `/dq/processors/${encodeURIComponent(manifest.processor_id)}/${encodeURIComponent(manifest.version)}/invoke`;

function ErrorNotice(props: { message?: string; retry: () => void }) {
  return (
    <Show when={props.message}>
      <div class="notice error" role="alert">
        <span>{props.message}</span>
        <button type="button" onClick={props.retry}>
          Retry
        </button>
      </div>
    </Show>
  );
}

function SessionForm() {
  const [installation, setInstallation] = createSignal(scope().installation_id);
  const [namespace, setNamespace] = createSignal(scope().namespace);
  const [token, setToken] = createSignal("");
  return (
    <section class="session">
      <p class="eyebrow">Installation access</p>
      <h1>Connect to Delip Queve</h1>
      <p>
        Use an authorized DQ bearer identity. Access is checked for every action
        and scope.
      </p>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          setScope({
            installation_id: installation().trim(),
            namespace: namespace().trim(),
          });
          setAccessToken(token().trim());
          setToken("");
        }}
      >
        <label class="field">
          <span>Installation ID</span>
          <input
            required
            pattern="[a-zA-Z0-9_.-]+"
            value={installation()}
            onInput={(event) => setInstallation(event.currentTarget.value)}
          />
        </label>
        <label class="field">
          <span>Namespace</span>
          <input
            required
            pattern="[a-zA-Z0-9_.-]+"
            value={namespace()}
            onInput={(event) => setNamespace(event.currentTarget.value)}
          />
        </label>
        <label class="field">
          <span>Bearer token</span>
          <input
            required
            type="password"
            autocomplete="off"
            value={token()}
            onInput={(event) => setToken(event.currentTarget.value)}
          />
        </label>
        <p class="muted">
          The shell keeps this credential in memory for this session. Processor
          extensions receive scoped metadata and actions.
        </p>
        <button type="submit" class="primary">
          Connect
        </button>
      </form>
    </section>
  );
}

function Catalog(props: { navigate: (path: string) => void }) {
  const [catalog, { refetch }] = createResource(() =>
    load(() => api.processors()),
  );
  const [query, setQuery] = createSignal("");
  const items = () =>
    catalog()?.value?.items.filter((item) =>
      `${item.processor_id} ${item.owner_team}`
        .toLowerCase()
        .includes(query().toLowerCase()),
    ) || [];
  return (
    <>
      <div class="page-title">
        <div>
          <p class="eyebrow">Registry</p>
          <h1>Processors</h1>
          <p>
            Authorized immutable versions for {scope().installation_id} /{" "}
            {scope().namespace}.
          </p>
        </div>
        <button onClick={() => void refetch()} disabled={catalog.loading}>
          Refresh catalog
        </button>
      </div>
      <label class="field search">
        <span>Find a processor</span>
        <input
          type="search"
          value={query()}
          onInput={(event) => setQuery(event.currentTarget.value)}
        />
      </label>
      <Show when={catalog.loading}>
        <p role="status">Loading processor catalog...</p>
      </Show>
      <ErrorNotice message={catalog()?.error} retry={() => void refetch()} />
      <Show when={catalog()?.value}>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Processor</th>
                <th>Version</th>
                <th>Owner</th>
                <th>Policy</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              <For each={items()}>
                {(item) => (
                  <tr>
                    <td>
                      <strong>{item.processor_id}</strong>
                    </td>
                    <td>
                      <code>{item.version}</code>
                    </td>
                    <td>{item.owner_team}</td>
                    <td>
                      <code>{item.policy_version}</code>
                    </td>
                    <td>
                      <button
                        onClick={() => props.navigate(processorPath(item))}
                      >
                        Open invocation
                      </button>
                    </td>
                  </tr>
                )}
              </For>
            </tbody>
          </table>
        </div>
        <Show when={!items().length}>
          <p class="notice" role="status">
            No authorized processors match this view.
          </p>
        </Show>
      </Show>
    </>
  );
}

function InvocationForm(props: {
  manifest: Manifest;
  navigate: (path: string) => void;
}) {
  const [parameters, setParameters] = createSignal<Record<string, unknown>>(
    defaultParameters(props.manifest.parameter_schema),
  );
  const [operation, setOperation] = createSignal(
    props.manifest.processor_id === "atheros.dedup" ? "prepare_candidates" : "",
  );
  const [key, setKey] = createSignal(crypto.randomUUID());
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");
  const [reviewing, setReviewing] = createSignal(false);
  const disabled = () =>
    props.manifest.processor_id === "atheros.dedup" && !betaEnabled;
  function changeParameters(next: Record<string, unknown>) {
    setParameters(next);
    setKey(crypto.randomUUID());
    setReviewing(false);
  }
  async function submit() {
    if (busy() || disabled()) return;
    const issues = checkParameters(
      props.manifest.parameter_schema,
      parameters(),
    );
    if (!operation().trim()) issues.push("An operation is required.");
    if (issues.length) {
      setError(issues.join(" "));
      return;
    }
    setBusy(true);
    setError("");
    try {
      const run = await api.admit(
        props.manifest,
        operation().trim(),
        parameters(),
        key(),
      );
      props.navigate(`/dq/runs/${encodeURIComponent(run.run_id)}`);
    } catch (problem) {
      setError(
        problem instanceof Error ? problem.message : "Admission failed.",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <p class="notice">
        Processor <strong>{props.manifest.processor_id}</strong> /{" "}
        <code>{props.manifest.version}</code>. Policy{" "}
        <code>{props.manifest.policy_version}</code>. Execution mode is selected
        by the authorized scope policy.
      </p>
      <Show when={disabled()}>
        <p class="notice" role="status">
          Dedup beta invocation is disabled in this shell. Existing Atheros
          review remains available.
        </p>
      </Show>
      <RemoteFrame
        manifest={props.manifest}
        mode="parameter_form"
        onParameters={changeParameters}
      />
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (!disabled()) setReviewing(true);
        }}
      >
        <label class="field">
          <span>Operation</span>
          <Show
            when={props.manifest.processor_id === "atheros.dedup"}
            fallback={
              <input
                required
                value={operation()}
                disabled={busy() || disabled()}
                onInput={(event) => {
                  setOperation(event.currentTarget.value);
                  setKey(crypto.randomUUID());
                  setReviewing(false);
                }}
              />
            }
          >
            <select
              value={operation()}
              disabled={busy() || disabled()}
              onChange={(event) => {
                setOperation(event.currentTarget.value);
                setKey(crypto.randomUUID());
                setReviewing(false);
              }}
            >
              <option value="prepare_candidates">Prepare candidates</option>
              <option value="reconcile_decision">
                Reconcile recorded decision
              </option>
            </select>
          </Show>
        </label>
        <SchemaForm
          schema={props.manifest.parameter_schema}
          parameters={parameters()}
          onChange={changeParameters}
          onError={setError}
          disabled={busy() || disabled()}
        />
        <Show when={error()}>
          <p class="notice error" role="alert">
            {error()} Retry uses the same invocation key while parameters remain
            unchanged.
          </p>
        </Show>
        <button type="submit" class="primary" disabled={busy() || disabled()}>
          Review invocation
        </button>
      </form>
      <Show when={reviewing()}>
        <section class="review" aria-label="Invocation review">
          <h2>Review invocation</h2>
          <p>
            {operation()} / {props.manifest.processor_id} /{" "}
            {props.manifest.version}
          </p>
          <pre>{JSON.stringify(parameters(), null, 2)}</pre>
          <p class="muted">
            Admission creates a durable run. Completion and projection
            application are reported separately.
          </p>
          <div class="actions">
            <button
              type="button"
              class="primary"
              disabled={busy() || disabled()}
              onClick={() => void submit()}
            >
              {busy() ? "Admitting..." : "Submit run"}
            </button>
            <button
              type="button"
              disabled={busy()}
              onClick={() => setReviewing(false)}
            >
              Close review
            </button>
          </div>
        </section>
      </Show>
    </>
  );
}

function Invocation(props: {
  id: string;
  version: string;
  navigate: (path: string) => void;
}) {
  const [manifest, { refetch }] = createResource(
    () => [props.id, props.version] as const,
    ([id, version]) => load(() => api.manifest(id, version)),
  );
  return (
    <>
      <div class="page-title">
        <div>
          <p class="eyebrow">Invocation</p>
          <h1>{props.id}</h1>
          <p>Version {props.version}</p>
        </div>
        <button onClick={() => props.navigate("/dq/processors")}>
          Back to processors
        </button>
      </div>
      <Show when={manifest.loading}>
        <p role="status">Loading the pinned parameter schema...</p>
      </Show>
      <ErrorNotice message={manifest()?.error} retry={() => void refetch()} />
      <Show when={manifest()?.value} keyed>
        {(value) => (
          <InvocationForm manifest={value} navigate={props.navigate} />
        )}
      </Show>
    </>
  );
}

function RunDetails(props: {
  run: () => Run;
  refresh: () => void;
  navigate: (path: string) => void;
}) {
  const [manifest] = createResource(
    () => JSON.stringify([props.run().processor_id, props.run().processor_version]),
    (key) => {
      const [id, version] = JSON.parse(key) as [string, string];
      return load(() => api.manifest(id, version));
    },
  );
  const [busy, setBusy] = createSignal(false);
  const [error, setError] = createSignal("");
  const [action, setAction] = createSignal<"cancel" | "replay">();
  const [replayKey, setReplayKey] = createSignal(crypto.randomUUID());
  async function performAction() {
    const selected = action();
    if (!selected || busy()) return;
    setBusy(true);
    setError("");
    try {
      const result =
        selected === "cancel"
          ? await api.cancel(props.run().run_id)
          : await api.replay(props.run().run_id, replayKey());
      if (selected === "replay") setReplayKey(crypto.randomUUID());
      setAction(undefined);
      if (selected === "replay")
        props.navigate(`/dq/runs/${encodeURIComponent(result.run_id)}`);
      else props.refresh();
    } catch (problem) {
      setError(
        problem instanceof Error ? problem.message : "The action failed.",
      );
    } finally {
      setBusy(false);
    }
  }
  const refs = () => [
    ...(props.run().input_refs || []),
    ...(props.run().output_refs || []),
  ];
  return (
    <>
      <div class="run-state" role="status">
        <strong class="status" data-status={props.run().status}>
          {props.run().status}
        </strong>
        <span>
          {props.run().execution_mode} mode / revision {props.run().state_revision}
        </span>
        <span>
          {props.run().status === "succeeded" ? "Run completed. " : ""}
          {props.run().projection_applied
            ? "Projection applied."
            : "Projection application is not confirmed."}
        </span>
      </div>
      <dl class="metadata">
        <dt>Processor version</dt>
        <dd>
          {props.run().processor_id} / {props.run().processor_version}
        </dd>
        <dt>Operation</dt>
        <dd>{props.run().operation}</dd>
        <dt>Created</dt>
        <dd>
          <time>{props.run().created_at}</time>
        </dd>
        <dt>Deadline</dt>
        <dd>
          <time>{props.run().deadline_at}</time>
        </dd>
        <dt>Policy</dt>
        <dd>
          <code>{props.run().policy_version}</code>
        </dd>
        <dt>Implementation digest</dt>
        <dd>
          <code>{props.run().implementation_digest}</code>
        </dd>
        <Show when={props.run().current_attempt}>
          {(attempt) => (
            <>
              <dt>Current attempt</dt>
              <dd>
                {attempt().attempt_number} / {attempt().status} /{" "}
                <code>{attempt().attempt_id}</code>
              </dd>
            </>
          )}
        </Show>
      </dl>
      <Show when={props.run().sanitized_error}>
        <p class="notice error" role="alert">
          {props.run().error_code}: {props.run().sanitized_error}
        </p>
      </Show>
      <Show when={manifest()?.value}>
        {(value) => (
          <RemoteFrame
            manifest={value()}
            mode="run_inspector"
            run={props.run()}
          />
        )}
      </Show>
      <h2>Parameters</h2>
      <pre>{JSON.stringify(props.run().parameters, null, 2)}</pre>
      <h2>Artifacts</h2>
      <Show
        when={refs().length}
        fallback={
          <p class="muted">No artifacts have been committed to this run.</p>
        }
      >
        <ul class="artifact-list">
          <For each={refs()}>
            {(ref) => (
              <li>
                <button
                  onClick={() =>
                    props.navigate(
                      `/dq/artifacts/${encodeURIComponent(ref.artifact_id)}`,
                    )
                  }
                >
                  {ref.artifact_id}
                </button>
                <span>
                  {ref.schema_id} / {ref.size_bytes.toLocaleString()} bytes /
                  exact version {ref.version_id}
                </span>
              </li>
            )}
          </For>
        </ul>
      </Show>
      <div class="actions">
        <button disabled={busy()} onClick={() => props.refresh()}>
          Refresh status
        </button>
        <Show when={!terminal(props.run().status)}>
          <button disabled={busy()} onClick={() => setAction("cancel")}>
            Cancel run
          </button>
        </Show>
        <Show when={terminal(props.run().status)}>
          <button disabled={busy()} onClick={() => setAction("replay")}>
            Replay as a new run
          </button>
        </Show>
      </div>
      <Show when={action()}>
        <section class="review" aria-label="Run action review">
          <h2>
            {action() === "cancel" ? "Cancel this run" : "Replay this run"}
          </h2>
          <p>
            {action() === "cancel"
              ? "Cancellation fences the current attempt. Already applied changes remain recorded."
              : "Replay creates a new audited run using retained source artifacts and the pinned processor version."}
          </p>
          <button
            class="primary"
            disabled={busy()}
            onClick={() => void performAction()}
          >
            {busy() ? "Submitting..." : "Confirm " + action()}
          </button>
          <button disabled={busy()} onClick={() => setAction(undefined)}>
            Close review
          </button>
        </section>
      </Show>
      <Show when={error()}>
        <p class="notice error" role="alert">
          {error()}
        </p>
      </Show>
    </>
  );
}

function RunPage(props: { id: string; navigate: (path: string) => void }) {
  let controller: AbortController | undefined;
  const [run, { refetch }] = createResource(
    () => props.id,
    (id) => {
      controller?.abort();
      controller = new AbortController();
      return load(() => api.run(id, controller!.signal));
    },
  );
  let highest: Run | undefined;
  const current = () => {
    const value = run()?.value;
    if (
      value &&
      (!highest ||
        highest.run_id !== value.run_id ||
        highest.state_revision <= value.state_revision)
    )
      highest = value;
    return highest?.run_id === props.id ? highest : undefined;
  };
  createEffect(() => {
    const value = current();
    if (!value || terminal(value.status) || run()?.error) return;
    const timer = window.setTimeout(() => void refetch(), 3000);
    onCleanup(() => window.clearTimeout(timer));
  });
  onCleanup(() => controller?.abort());
  return (
    <>
      <div class="page-title">
        <div>
          <p class="eyebrow">Authoritative lifecycle</p>
          <h1>
            Run <code>{props.id}</code>
          </h1>
        </div>
        <button onClick={() => props.navigate("/dq/processors")}>
          Processors
        </button>
      </div>
      <Show when={run.loading}>
        <p role="status">Loading run status...</p>
      </Show>
      <ErrorNotice message={run()?.error} retry={() => void refetch()} />
      <Show when={current()}>
        {(value) => (
          <RunDetails
            run={value}
            refresh={() => void refetch()}
            navigate={props.navigate}
          />
        )}
      </Show>
    </>
  );
}

function ArtifactDetails(props: {
  artifact: ArtifactRef;
  navigate: (path: string) => void;
}) {
  const [error, setError] = createSignal("");
  const [busy, setBusy] = createSignal(false);
  const [download, setDownload] = createSignal("");
  const [expires, setExpires] = createSignal("");
  const [run] = createResource(
    () => props.artifact.producer_run_id,
    (id) => load(() => api.run(id)),
  );
  const [manifest] = createResource(
    () =>
      run()?.value &&
      ([run()!.value!.processor_id, run()!.value!.processor_version] as const),
    ([id, version]) => load(() => api.manifest(id, version)),
  );
  async function authorizeDownload() {
    setBusy(true);
    setError("");
    setDownload("");
    try {
      const access = await api.artifactAccess(props.artifact.artifact_id);
      const url = new URL(access.download_url || access.url || "");
      if (
        url.username ||
        url.password ||
        (url.protocol !== "https:" &&
          !(
            url.protocol === "http:" &&
            ["localhost", "127.0.0.1"].includes(url.hostname)
          ))
      )
        throw new Error("The authorized download URL is invalid.");
      setDownload(url.toString());
      setExpires(access.expires_at || "");
    } catch (problem) {
      setError(
        problem instanceof Error
          ? problem.message
          : "Artifact access is unavailable.",
      );
    } finally {
      setBusy(false);
    }
  }
  return (
    <>
      <dl class="metadata">
        <dt>Schema</dt>
        <dd>{props.artifact.schema_id}</dd>
        <dt>Media type</dt>
        <dd>{props.artifact.media_type}</dd>
        <dt>Size</dt>
        <dd>{props.artifact.size_bytes.toLocaleString()} bytes</dd>
        <dt>Content SHA-256</dt>
        <dd>
          <code>{props.artifact.sha256}</code>
        </dd>
        <dt>Exact object version</dt>
        <dd>
          <code>{props.artifact.version_id}</code>
        </dd>
        <dt>ETag</dt>
        <dd>
          <code>{props.artifact.etag}</code>
        </dd>
      </dl>
      <p class="muted">
        ETags identify object versions; the SHA-256 digest verifies content.
        Artifact bytes remain outside the manager.
      </p>
      <Show when={manifest()?.value}>
        {(value) => (
          <RemoteFrame
            manifest={value()}
            mode="artifact_preview"
            artifact={props.artifact}
          />
        )}
      </Show>
      <div class="actions">
        <button
          class="primary"
          disabled={busy()}
          onClick={() => void authorizeDownload()}
        >
          {busy() ? "Authorizing..." : "Authorize exact-version download"}
        </button>
        <Show when={props.artifact.producer_run_id}>
          <button
            onClick={() =>
              props.navigate(
                `/dq/runs/${encodeURIComponent(props.artifact.producer_run_id!)}`,
              )
            }
          >
            Open producer run
          </button>
        </Show>
      </div>
      <Show when={download()}>
        <p class="notice">
          <a href={download()} target="_blank" rel="noreferrer noopener">
            Download authorized artifact
          </a>
          <Show when={expires()}>
            <span> / expires {expires()}</span>
          </Show>
        </p>
      </Show>
      <Show when={error()}>
        <p class="notice error" role="alert">
          {error()}
        </p>
      </Show>
    </>
  );
}

function ArtifactPage(props: { id: string; navigate: (path: string) => void }) {
  const [artifact, { refetch }] = createResource(
    () => props.id,
    (id) => load(() => api.artifact(id)),
  );
  return (
    <>
      <div class="page-title">
        <div>
          <p class="eyebrow">Artifact access</p>
          <h1>
            Artifact <code>{props.id}</code>
          </h1>
        </div>
      </div>
      <Show when={artifact.loading}>
        <p role="status">Loading authorized artifact metadata...</p>
      </Show>
      <ErrorNotice message={artifact()?.error} retry={() => void refetch()} />
      <Show when={artifact()?.value} keyed>
        {(value) => (
          <ArtifactDetails artifact={value} navigate={props.navigate} />
        )}
      </Show>
    </>
  );
}

function DedupPage(props: { navigate: (path: string) => void }) {
  const [catalog, { refetch }] = createResource(
    () => betaEnabled,
    () => load(() => api.processors()),
  );
  const [runID, setRunID] = createSignal("");
  return (
    <>
      <p class="eyebrow">Atheros / Identity review</p>
      <h1>
        Dedup processing <span class="beta">Beta</span>
      </h1>
      <p>
        Candidate preparation and decision reconciliation run asynchronously. A
        recorded decision, a completed run and an applied projection represent
        separate outcomes.
      </p>
      <Show
        when={betaEnabled}
        fallback={
          <p class="notice" role="status">
            Beta processing is disabled in this shell. The existing human review
            queue remains available.
          </p>
        }
      >
        <Show when={catalog.loading}>
          <p role="status">Checking authorized dedup versions...</p>
        </Show>
        <ErrorNotice message={catalog()?.error} retry={() => void refetch()} />
        <For
          each={
            catalog()?.value?.items.filter(
              (manifest) => manifest.processor_id === "atheros.dedup",
            ) || []
          }
        >
          {(manifest) => (
            <p>
              <button
                class="primary"
                onClick={() => props.navigate(processorPath(manifest))}
              >
                Invoke {manifest.processor_id} / {manifest.version}
              </button>
            </p>
          )}
        </For>
        <Show
          when={
            catalog()?.value &&
            !catalog()!.value!.items.some(
              (manifest) => manifest.processor_id === "atheros.dedup",
            )
          }
        >
          <p class="notice" role="status">
            No dedup version is authorized for this installation.
          </p>
        </Show>
      </Show>
      <h2>Inspect an existing run</h2>
      <form
        class="inline-form"
        onSubmit={(event) => {
          event.preventDefault();
          props.navigate(`/dq/runs/${encodeURIComponent(runID().trim())}`);
        }}
      >
        <label class="field">
          <span>Run ID</span>
          <input
            required
            value={runID()}
            onInput={(event) => setRunID(event.currentTarget.value)}
          />
        </label>
        <button type="submit">Inspect run</button>
      </form>
      <Show when={reviewURL}>
        <p>
          <a href={reviewURL} rel="noreferrer">
            Open Atheros review queue
          </a>
        </p>
      </Show>
      <p class="muted">
        The backend independently checks beta entitlement, roles and scope. This
        view never confirms device identities.
      </p>
    </>
  );
}

export default function App() {
  const [path, setPath] = createSignal(window.location.pathname);
  const route = () => parseRoute(path());
  const popstate = () => setPath(window.location.pathname);
  window.addEventListener("popstate", popstate);
  onCleanup(() => window.removeEventListener("popstate", popstate));
  function navigate(next: string) {
    window.history.pushState({}, "", next);
    setPath(next);
    document.getElementById("main-content")?.focus();
    window.scrollTo({ top: 0, behavior: "instant" });
  }
  const routeID = () => {
    const value = route();
    return ("id" in value ? value.id : "") || "";
  };
  const routeVersion = () => {
    const value = route();
    return ("version" in value ? value.version : "") || "";
  };
  return (
    <>
      <a class="skip-link" href="#main-content">
        Skip to content
      </a>
      <header class="topbar">
        <a
          href="/dq/processors"
          class="brand"
          onClick={(event) => {
            event.preventDefault();
            navigate("/dq/processors");
          }}
        >
          <span class="brand-mark">DQ</span>
          <span>Delip Queve</span>
        </a>
        <span class="installation">
          <Show when={accessToken()}>
            {scope().installation_id} / {scope().namespace}
          </Show>
        </span>
        <Show when={accessToken()}>
          <button onClick={signOut}>Sign out</button>
        </Show>
      </header>
      <div class="workspace">
        <nav aria-label="Primary navigation">
          <a
            href="/dq/processors"
            aria-current={route().kind === "processors" ? "page" : undefined}
            onClick={(event) => {
              event.preventDefault();
              navigate("/dq/processors");
            }}
          >
            Processors
          </a>
          <a
            href="/dq/atheros/dedup"
            aria-current={route().kind === "dedup" ? "page" : undefined}
            onClick={(event) => {
              event.preventDefault();
              navigate("/dq/atheros/dedup");
            }}
          >
            Atheros dedup <span class="beta">Beta</span>
          </a>
          <p class="nav-note">
            Immutable versions.
            <br />
            Authoritative run status.
            <br />
            Scoped artifact access.
          </p>
        </nav>
        <main id="main-content" tabIndex={-1}>
          <Show when={accessToken()} fallback={<SessionForm />}>
            <Switch>
              <Match when={route().kind === "processors"}>
                <Catalog navigate={navigate} />
              </Match>
              <Match when={route().kind === "invoke"}>
                <Invocation
                  id={routeID()}
                  version={routeVersion()}
                  navigate={navigate}
                />
              </Match>
              <Match when={route().kind === "run"}>
                <RunPage id={routeID()} navigate={navigate} />
              </Match>
              <Match when={route().kind === "artifact"}>
                <ArtifactPage id={routeID()} navigate={navigate} />
              </Match>
              <Match when={route().kind === "dedup"}>
                <DedupPage navigate={navigate} />
              </Match>
              <Match when={route().kind === "missing"}>
                <h1>Page not found</h1>
                <button onClick={() => navigate("/dq/processors")}>
                  Open processors
                </button>
              </Match>
            </Switch>
          </Show>
        </main>
      </div>
    </>
  );
}
