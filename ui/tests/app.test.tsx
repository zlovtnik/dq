import { cleanup, fireEvent, render, waitFor } from "@solidjs/testing-library";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import App from "../src/App";
import { api } from "../src/api";
import { accessToken, setAccessToken, setScope } from "../src/session";
import type { Manifest, Run } from "../src/types";

beforeEach(() => {
  window.history.replaceState({}, "", "/dq/processors");
  setAccessToken("");
  setScope({ installation_id: "test", namespace: "audit" });
  vi.stubGlobal("scrollTo", vi.fn());
});
afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  setAccessToken("");
});
const manifest: Manifest = {
  installation_id: "test",
  namespace: "audit",
  processor_id: "example.processor",
  version: "1.0.0",
  owner_team: "test",
  implementation_digest: "a".repeat(64),
  policy_version: "v1",
  parameter_schema: {
    type: "object",
    required: ["limit"],
    properties: { limit: { type: "integer", minimum: 1, maximum: 10 } },
  },
};

describe("stable operational shell", () => {
  it("keeps credentials only in host memory and gates catalog loading", async () => {
    const catalog = vi
      .spyOn(api, "processors")
      .mockResolvedValue({ items: [] });
    const view = render(() => <App />);
    expect(catalog).not.toHaveBeenCalled();
    fireEvent.input(view.getByLabelText("Bearer token"), {
      target: { value: "secret" },
    });
    fireEvent.click(view.getByRole("button", { name: "Connect" }));
    await waitFor(() => expect(catalog).toHaveBeenCalledOnce());
    expect(accessToken()).toBe("secret");
    expect(localStorage.length).toBe(0);
    fireEvent.click(view.getByRole("button", { name: "Sign out" }));
    expect(accessToken()).toBe("");
  });

  it("shows manager errors without presenting successful runs", async () => {
    setAccessToken("secret");
    vi.spyOn(api, "processors").mockRejectedValue(
      new Error("Manager unavailable"),
    );
    const view = render(() => <App />);
    await waitFor(() =>
      expect(view.getByRole("alert")).toHaveTextContent("Manager unavailable"),
    );
    expect(view.queryByText("succeeded")).toBeNull();
  });

  it("requires invocation review and keeps idempotency for retries", async () => {
    setAccessToken("secret");
    window.history.replaceState(
      {},
      "",
      "/dq/processors/example.processor/1.0.0/invoke",
    );
    vi.spyOn(api, "manifest").mockResolvedValue(manifest);
    const admit = vi
      .spyOn(api, "admit")
      .mockRejectedValue(new Error("Admission unavailable"));
    const view = render(() => <App />);
    await waitFor(() =>
      expect(view.getByLabelText(/limit/)).toBeInTheDocument(),
    );
    fireEvent.input(view.getByLabelText(/limit/), { target: { value: "5" } });
    fireEvent.input(view.getByLabelText("Operation"), {
      target: { value: "execute" },
    });
    fireEvent.click(view.getByRole("button", { name: "Review invocation" }));
    expect(admit).not.toHaveBeenCalled();
    fireEvent.click(view.getByRole("button", { name: "Submit run" }));
    await waitFor(() =>
      expect(view.getByRole("alert")).toHaveTextContent(
        "Admission unavailable",
      ),
    );
    fireEvent.click(view.getByRole("button", { name: "Submit run" }));
    await waitFor(() => expect(admit).toHaveBeenCalledTimes(2));
    expect(admit.mock.calls[0]![3]).toEqual(admit.mock.calls[1]![3]);
  });

  it("distinguishes completed shadow runs from applied projections", async () => {
    setAccessToken("secret");
    window.history.replaceState({}, "", "/dq/runs/run-1");
    vi.spyOn(api, "manifest").mockResolvedValue(manifest);
    vi.spyOn(api, "run").mockResolvedValue({
      run_id: "run-1",
      processor_id: manifest.processor_id,
      processor_version: manifest.version,
      status: "succeeded",
      execution_mode: "shadow",
      projection_applied: false,
      state_revision: 4,
      input_refs: [],
      output_refs: [],
      parameters: {},
      created_at: "2026-01-01T00:00:00Z",
      deadline_at: "2026-01-01T00:10:00Z",
    } as unknown as Run);
    const view = render(() => <App />);
    await waitFor(() =>
      expect(view.getByText(/Run completed/)).toHaveTextContent(
        "Projection application is not confirmed.",
      ),
    );
  });
});
