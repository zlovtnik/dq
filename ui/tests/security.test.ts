import { describe, expect, it } from "vitest";
import {
  hostAPIVersion,
  pinnedRemoteURL,
  validateRemoteRequest,
} from "../src/remoteProtocol";
import { checkParameters, fieldValue } from "../src/schema";
import { parseRoute } from "../src/App";
import type { Manifest } from "../src/types";

const digest = "a".repeat(64);
const manifest = {
  processor_id: "atheros.dedup",
  version: "0.1.0",
  ui_remote: {
    immutable_url: `https://remote.example/atheros.dedup/0.1.0/${digest}/index.html`,
    bundle_digest: digest,
    host_api_version: hostAPIVersion,
  },
} as Manifest;

describe("pinned extension boundary", () => {
  it("requires a separate approved origin and exact processor version/digest path", () => {
    expect(
      pinnedRemoteURL(
        manifest,
        "https://remote.example",
        "https://host.example",
      ),
    ).toBeDefined();
    expect(
      pinnedRemoteURL(
        manifest,
        "https://other.example",
        "https://host.example",
      ),
    ).toBeUndefined();
    expect(
      pinnedRemoteURL(
        manifest,
        "https://remote.example",
        "https://remote.example",
      ),
    ).toBeUndefined();
    for (const url of [
      "https://remote.example/arbitrary.js",
      manifest.ui_remote!.immutable_url + "?next=evil",
      manifest.ui_remote!.immutable_url.replace(digest, "b".repeat(64)),
    ]) {
      expect(
        pinnedRemoteURL(
          {
            ...manifest,
            ui_remote: { ...manifest.ui_remote!, immutable_url: url },
          },
          "https://remote.example",
          "https://host.example",
        ),
      ).toBeUndefined();
    }
  });

  it("binds opaque-origin messages to the selected frame, channel, version and operation", () => {
    const frame = {} as Window;
    const data = {
      version: hostAPIVersion,
      channel: "channel-1",
      request_id: "r1",
      kind: "request",
      operation: "inspect_run",
      payload: { run_id: "run-1" },
    };
    const event = { origin: "null", source: frame, data };
    expect(validateRemoteRequest(event, frame, "channel-1")).toEqual(data);
    expect(
      validateRemoteRequest(
        { ...event, source: {} as Window },
        frame,
        "channel-1",
      ),
    ).toBeUndefined();
    expect(
      validateRemoteRequest(
        { ...event, origin: "https://remote.example" },
        frame,
        "channel-1",
      ),
    ).toBeUndefined();
    for (const patch of [
      { channel: "other" },
      { operation: "fetch" },
      { kind: "response" },
      { version: "v2" },
      { request_id: "../bad" },
      { bearer: "secret" },
      { payload: "x".repeat(65536) },
    ]) {
      expect(
        validateRemoteRequest(
          { ...event, data: { ...data, ...patch } },
          frame,
          "channel-1",
        ),
      ).toBeUndefined();
    }
  });
});

describe("generic form and routes", () => {
  it("checks integer bounds and required fields before host submission", () => {
    const schema = {
      type: "object",
      required: ["scope"],
      additionalProperties: false,
      properties: {
        scope: { type: "string" },
        limit: { type: "integer", minimum: 1, maximum: 1000 },
      },
    };
    expect(checkParameters(schema, { scope: "*", limit: 100 })).toEqual([]);
    expect(checkParameters(schema, { limit: 1001, extra: true })).toHaveLength(
      3,
    );
    expect(checkParameters(schema, { scope: "*", limit: 1.5 })).toHaveLength(1);
    expect(() => fieldValue({ type: "number" }, "Infinity")).toThrow();
  });
  it("resolves historical version routes and rejects malformed paths", () => {
    expect(parseRoute("/dq/processors/atheros.dedup/0.1.0/invoke")).toEqual({
      kind: "invoke",
      id: "atheros.dedup",
      version: "0.1.0",
    });
    expect(parseRoute("/dq/runs/run-1")).toEqual({ kind: "run", id: "run-1" });
    expect(parseRoute("/dq/artifacts/artifact-1")).toEqual({
      kind: "artifact",
      id: "artifact-1",
    });
    expect(parseRoute("/dq/atheros/dedup").kind).toBe("dedup");
    expect(parseRoute("/dq/runs/%bad").kind).toBe("missing");
  });
});
