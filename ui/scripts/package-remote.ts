import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

// Release only these two immutable assets. The manager verifies the bundle on serving.
const processor = "atheros.dedup";
const version = "0.1.0";
const bundle = await readFile(resolve("remote-build/remote.js"));
const digest = createHash("sha256").update(bundle).digest("hex");
const directory = resolve("remotes-dist", processor, version, digest);
await mkdir(directory, { recursive: true });
const map = JSON.stringify({ imports: { "@dq/remote": "./remote.js" } });
const html = `<!doctype html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Atheros dedup extension</title>
<style>body{margin:0;padding:16px;font:14px/1.5 system-ui,sans-serif;color:#18283d;background:#f8fafc}h2{font-size:16px;margin:0 0 12px}label{display:grid;gap:6px}textarea,button{font:inherit;border:1px solid #687a92;border-radius:4px;padding:10px;max-width:100%;box-sizing:border-box}textarea{width:100%;font-family:monospace}button{margin-top:12px;min-height:44px;cursor:pointer}button:disabled{opacity:.7;cursor:wait}:focus-visible{outline:3px solid #2563eb;outline-offset:2px}pre{white-space:pre-wrap;overflow-wrap:anywhere;font-size:12px}p{overflow-wrap:anywhere}@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}</style>
<script type="importmap">${map}</script></head><body><div id="remote-root"></div><script type="module">import '@dq/remote';</script></body></html>`;
await writeFile(resolve(directory, "remote.js"), bundle);
await writeFile(resolve(directory, "index.html"), html);
await writeFile(
  resolve("remotes-dist/release.json"),
  JSON.stringify(
    {
      processor_id: processor,
      version,
      bundle_digest: digest,
      host_api_version: "dq.host.v1",
      immutable_path: `/${processor}/${version}/${digest}/index.html`,
      parameter_form: true,
      run_inspector: true,
      artifact_preview: true,
    },
    null,
    2,
  ) + "\n",
);
console.log(`Remote release: ${processor}/${version}/${digest}/index.html`);
