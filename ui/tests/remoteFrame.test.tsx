import { cleanup, render } from '@solidjs/testing-library';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { RemoteFrame } from '../src/RemoteFrame';
import { hostAPIVersion } from '../src/remoteProtocol';
import type { Manifest, Run } from '../src/types';

vi.mock('../src/session', () => ({ approvedRemoteOrigin: 'https://remote.example' }));
const digest = 'a'.repeat(64);
const manifest: Manifest = { installation_id: 'test', namespace: 'audit', processor_id: 'atheros.dedup', version: '0.1.0', owner_team: 'test', implementation_digest: digest, policy_version: 'v1', parameter_schema: { type: 'object', required: ['limit'], properties: { limit: { type: 'integer', minimum: 1, maximum: 10 } } }, ui_remote: { immutable_url: `https://remote.example/atheros.dedup/0.1.0/${digest}/index.html`, bundle_digest: digest, host_api_version: hostAPIVersion, parameter_form: true, run_inspector: true } };

beforeEach(() => vi.useFakeTimers());
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });

function send(frame: HTMLIFrameElement, operation: string, payload: unknown, id: string) {
  window.dispatchEvent(new MessageEvent('message', { source: frame.contentWindow, origin: 'null', data: { version: hostAPIVersion, channel: new URL(frame.src).searchParams.get('channel'), kind: 'request', request_id: id, operation, payload } }));
}

describe('extension fallback and host actions', () => {
  it('removes timed-out sandbox frames while preserving the generic view', () => {
    const view = render(() => <><p>Generic form available</p><RemoteFrame manifest={manifest} mode="parameter_form" /></>);
    const frame = view.getByTitle('atheros.dedup parameter form');
    expect(frame).toHaveAttribute('sandbox', 'allow-scripts');
    vi.advanceTimersByTime(5001);
    expect(view.queryByTitle('atheros.dedup parameter form')).toBeNull();
    expect(view.getByText(/extension timed out/)).toBeInTheDocument();
    expect(view.getByText('Generic form available')).toBeInTheDocument();
  });

  it('accepts bounded schema-checked parameters but leaves submission in the host', () => {
    const parameters = vi.fn();
    const view = render(() => <RemoteFrame manifest={manifest} mode="parameter_form" onParameters={parameters} />);
    const frame = view.getByTitle('atheros.dedup parameter form') as HTMLIFrameElement;
    const reply = vi.spyOn(frame.contentWindow!, 'postMessage');
    send(frame, 'ready', {}, 'ready-1');
    send(frame, 'set_parameters', { limit: 11 }, 'bad-1');
    expect(parameters).not.toHaveBeenCalled();
    send(frame, 'set_parameters', { limit: 5 }, 'good-1');
    send(frame, 'set_parameters', { limit: 7 }, 'good-1');
    expect(parameters).toHaveBeenCalledExactlyOnceWith({ limit: 5 });
    expect(reply.mock.calls.at(-1)?.[0].payload).toEqual({ accepted: true, submitted: false });
    expect(JSON.stringify(reply.mock.calls)).not.toContain('Bearer');
  });

  it('only exposes the bound run and strips fencing credentials', () => {
    const run = { run_id: 'run-1', status: 'running', parameters: {}, current_attempt: { attempt_id: 'attempt-1', attempt_number: 1, status: 'running', lease_expires_at: '2026-01-01T00:01:00Z', lease_token: 'secret-fence' } } as unknown as Run;
    const view = render(() => <RemoteFrame manifest={manifest} mode="run_inspector" run={run} />);
    const frame = view.getByTitle('atheros.dedup run inspector') as HTMLIFrameElement;
    const reply = vi.spyOn(frame.contentWindow!, 'postMessage');
    send(frame, 'ready', {}, 'ready-1');
    send(frame, 'inspect_run', { run_id: 'outside' }, 'outside-1');
    expect(reply.mock.calls.at(-1)?.[0].error).toContain('outside the extension scope');
    send(frame, 'inspect_run', { run_id: 'run-1' }, 'inside-1');
    expect(reply.mock.calls.at(-1)?.[0].payload.run_id).toBe('run-1');
    expect(JSON.stringify(reply.mock.calls)).not.toContain('secret-fence');
  });
});
