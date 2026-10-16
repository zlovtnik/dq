import { afterEach, expect, it, vi } from 'vitest';

let dispose: (() => void) | undefined;
afterEach(() => {
  dispose?.();
  dispose = undefined;
  vi.resetModules();
  window.history.replaceState({}, '', '/');
});

async function bridge() {
  window.history.replaceState({}, '', '/dq-ui/embedded.html#channel=channel-1');
  const value = await import('../src/embeddedBridge');
  dispose = value.disposeBridge;
  return value;
}

function route(path: string) {
  window.dispatchEvent(new MessageEvent('message', {
    source: window.parent,
    origin: window.location.origin,
    data: { version: 1, channel: 'channel-1', type: 'route', path },
  }));
}

it('receives host routes after repeated back-forward cache restores', async () => {
  const { embeddedPath } = await bridge();
  for (const path of ['/dq/runs/one', '/dq/runs/two']) {
    window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted: true }));
    window.dispatchEvent(new PageTransitionEvent('pageshow', { persisted: true }));
    route(path);
    expect(embeddedPath()).toBe(path);
  }
  window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted: false }));
  route('/dq/runs/closed');
  expect(embeddedPath()).toBe('/dq/runs/two');
});

it('removes the message listener on ordinary page departure', async () => {
  const { embeddedPath } = await bridge();
  window.dispatchEvent(new PageTransitionEvent('pagehide', { persisted: false }));
  route('/dq/runs/closed');
  expect(embeddedPath()).toBe('/dq/processors');
});
