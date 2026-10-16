import { cleanup, render, waitFor } from '@solidjs/testing-library';
import { afterEach, expect, it, vi } from 'vitest';

afterEach(()=>{ cleanup(); vi.restoreAllMocks(); vi.resetModules(); window.history.replaceState({},'', '/'); });
it('uses the host bridge without credentials or standalone controls',async()=>{
  window.history.replaceState({},'', '/dq-ui/embedded.html#channel=channel-1');
  vi.resetModules();
  const {default:App}=await import('../src/App');
  const {api}=await import('../src/api');
  const {disposeBridge}=await import('../src/embeddedBridge');
  vi.spyOn(api,'processors').mockResolvedValue({items:[]});
  const view=render(()=><App />);
  await waitFor(()=>expect(view.getByText('Processors',{selector:'h1'})).toBeVisible());
  expect(view.queryByLabelText('Bearer token')).toBeNull();expect(view.queryByText('Sign out')).toBeNull();expect(view.queryByText('Connect to Delip Queve')).toBeNull();
  disposeBridge();
});
