import { createSignal } from 'solid-js';

export const embedded = window.location.pathname === '/dq-ui/embedded.html';
const channel = new URLSearchParams(window.location.hash.slice(1)).get('channel') || '';
const origin = window.location.origin;
export const [embeddedPath,setEmbeddedPath] = createSignal('/dq/processors');
const pending = new Map<string,{resolve:(value:unknown)=>void;reject:(error:Error)=>void;cleanup:()=>void}>();
const bounded = (value:unknown) => {
  try { const json=JSON.stringify(value); return !!json && new TextEncoder().encode(json).length<=65536 && !/"(?:authorization|access_token|refresh_token|service_token|bearer|token|lease_token)"\s*:|Bearer\s/i.test(json); }
  catch { return false; }
};
function receive(event:MessageEvent) {
  if (event.source!==window.parent || event.origin!==origin || !bounded(event.data)) return;
  const message=event.data;
  if (!message || message.version!==1 || message.channel!==channel) return;
  if (message.type==='route' && typeof message.path==='string' && message.path.startsWith('/dq/')) { setEmbeddedPath(message.path); return; }
  if (message.type!=='response' || typeof message.request_id!=='string') return;
  const request=pending.get(message.request_id); if (!request) return;
  pending.delete(message.request_id); request.cleanup();
  if (message.error) request.reject(new Error(typeof message.error==='string' ? message.error : 'DQ request failed.'));
  else request.resolve(message.payload);
}
if (embedded) window.addEventListener('message',receive);
export function disposeBridge() {
  window.removeEventListener('message',receive);
  window.removeEventListener('pagehide',pageHide);
  for (const request of pending.values()) { request.cleanup(); request.reject(new Error('Workspace closed.')); }
  pending.clear();
}
function pageHide(event:PageTransitionEvent) {
  if (!event.persisted) disposeBridge();
}
if (embedded) window.addEventListener('pagehide',pageHide);
export function hostRequest<T>(operation:string,payload:Record<string,unknown>={},signal?:AbortSignal):Promise<T> {
  if (!embedded || window.parent===window || !/^[a-zA-Z0-9-]{1,200}$/.test(channel) || !bounded(payload) || signal?.aborted || pending.size>=32) return Promise.reject(new Error('Invalid host request.'));
  return new Promise<T>((resolve,reject)=>{
    const request_id=crypto.randomUUID();
    const abort=()=>{ const request=pending.get(request_id); if (!request) return; pending.delete(request_id); request.cleanup(); reject(new Error('DQ request timed out or was cancelled.')); };
    const timer=window.setTimeout(abort,12000);
    const cleanup=()=>{ window.clearTimeout(timer); signal?.removeEventListener('abort',abort); };
    pending.set(request_id,{resolve:(value)=>resolve(value as T),reject,cleanup});
    signal?.addEventListener('abort',abort,{once:true});
    const message={version:1,channel,type:'request',request_id,operation,payload};
    if (!bounded(message)) { abort(); return; }
    window.parent.postMessage(message,origin);
  });
}
export function embeddedAPI<T>(path:string,method:string,body:unknown,signal?:AbortSignal):Promise<T> {
  const parts=path.split('/');
  if (path==='/v1/processors' && method==='GET') return hostRequest<T>('catalog',{},signal);
  if (parts[2]==='processor-versions' && parts.length===5 && method==='GET') return hostRequest<T>('version',{id:decodeURIComponent(parts[3]!),version:decodeURIComponent(parts[4]!)},signal);
  if (path==='/v1/runs' && method==='POST') return hostRequest<T>('admit',body as Record<string,unknown>,signal);
  if ((parts[2]==='runs'||parts[2]==='artifacts') && parts.length===4 && method==='GET') return hostRequest<T>(parts[2]==='runs'?'run':'artifact',{id:decodeURIComponent(parts[3]!)},signal);
  if (parts.length===5 && method==='POST' && ['cancel','replay','access'].includes(parts[4]!)) return hostRequest<T>(parts[4]!,{...body as object,id:decodeURIComponent(parts[3]!)},signal);
  return Promise.reject(new Error('Unsupported host operation.'));
}
