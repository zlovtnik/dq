import { render } from 'solid-js/web';
import App from './App';
import { hostRequest } from './embeddedBridge';
import { configureEmbedded } from './session';
import './styles.css';

const root=document.getElementById('root');
if (root) {
  root.textContent='Loading DQ workspace...';
  void hostRequest<{enabled:boolean;beta_enabled:boolean;approved_remote_origin:string;bridge_version:number}>('context').then((context)=>{
    if (!context.enabled || context.bridge_version!==1) throw new Error('DQ is disabled or incompatible.');
    configureEmbedded(context);
    root.textContent='';
    render(()=><App />,root);
  }).catch(()=>{ root.textContent='DQ workspace is unavailable. Use Retry DQ in Atheros.'; });
}
