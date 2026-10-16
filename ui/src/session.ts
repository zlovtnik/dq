import { createSignal } from "solid-js";
import type { Scope } from "./types";
export const [accessToken, setAccessToken] = createSignal("");
export const [scope, setScope] = createSignal<Scope>({
  installation_id: import.meta.env.VITE_DQ_INSTALLATION_ID || "",
  namespace: import.meta.env.VITE_DQ_NAMESPACE || "",
});
export let betaEnabled = import.meta.env.VITE_DQ_BETA_ENABLED === "true";
export let approvedRemoteOrigin =
  import.meta.env.VITE_DQ_APPROVED_REMOTE_ORIGIN || "";
export const reviewURL = import.meta.env.VITE_DQ_ATHEROS_REVIEW_URL || "";
export function configureEmbedded(context: {beta_enabled:boolean;approved_remote_origin:string}) {
  betaEnabled=context.beta_enabled;
  approvedRemoteOrigin=context.approved_remote_origin;
}
// Session credentials stay in host memory and are cleared on sign-out or reload.
export function signOut() {
  setAccessToken("");
}
