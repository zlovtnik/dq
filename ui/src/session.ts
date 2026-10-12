import { createSignal } from "solid-js";
import type { Scope } from "./types";
export const [accessToken, setAccessToken] = createSignal("");
export const [scope, setScope] = createSignal<Scope>({
  installation_id: import.meta.env.VITE_DQ_INSTALLATION_ID || "",
  namespace: import.meta.env.VITE_DQ_NAMESPACE || "",
});
export const betaEnabled = import.meta.env.VITE_DQ_BETA_ENABLED === "true";
export const approvedRemoteOrigin =
  import.meta.env.VITE_DQ_APPROVED_REMOTE_ORIGIN || "";
export const reviewURL = import.meta.env.VITE_DQ_ATHEROS_REVIEW_URL || "";
// Session credentials stay in host memory and are cleared on sign-out or reload.
export function signOut() {
  setAccessToken("");
}
