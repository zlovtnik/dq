export interface ArtifactRef {
  artifact_id: string;
  installation_id: string;
  namespace: string;
  uri: string;
  version_id: string;
  etag: string;
  sha256: string;
  schema_id: string;
  media_type: string;
  size_bytes: number;
  producer_run_id?: string;
}
export interface FieldSchema {
  type?: string;
  title?: string;
  description?: string;
  enum?: unknown[];
  default?: unknown;
  minimum?: number;
  maximum?: number;
  minLength?: number;
  maxLength?: number;
  properties?: Record<string, FieldSchema>;
  required?: string[];
  additionalProperties?: boolean;
}
export interface RemoteManifest {
  immutable_url: string;
  bundle_digest: string;
  host_api_version: string;
  parameter_form?: boolean;
  run_inspector?: boolean;
  artifact_preview?: boolean;
}
export interface Manifest {
  installation_id: string;
  namespace: string;
  processor_id: string;
  version: string;
  owner_team: string;
  implementation_digest: string;
  policy_version: string;
  parameter_schema: FieldSchema;
  ui_remote?: RemoteManifest;
}
export interface Run {
  run_id: string;
  installation_id: string;
  namespace: string;
  processor_id: string;
  processor_version: string;
  operation: string;
  parameters: Record<string, unknown>;
  implementation_digest: string;
  policy_version: string;
  status: string;
  state_revision: number;
  execution_mode: "shadow" | "enabled";
  projection_applied: boolean;
  created_at: string;
  deadline_at: string;
  completed_at?: string;
  sanitized_error?: string;
  error_code?: string;
  input_refs: ArtifactRef[];
  output_refs: ArtifactRef[];
  current_attempt?: {
    attempt_id: string;
    attempt_number: number;
    status: string;
    lease_expires_at: string;
  };
}
export interface Scope {
  installation_id: string;
  namespace: string;
}
export const terminal = (status: string) =>
  ["succeeded", "failed", "cancelled"].includes(status);
