// Package contract defines the bounded control messages shared with Octopus.
package contract

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"
)

const MaxRecordBytes = 64 * 1024
const ProtocolVersion = "dq.v1"

type ArtifactRef struct {
	ArtifactID        string `json:"artifact_id"`
	InstallationID    string `json:"installation_id"`
	Namespace         string `json:"namespace"`
	URI               string `json:"uri"`
	VersionID         string `json:"version_id"`
	ETag              string `json:"etag"`
	SHA256            string `json:"sha256"`
	SchemaID          string `json:"schema_id"`
	MediaType         string `json:"media_type"`
	SizeBytes         int64  `json:"size_bytes"`
	ProducerRunID     string `json:"producer_run_id,omitempty"`
	ProducerAttemptID string `json:"producer_attempt_id,omitempty"`
}

type Binding struct {
	Name          string `json:"name"`
	Direction     string `json:"direction"`
	Transport     string `json:"transport"`
	Topic         string `json:"topic,omitempty"`
	Bucket        string `json:"bucket,omitempty"`
	AllowedPrefix string `json:"allowed_prefix,omitempty"`
	SchemaID      string `json:"schema_id"`
	Required      bool   `json:"required"`
	MaxItems      int    `json:"max_items"`
	MaxBytes      int64  `json:"max_bytes"`
	Semantics     string `json:"semantics"`
}

type ResourceLimits struct {
	Concurrency    int   `json:"concurrency"`
	TimeoutSeconds int   `json:"timeout_seconds"`
	MaxInputBytes  int64 `json:"max_input_bytes"`
	MaxOutputBytes int64 `json:"max_output_bytes"`
}

type UIRemote struct {
	ImmutableURL    string `json:"immutable_url"`
	BundleDigest    string `json:"bundle_digest"`
	HostAPIVersion  string `json:"host_api_version"`
	ParameterForm   bool   `json:"parameter_form,omitempty"`
	RunInspector    bool   `json:"run_inspector,omitempty"`
	ArtifactPreview bool   `json:"artifact_preview,omitempty"`
}

type Manifest struct {
	InstallationID        string          `json:"installation_id"`
	Namespace             string          `json:"namespace"`
	ProcessorID           string          `json:"processor_id"`
	Version               string          `json:"version"`
	OwnerTeam             string          `json:"owner_team"`
	ImplementationDigest  string          `json:"implementation_digest"`
	WorkerService         string          `json:"worker_service"`
	ProtocolVersion       string          `json:"protocol_version"`
	ManifestSHA256        string          `json:"manifest_sha256"`
	ParameterSchema       json.RawMessage `json:"parameter_schema"`
	ParameterSchemaSHA256 string          `json:"parameter_schema_sha256"`
	Inputs                []Binding       `json:"inputs"`
	Outputs               []Binding       `json:"outputs"`
	Resources             ResourceLimits  `json:"resources"`
	PolicyVersion         string          `json:"policy_version"`
	UIRemote              *UIRemote       `json:"ui_remote,omitempty"`
}

type Processor struct {
	InstallationID string `json:"installation_id"`
	Namespace      string `json:"namespace"`
	ProcessorID    string `json:"processor_id"`
	OwnerTeam      string `json:"owner_team"`
	Enabled        bool   `json:"enabled"`
	DefaultVersion string `json:"default_version"`
	BetaPolicy     string `json:"beta_policy"`
}

type AdmitRequest struct {
	InstallationID   string          `json:"installation_id"`
	Namespace        string          `json:"namespace"`
	ProcessorID      string          `json:"processor_id"`
	ProcessorVersion string          `json:"processor_version,omitempty"`
	Operation        string          `json:"operation"`
	Parameters       json.RawMessage `json:"parameters"`
	IdempotencyKey   string          `json:"idempotency_key"`
	DeadlineAt       *time.Time      `json:"deadline_at,omitempty"`
	Traceparent      string          `json:"traceparent,omitempty"`
	Tracestate       string          `json:"tracestate,omitempty"`
}

type Attempt struct {
	AttemptID        string     `json:"attempt_id"`
	RunID            string     `json:"run_id"`
	AttemptNumber    int        `json:"attempt_number"`
	WorkerInstanceID string     `json:"worker_instance_id"`
	WorkerSlot       int        `json:"worker_slot"`
	LeaseToken       string     `json:"lease_token"`
	Fence            int64      `json:"fence"`
	LeaseExpiresAt   time.Time  `json:"lease_expires_at"`
	Status           string     `json:"status"`
	StartedAt        *time.Time `json:"started_at,omitempty"`
	FinishedAt       *time.Time `json:"finished_at,omitempty"`
}

type Run struct {
	InstallationID       string          `json:"installation_id"`
	Namespace            string          `json:"namespace"`
	RunID                string          `json:"run_id"`
	ProcessorID          string          `json:"processor_id"`
	ProcessorVersion     string          `json:"processor_version"`
	ImplementationDigest string          `json:"implementation_digest"`
	Operation            string          `json:"operation"`
	Parameters           json.RawMessage `json:"parameters"`
	ParametersSHA256     string          `json:"parameters_sha256"`
	PolicyVersion        string          `json:"policy_version"`
	IdempotencyKey       string          `json:"idempotency_key,omitempty"`
	RequestFingerprint   string          `json:"request_fingerprint,omitempty"`
	InputRefs            []ArtifactRef   `json:"input_refs"`
	OutputRefs           []ArtifactRef   `json:"output_refs"`
	SourceRevisionDigest string          `json:"source_revision_digest"`
	ReleaseChannel       string          `json:"release_channel"`
	ExecutionMode        string          `json:"execution_mode"`
	Status               string          `json:"status"`
	StateRevision        int64           `json:"state_revision"`
	CurrentAttempt       *Attempt        `json:"current_attempt,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	DeadlineAt           time.Time       `json:"deadline_at"`
	CompletedAt          *time.Time      `json:"completed_at,omitempty"`
	ProjectionApplied    bool            `json:"projection_applied"`
	Traceparent          string          `json:"traceparent,omitempty"`
	Tracestate           string          `json:"tracestate,omitempty"`
	ErrorCode            string          `json:"error_code,omitempty"`
	SanitizedError       string          `json:"sanitized_error,omitempty"`
}

type Envelope struct {
	SchemaVersion        int             `json:"schema_version"`
	EventID              string          `json:"event_id"`
	EventType            string          `json:"event_type"`
	InstallationID       string          `json:"installation_id"`
	Namespace            string          `json:"namespace"`
	RunID                string          `json:"run_id"`
	AttemptID            string          `json:"attempt_id,omitempty"`
	ProcessorID          string          `json:"processor_id"`
	ProcessorVersion     string          `json:"processor_version"`
	Fence                int64           `json:"fence,omitempty"`
	StateRevision        int64           `json:"state_revision,omitempty"`
	OccurredAt           time.Time       `json:"occurred_at"`
	CorrelationID        string          `json:"correlation_id"`
	CausationID          string          `json:"causation_id,omitempty"`
	Traceparent          string          `json:"traceparent,omitempty"`
	Tracestate           string          `json:"tracestate,omitempty"`
	Parameters           json.RawMessage `json:"parameters,omitempty"`
	Artifacts            []ArtifactRef   `json:"artifacts"`
	SourceRevisionDigest string          `json:"source_revision_digest,omitempty"`
	Sequence             int64           `json:"sequence,omitempty"`
	Summary              json.RawMessage `json:"summary,omitempty"`
	ErrorCode            string          `json:"error_code,omitempty"`
	ErrorClass           string          `json:"error_class,omitempty"`
}

type Scope struct {
	InstallationID string `json:"installation_id"`
	Namespace      string `json:"namespace"`
}
type ReservationRequest struct {
	Scope
	WorkerInstanceID string `json:"worker_instance_id"`
	WorkerSlot       int    `json:"worker_slot"`
}
type Reservation struct {
	WorkerAddress string   `json:"worker_address"`
	Run           Run      `json:"run"`
	Attempt       Attempt  `json:"attempt"`
	Manifest      Manifest `json:"manifest"`
}
type Dispatch struct {
	Run      Run      `json:"run"`
	Manifest Manifest `json:"manifest"`
	Attempt  *Attempt `json:"attempt,omitempty"`
}
type Dispatches struct {
	Items []Dispatch `json:"items"`
}
type LeaseMutation struct {
	Scope
	RunID            string `json:"run_id"`
	WorkerInstanceID string `json:"worker_instance_id"`
	LeaseToken       string `json:"lease_token"`
	Fence            int64  `json:"fence"`
}
type Heartbeat struct {
	Scope
	ProcessorID          string `json:"processor_id"`
	ProcessorVersion     string `json:"processor_version"`
	ImplementationDigest string `json:"implementation_digest"`
	WorkerInstanceID     string `json:"worker_instance_id"`
	Address              string `json:"address"`
	Concurrency          int    `json:"concurrency"`
	Ready                bool   `json:"ready"`
}
type Receipt struct {
	Scope
	ConsumerGroup    string    `json:"consumer_group"`
	Topic            string    `json:"topic"`
	Partition        int32     `json:"partition"`
	Offset           int64     `json:"offset"`
	EventID          string    `json:"event_id"`
	PayloadSHA256    string    `json:"payload_sha256"`
	Envelope         *Envelope `json:"envelope,omitempty"`
	QuarantineReason string    `json:"quarantine_reason,omitempty"`
}

// RunRequest contains an Octopus reservation, not caller-controlled resources.
type RunRequest struct {
	Scope
	RunID                string          `json:"run_id"`
	AttemptID            string          `json:"attempt_id"`
	Fence                int64           `json:"fence"`
	WorkerInstanceID     string          `json:"worker_instance_id"`
	WorkerSlot           int             `json:"worker_slot"`
	ProcessorID          string          `json:"processor_id"`
	ProcessorVersion     string          `json:"processor_version"`
	ImplementationDigest string          `json:"implementation_digest"`
	Operation            string          `json:"operation"`
	Parameters           json.RawMessage `json:"parameters"`
	InputRefs            []ArtifactRef   `json:"input_refs"`
	PolicyVersion        string          `json:"policy_version"`
	SourceRevisionDigest string          `json:"source_revision_digest"`
	DeadlineAt           time.Time       `json:"deadline_at"`
	LeaseExpiresAt       time.Time       `json:"lease_expires_at"`
	Traceparent          string          `json:"traceparent,omitempty"`
	Tracestate           string          `json:"tracestate,omitempty"`
}
type InitRequest struct {
	ProtocolVersion      string `json:"protocol_version"`
	ProcessorID          string `json:"processor_id"`
	ProcessorVersion     string `json:"processor_version"`
	ImplementationDigest string `json:"implementation_digest"`
}
type InitResponse struct {
	WorkerInstanceID     string `json:"worker_instance_id"`
	ProtocolVersion      string `json:"protocol_version"`
	ProcessorID          string `json:"processor_id"`
	ProcessorVersion     string `json:"processor_version"`
	ImplementationDigest string `json:"implementation_digest"`
	Concurrency          int    `json:"concurrency"`
}
type RunResponse struct {
	AttemptID string `json:"attempt_id"`
	Fence     int64  `json:"fence"`
	Status    string `json:"status"`
}
type CancelRequest struct {
	Scope
	RunID     string `json:"run_id"`
	AttemptID string `json:"attempt_id"`
	Fence     int64  `json:"fence"`
}
type CancelResponse struct {
	Status string `json:"status"`
}
type HealthResponse struct {
	InitResponse
	Ready          bool  `json:"ready"`
	AvailableSlots []int `json:"available_slots"`
}
type OutputManifest struct {
	SchemaID             string          `json:"schema_id"`
	OutputRefs           []ArtifactRef   `json:"output_refs"`
	Summary              json.RawMessage `json:"summary"`
	SourceRevisionDigest string          `json:"source_revision_digest"`
}

func SHA256(data []byte) string { digest := sha256.Sum256(data); return hex.EncodeToString(digest[:]) }
func ValidSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && value == strings.ToLower(value)
}
func ValidSegment(value string) bool {
	if value == "" || len(value) > 200 || value == "." || value == ".." {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '-' || ch == '_' || ch == '.') {
			return false
		}
	}
	return true
}

func EncodeEnvelope(envelope Envelope) ([]byte, error) {
	if envelope.SchemaVersion != 1 || envelope.EventID == "" || envelope.RunID == "" || envelope.EventType == "" || envelope.InstallationID == "" || envelope.Namespace == "" {
		return nil, errors.New("invalid envelope identity")
	}
	for _, ref := range envelope.Artifacts {
		if err := ref.Validate(); err != nil {
			return nil, err
		}
	}
	data, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(data) > MaxRecordBytes {
		return nil, errors.New("control record exceeds 64 KiB")
	}
	return data, nil
}
func (ref ArtifactRef) Validate() error {
	if !ValidSegment(ref.ArtifactID) || !ValidSegment(ref.InstallationID) || !ValidSegment(ref.Namespace) || ref.VersionID == "" || ref.VersionID == "null" || ref.ETag == "" || !ValidSHA256(ref.SHA256) || ref.SizeBytes < 0 || ref.SchemaID == "" || ref.MediaType == "" {
		return errors.New("invalid versioned artifact reference")
	}
	u, err := url.Parse(ref.URI)
	if err != nil || u.Scheme != "s3" || u.Opaque != "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Host == "" || u.RawPath != "" || strings.Contains(u.Path, "%") {
		return errors.New("invalid artifact URI")
	}
	key := strings.TrimPrefix(u.Path, "/")
	if key == "" {
		return errors.New("invalid artifact URI")
	}
	for _, segment := range strings.Split(key, "/") {
		if !ValidSegment(segment) {
			return errors.New("invalid artifact URI")
		}
	}
	return nil
}
