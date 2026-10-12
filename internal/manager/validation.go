package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/url"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

const SchemaDialect = "https://json-schema.org/draft/2020-12/schema"

// CanonicalJSON is the wire canonical form: recursively sorted keys, no
// whitespace, exact plain decimal numbers, and no HTML escaping. Duplicate names are
// rejected rather than allowing different validators to interpret them.
func CanonicalJSON(data []byte) ([]byte, error) {
	return contract.CanonicalJSON(data)
}

type denySchemaLoader struct{}

func (denySchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external schema references are forbidden")
}

func CompileParameters(schemaJSON []byte) (*jsonschema.Schema, error) {
	canonical, err := CanonicalJSON(schemaJSON)
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(canonical))
	if err != nil {
		return nil, errors.New("invalid parameter schema")
	}
	obj, ok := doc.(map[string]any)
	if !ok || obj["$schema"] != SchemaDialect {
		return nil, errors.New("JSON Schema 2020-12 required")
	}
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(denySchemaLoader{})
	c.AssertFormat()
	const resource = "urn:dq:parameter-schema"
	if err := c.AddResource(resource, doc); err != nil {
		return nil, errors.New("invalid parameter schema")
	}
	schema, err := c.Compile(resource)
	if err != nil {
		return nil, errors.New("invalid or external parameter schema")
	}
	return schema, nil
}

func ValidateParameters(m contract.Manifest, parameters []byte) ([]byte, error) {
	canonical, err := CanonicalJSON(parameters)
	if err != nil {
		return nil, err
	}
	schema, err := CompileParameters(m.ParameterSchema)
	if err != nil {
		return nil, err
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(canonical))
	if err != nil {
		return nil, errors.New("invalid parameters")
	}
	if _, ok := doc.(map[string]any); !ok {
		return nil, errors.New("parameters must be an object")
	}
	if err := schema.Validate(doc); err != nil {
		return nil, errors.New("parameters do not satisfy pinned schema")
	}
	return canonical, nil
}

func ValidateManifest(m contract.Manifest, approvedRemoteOrigin string) error {
	if !contract.ValidSegment(m.InstallationID) || !contract.ValidSegment(m.Namespace) || !contract.ValidSegment(m.ProcessorID) || !contract.ValidSegment(m.Version) || m.OwnerTeam == "" || m.WorkerService == "" || m.ProtocolVersion != contract.ProtocolVersion || m.PolicyVersion == "" {
		return errors.New("invalid processor manifest identity")
	}
	if !strings.HasPrefix(m.ImplementationDigest, "sha256:") || !contract.ValidSHA256(strings.TrimPrefix(m.ImplementationDigest, "sha256:")) {
		return errors.New("immutable implementation digest required")
	}
	if m.Resources.Concurrency < 1 || m.Resources.Concurrency > 128 || m.Resources.TimeoutSeconds < 1 || m.Resources.TimeoutSeconds > 3600 || m.Resources.MaxInputBytes < 1 || m.Resources.MaxOutputBytes < 1 {
		return errors.New("invalid processor resource limits")
	}
	schema, err := CanonicalJSON(m.ParameterSchema)
	if err != nil {
		return err
	}
	if contract.SHA256(schema) != m.ParameterSchemaSHA256 {
		return errors.New("parameter schema checksum mismatch")
	}
	if _, err := CompileParameters(schema); err != nil {
		return err
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return errors.New("invalid manifest")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &object); err != nil {
		return errors.New("invalid manifest")
	}
	delete(object, "manifest_sha256")
	encoded, _ = json.Marshal(object)
	canonical, err := CanonicalJSON(encoded)
	if err != nil {
		return err
	}
	if contract.SHA256(canonical) != m.ManifestSHA256 {
		return errors.New("manifest checksum mismatch")
	}
	names := map[string]bool{}
	for direction, bindings := range map[string][]contract.Binding{"input": m.Inputs, "output": m.Outputs} {
		for _, b := range bindings {
			if b.Direction != direction || !contract.ValidSegment(b.Name) || names[direction+"/"+b.Name] || b.SchemaID == "" || b.MaxItems < 1 || b.MaxItems > 10000 || b.MaxBytes < 1 {
				return errors.New("invalid binding limits")
			}
			names[direction+"/"+b.Name] = true
			if b.Semantics != "command" && b.Semantics != "metadata" && b.Semantics != "artifact_reference" {
				return errors.New("invalid binding semantics")
			}
			switch b.Transport {
			case "minio":
				if b.Bucket == "" || b.AllowedPrefix == "" || strings.Contains(b.AllowedPrefix, "..") || b.Semantics != "artifact_reference" {
					return errors.New("invalid artifact binding")
				}
			case "redpanda":
				if b.Topic == "" || b.Semantics == "command" && direction != "input" {
					return errors.New("invalid broker binding")
				}
			default:
				return errors.New("unsupported binding transport")
			}
		}
	}
	if m.UIRemote != nil {
		u, err := url.Parse(m.UIRemote.ImmutableURL)
		if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" || approvedRemoteOrigin == "" || u.Scheme+"://"+u.Host != approvedRemoteOrigin || !contract.ValidSHA256(m.UIRemote.BundleDigest) || m.UIRemote.HostAPIVersion != "dq.host.v1" || !strings.Contains(u.Path, "/"+m.UIRemote.BundleDigest+"/") {
			return errors.New("remote must use approved immutable origin and digest")
		}
	}
	return nil
}

func ParameterScope(parameters []byte) string {
	var p struct {
		Scope string `json:"authorized_scope"`
	}
	if json.Unmarshal(parameters, &p) != nil {
		return ""
	}
	return p.Scope
}
