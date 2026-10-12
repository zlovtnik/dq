package manager

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

func fixtureManifest(t testing.TB) contract.Manifest {
	t.Helper()
	m := contract.Manifest{InstallationID: "test", Namespace: "default", ProcessorID: "test.processor", Version: "1.0.0", OwnerTeam: "tests", ImplementationDigest: "sha256:" + strings.Repeat("a", 64), WorkerService: "test-worker", ProtocolVersion: contract.ProtocolVersion, PolicyVersion: "v1", ParameterSchema: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","required":["authorized_scope","count"],"properties":{"authorized_scope":{"type":"string"},"count":{"type":"integer","minimum":1,"maximum":10}},"additionalProperties":false}`), Inputs: []contract.Binding{}, Outputs: []contract.Binding{}, Resources: contract.ResourceLimits{Concurrency: 1, TimeoutSeconds: 30, MaxInputBytes: 1024, MaxOutputBytes: 1024}}
	schema, err := CanonicalJSON(m.ParameterSchema)
	if err != nil {
		t.Fatal(err)
	}
	m.ParameterSchema = schema
	m.ParameterSchemaSHA256 = contract.SHA256(schema)
	b, _ := json.Marshal(m)
	var obj map[string]json.RawMessage
	_ = json.Unmarshal(b, &obj)
	delete(obj, "manifest_sha256")
	b, _ = json.Marshal(obj)
	canonical, err := CanonicalJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	m.ManifestSHA256 = contract.SHA256(canonical)
	return m
}

func TestCanonicalJSONRejectsAmbiguousOrUnboundedDocuments(t *testing.T) {
	for _, input := range []string{`{"scope":"a","scope":"b"}`, `{} {}`, `{"nested":{"x":1,"x":2}}`, strings.Repeat("[", 66) + "0" + strings.Repeat("]", 66), strings.Repeat(" ", contract.MaxRecordBytes+1)} {
		if _, err := CanonicalJSON([]byte(input)); err == nil {
			t.Fatalf("accepted ambiguous JSON: %q", input[:min(len(input), 80)])
		}
	}
	canonical, err := CanonicalJSON([]byte(`{ "z": {"b":2,"a":1},"a":"<>&" }`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"a":"<>&","z":{"a":1,"b":2}}` {
		t.Fatalf("unexpected canonical: %s", canonical)
	}
}

func TestManifestAndFullSchemaValidation(t *testing.T) {
	m := fixtureManifest(t)
	if err := ValidateManifest(m, ""); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`{"authorized_scope":"a","count":0}`, `{"authorized_scope":"a","count":1.5}`, `{"count":1}`, `{"authorized_scope":"a","count":1,"extra":true}`} {
		if _, err := ValidateParameters(m, []byte(bad)); err == nil {
			t.Fatalf("accepted invalid parameters %s", bad)
		}
	}
	canonical, err := ValidateParameters(m, []byte(`{"count":2,"authorized_scope":"a"}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(canonical) != `{"authorized_scope":"a","count":2}` {
		t.Fatal(string(canonical))
	}
	m.ImplementationDigest = "latest"
	if ValidateManifest(m, "") == nil {
		t.Fatal("mutable manifest accepted")
	}
}

func TestSchemaCannotReadNetworkOrLocalFiles(t *testing.T) {
	for _, ref := range []string{"https://example.com/schema", "file:///etc/passwd"} {
		schema := `{"$schema":"` + SchemaDialect + `","$ref":"` + ref + `"}`
		if _, err := CompileParameters([]byte(schema)); err == nil {
			t.Fatalf("external schema accepted %s", ref)
		}
	}
	// Draft 2020-12 unevaluatedProperties is enforced, not ignored by a partial
	// home-grown validator.
	schema := `{"$schema":"` + SchemaDialect + `","allOf":[{"properties":{"x":{"type":"integer"}}}],"unevaluatedProperties":false}`
	compiled, err := CompileParameters([]byte(schema))
	if err != nil {
		t.Fatal(err)
	}
	if compiled.Validate(map[string]any{"extra": true}) == nil {
		t.Fatal("unevaluatedProperties ignored")
	}
}

func BenchmarkValidatePinnedParameters(b *testing.B) {
	m := fixtureManifest(b)
	parameters := []byte(`{"authorized_scope":"floor-a","count":2}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ValidateParameters(m, parameters); err != nil {
			b.Fatal(err)
		}
	}
}
func BenchmarkCanonicalParameters(b *testing.B) {
	parameters := []byte(`{"count":2,"authorized_scope":"floor-a","options":{"c":true,"b":false,"a":3}}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := CanonicalJSON(parameters); err != nil {
			b.Fatal(err)
		}
	}
}
