package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalWireFixture(t *testing.T) {
	input := []byte(`{ "z": [3, {"b":"<>&","a":"snowman: \u2603"}], "a": 1.0 }`)
	want := `{"a":1,"z":[3,{"a":"snowman: ☃","b":"<>&"}]}`
	got, err := CanonicalJSON(input)
	if err != nil || string(got) != want {
		t.Fatalf("canonical %s %v", got, err)
	}
	if _, err = CanonicalJSON([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Fatal("duplicate name accepted")
	}
	if _, err = CanonicalJSON([]byte(`{} {}`)); err == nil {
		t.Fatal("trailing document accepted")
	}
}

func TestExactNumericCanonicalizationSurvivesDatabaseSpellings(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`{"n":1e3}`, `{"n":1000}`},
		{`{"n":1000.000}`, `{"n":1000}`},
		{`{"n":1.00}`, `{"n":1}`},
		{`{"n":-0.000e20}`, `{"n":0}`},
		{`{"n":9.0071992547409931234567890123456789e20}`, `{"n":900719925474099312345.67890123456789}`},
		{`{"nested":[1e-3,-12.3400e-2,100e-2]}`, `{"nested":[0.001,-0.1234,1]}`},
	} {
		got, err := CanonicalJSON([]byte(tc.input))
		if err != nil || string(got) != tc.want {
			t.Fatalf("%s canonicalized to %s: %v", tc.input, got, err)
		}
		stable, err := CanonicalJSON(got)
		if err != nil || string(stable) != tc.want {
			t.Fatalf("canonical output was not stable: %s %v", stable, err)
		}
	}
	for _, input := range []string{`{"n":1e99999999999999999999999}`, `{"n":1e65536}`, `{"n":[1e40000,1e40000]}`} {
		if _, err := CanonicalJSON([]byte(input)); err == nil {
			t.Fatalf("unbounded exact decimal expansion accepted: %s", input)
		}
	}
}
func TestEnvelopeCannotInlineLargeRecordsOrInvalidArtifacts(t *testing.T) {
	envelope := Envelope{SchemaVersion: 1, EventID: "event", EventType: "progress", InstallationID: "i", Namespace: "n", RunID: "run", Artifacts: []ArtifactRef{}}
	envelope.Summary = json.RawMessage(`{"text":"` + strings.Repeat("x", MaxRecordBytes) + `"}`)
	if _, err := EncodeEnvelope(envelope); err == nil {
		t.Fatal("unbounded envelope accepted")
	}
	envelope.Summary = nil
	envelope.Artifacts = []ArtifactRef{{ArtifactID: "artifact", InstallationID: "i", Namespace: "n", URI: "s3://bucket/key", VersionID: "version", ETag: "etag", SHA256: strings.Repeat("a", 64), SchemaID: "schema", MediaType: "application/json"}}
	envelope.Artifacts[0].URI = "s3://user@bucket/key?credentials=secret"
	if _, err := EncodeEnvelope(envelope); err == nil {
		t.Fatal("invalid artifact accepted")
	}
}
