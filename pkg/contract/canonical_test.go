package contract

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCanonicalWireFixture(t *testing.T) {
	input := []byte(`{ "z": [3, {"b":"<>&","a":"snowman: \u2603"}], "a": 1.0 }`)
	want := `{"a":1.0,"z":[3,{"a":"snowman: ☃","b":"<>&"}]}`
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
func TestEnvelopeCannotInlineLargeRecordsOrInvalidArtifacts(t *testing.T) {
	envelope := Envelope{SchemaVersion: 1, EventID: "event", EventType: "progress", InstallationID: "i", Namespace: "n", RunID: "run", Artifacts: []ArtifactRef{}}
	envelope.Summary = json.RawMessage(`{"text":"` + strings.Repeat("x", MaxRecordBytes) + `"}`)
	if _, err := EncodeEnvelope(envelope); err == nil {
		t.Fatal("unbounded envelope accepted")
	}
	envelope.Summary = nil
	envelope.Artifacts = []ArtifactRef{{URI: "https://example.invalid?credentials=secret"}}
	if _, err := EncodeEnvelope(envelope); err == nil {
		t.Fatal("invalid artifact accepted")
	}
}
