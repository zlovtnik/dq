package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

type receipts struct {
	entries []contract.Receipt
	fail    bool
	reject  error
}

func (s *receipts) Do(_ context.Context, method, path string, _ url.Values, body, out any) error {
	if method != "POST" || path != "/receipts" {
		return errors.New("unexpected request")
	}
	if s.reject != nil {
		return s.reject
	}
	if s.fail {
		return errors.New("storage unavailable")
	}
	s.entries = append(s.entries, body.(contract.Receipt))
	return nil
}

func validRecord(t *testing.T) Record {
	t.Helper()
	data, err := contract.EncodeEnvelope(contract.Envelope{SchemaVersion: 1, EventID: "event-1", EventType: "invoke", InstallationID: "test", Namespace: "ns", RunID: "run-1", ProcessorID: "test.processor", ProcessorVersion: "1.0.0", OccurredAt: time.Now().UTC(), CorrelationID: "run-1", Artifacts: []contract.ArtifactRef{}})
	if err != nil {
		t.Fatal(err)
	}
	return Record{Topic: CommandTopic, Partition: 0, Offset: 7, Key: []byte("test/ns/run-1"), Value: data}
}
func TestReceiptAndQuarantinePrecedeCommit(t *testing.T) {
	for _, malformed := range []bool{false, true} {
		store := &receipts{}
		c := Consumer{Control: store, Scope: contract.Scope{InstallationID: "test", Namespace: "ns"}, Group: "test-group"}
		record := validRecord(t)
		if malformed {
			record.Value = []byte(`{"token":"never-persist"`)
		}
		commits := 0
		commit := func(context.Context) error {
			if len(store.entries) == 0 {
				t.Fatal("committed before durable receipt")
			}
			commits++
			return nil
		}
		store.fail = true
		if c.ProcessRecord(context.Background(), record, commit) == nil || commits != 0 {
			t.Fatal("receipt outage advanced offset")
		}
		store.fail = false
		if err := c.ProcessRecord(context.Background(), record, commit); err != nil {
			t.Fatal(err)
		}
		r := store.entries[0]
		if r.Offset != 7 || r.Partition != 0 || r.ConsumerGroup != "test-group" || r.PayloadSHA256 != contract.SHA256(record.Value) {
			t.Fatal("receipt evidence incomplete")
		}
		if malformed {
			encoded, _ := json.Marshal(r)
			if r.QuarantineReason == "" || r.Envelope != nil || strings.Contains(string(encoded), "never-persist") {
				t.Fatal("quarantine leaked raw record")
			}
		}
	}
}
func TestCommitFailureCanRedeliverIdenticalDurableEvidence(t *testing.T) {
	store := &receipts{}
	c := Consumer{Control: store, Scope: contract.Scope{InstallationID: "test", Namespace: "ns"}, Group: "test"}
	record := validRecord(t)
	if c.ProcessRecord(context.Background(), record, func(context.Context) error { return errors.New("lost commit") }) == nil {
		t.Fatal("commit error hidden")
	}
	if err := c.ProcessRecord(context.Background(), record, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if len(store.entries) != 2 || store.entries[0].PayloadSHA256 != store.entries[1].PayloadSHA256 || store.entries[0].EventID != store.entries[1].EventID {
		t.Fatal("redelivery changed durable identity")
	}
}
func TestQuarantineBoundaries(t *testing.T) {
	for _, mutate := range []func(*Record){func(r *Record) { r.Value = []byte(strings.Repeat("a", contract.MaxRecordBytes+1)) }, func(r *Record) { r.Key = []byte("other/ns/run-1") }, func(r *Record) { r.Value = []byte(`{"schema_version":1,"schema_version":2}`) }} {
		store := &receipts{}
		c := Consumer{Control: store, Scope: contract.Scope{InstallationID: "test", Namespace: "ns"}, Group: "test"}
		r := validRecord(t)
		mutate(&r)
		if err := c.Receipt(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		if store.entries[0].QuarantineReason == "" || store.entries[0].Envelope != nil {
			t.Fatal("invalid envelope not quarantined")
		}
	}
}

func TestPermanentCoordinatorRejectionUsesQuarantineReceipt(t *testing.T) {
	store := &receipts{reject: &control.StatusError{Status: 400}}
	c := Consumer{Control: store, Scope: contract.Scope{InstallationID: "test", Namespace: "ns"}, Group: "test-group"}
	record := validRecord(t)
	if err := c.Receipt(context.Background(), record); err == nil {
		t.Fatal("expected rejected valid envelope")
	}
	store.reject = nil
	if err := c.quarantineRejection(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(store.entries) != 1 || store.entries[0].QuarantineReason != "coordinator_rejected" || store.entries[0].Envelope != nil {
		t.Fatal("coordinator rejection was not quarantined without an envelope")
	}
}

func TestReceiptRetryClassification(t *testing.T) {
	for _, test := range []struct {
		err  error
		want bool
	}{
		{control.ErrUnavailable, true},
		{&control.StatusError{Status: 429}, true},
		{&control.StatusError{Status: 503}, true},
		{&control.StatusError{Status: 400}, false},
		{errors.New("invalid response"), false},
	} {
		if got := retryable(test.err); got != test.want {
			t.Fatalf("retryable(%v)=%v, want %v", test.err, got, test.want)
		}
	}
}
