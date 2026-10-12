package rpc

import (
	"testing"

	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/rpc/pb"
)

func TestLifecycleRejectsAmbiguousDocuments(t *testing.T) {
	for _, input := range []string{`{"fence":1,"fence":2}`, `{"parameters":{"x":1,"x":2}}`, `{"unknown":true}`, `{} {}`} {
		var request contract.RunRequest
		if err := decode(&pb.Document{Json: []byte(input)}, &request); err == nil {
			t.Fatalf("ambiguous lifecycle document accepted: %s", input)
		}
	}
	var request contract.RunRequest
	if err := decode(&pb.Document{Json: []byte(`{"fence":1e3,"parameters":{"count":1.00}}`)}, &request); err != nil {
		t.Fatal(err)
	}
	if request.Fence != 1000 || string(request.Parameters) != `{"count":1}` {
		t.Fatalf("lifecycle canonicalization lost numeric identity: %+v", request)
	}
}
