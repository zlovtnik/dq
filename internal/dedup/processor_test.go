package dedup

import (
	"context"
	"fmt"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"math"
	"strings"
	"testing"
	"time"
)

func testSnapshot() (contract.RunRequest, Parameters, Snapshot) {
	req := contract.RunRequest{Scope: contract.Scope{InstallationID: "i", Namespace: "n"}, RunID: "run", AttemptID: "attempt", Fence: 1, Operation: "prepare_candidates", SourceRevisionDigest: strings.Repeat("a", 64), PolicyVersion: "1"}
	parameters := Parameters{AuthorizedScope: "*", PolicyVersion: "1", MaxCandidates: 1000}
	snapshot := Snapshot{InstallationID: "i", Namespace: "n", AuthorizedScope: "*", Operation: req.Operation, SourceRevisionDigest: req.SourceRevisionDigest, PolicyVersion: "1", Pairs: []Pair{{PairID: "pair", MACA: "00:00:00:00:00:02", MACB: "00:00:00:00:00:01", Confidence: .9, LeftRevision: 1, RightRevision: 1, PairRevision: 1}}, Devices: []Device{{MAC: "00:00:00:00:00:01", Revision: 1}, {MAC: "00:00:00:00:00:02", Revision: 1}}}
	return req, parameters, snapshot
}
func TestCanonicalPairsAndMaximumConfidence(t *testing.T) {
	req, p, s := testSnapshot()
	next := s.Pairs[0]
	next.MACA, next.MACB = next.MACB, next.MACA
	next.Confidence = .95
	next.PairID = "other"
	s.Pairs = append(s.Pairs, next)
	proposal, err := Prepare(context.Background(), req, p, s)
	if err != nil {
		t.Fatal(err)
	}
	if len(proposal.Candidates) != 1 || proposal.Candidates[0].MACA != "00:00:00:00:00:01" || proposal.Candidates[0].Confidence != .95 {
		t.Fatal(proposal)
	}
}
func TestHumanDecisionsPrecedeConfidenceUpdates(t *testing.T) {
	for _, decision := range []string{"merge", "not_match", "needs_more_data"} {
		t.Run(decision, func(t *testing.T) {
			req, p, s := testSnapshot()
			s.Candidates = []Candidate{{CandidateID: "candidate", MACA: s.Pairs[0].MACA, MACB: s.Pairs[0].MACB, Confidence: .7, Status: "pending", Revision: 1}}
			s.Decisions = []Decision{{CandidateID: "candidate", Decision: decision, DecidedAt: time.Now()}}
			proposal, err := Prepare(context.Background(), req, p, s)
			if err != nil || len(proposal.Candidates) != 0 {
				t.Fatalf("human decision overwritten %v %v", proposal, err)
			}
		})
	}
	req, p, s := testSnapshot()
	s.Candidates = []Candidate{{CandidateID: "candidate", MACA: s.Pairs[0].MACA, MACB: s.Pairs[0].MACB, Confidence: .7, Status: "pending", Revision: 1}}
	proposal, err := Prepare(context.Background(), req, p, s)
	if err != nil || len(proposal.Candidates) != 1 {
		t.Fatalf("improved evidence discarded %v %v", proposal, err)
	}
}
func TestDecisionReconciliationPreservesDecision(t *testing.T) {
	for _, decision := range []string{"merge", "not_match", "needs_more_data"} {
		req, p, s := testSnapshot()
		req.Operation = "reconcile_decision"
		s.Operation = req.Operation
		p.CandidateID = "candidate"
		s.CandidateID = p.CandidateID
		s.Candidates = []Candidate{{CandidateID: p.CandidateID, MACA: s.Pairs[0].MACA, MACB: s.Pairs[0].MACB, Confidence: .9, Status: "pending", Revision: 1}}
		s.Decisions = []Decision{{CandidateID: p.CandidateID, Decision: decision, DecidedAt: time.Now()}}
		proposal, err := Prepare(context.Background(), req, p, s)
		if err != nil || len(proposal.Candidates) != 0 || len(proposal.Reconciliations) != 1 || proposal.Reconciliations[0].Decision != decision {
			t.Fatalf("reconciliation %v %v", proposal, err)
		}
	}
}
func TestInvalidNumericScopeAndDecisionEvidence(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*contract.RunRequest, *Parameters, *Snapshot)
	}{{"nan", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Pairs[0].Confidence = math.NaN() }}, {"infinity", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Pairs[0].Confidence = math.Inf(1) }}, {"negative", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Pairs[0].Confidence = -1 }}, {"excess", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Pairs[0].Confidence = 1.1 }}, {"scope", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.AuthorizedScope = "other" }}, {"revision", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) {
		s.SourceRevisionDigest = strings.Repeat("b", 64)
	}}, {"missing endpoint", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Devices = s.Devices[:1] }}, {"self pair", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) { s.Pairs[0].MACA = s.Pairs[0].MACB }}, {"unknown decision", func(_ *contract.RunRequest, _ *Parameters, s *Snapshot) {
		s.Decisions = []Decision{{CandidateID: "unknown", Decision: "merge", DecidedAt: time.Now()}}
	}}, {"unbounded candidates", func(_ *contract.RunRequest, p *Parameters, _ *Snapshot) { p.MaxCandidates = 1001 }}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, p, s := testSnapshot()
			tc.mutate(&req, &p, &s)
			if _, err := Prepare(context.Background(), req, p, s); err == nil {
				t.Fatal("invalid evidence accepted")
			}
		})
	}
}
func BenchmarkPrepare1000Candidates(b *testing.B) {
	req, p, s := testSnapshot()
	s.Devices = nil
	s.Pairs = nil
	for i := range 1000 {
		a := fmt.Sprintf("00:00:00:01:%02x:%02x", i/256, i%256)
		z := fmt.Sprintf("00:00:00:02:%02x:%02x", i/256, i%256)
		s.Devices = append(s.Devices, Device{MAC: a, Revision: 1}, Device{MAC: z, Revision: 1})
		s.Pairs = append(s.Pairs, Pair{PairID: fmt.Sprintf("pair%d", i), MACA: a, MACB: z, Confidence: .9, LeftRevision: 1, RightRevision: 1, PairRevision: 1})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		proposal, err := Prepare(context.Background(), req, p, s)
		if err != nil || len(proposal.Candidates) != 1000 {
			b.Fatalf("prepare %v", err)
		}
	}
}
