// Package dedup prepares proposals only. Octopus remains responsible for
// checking current revisions, human decisions, and confirmation safeguards.
package dedup

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/sdk"
)

const ProcessorID = "atheros.dedup"
const Version = "0.1.0"
const SnapshotSchema = "atheros.dedup.snapshot.v1"
const ProposalSchema = "atheros.dedup.proposal.v1"

type Parameters struct {
	AuthorizedScope  string `json:"authorized_scope"`
	SnapshotRevision string `json:"snapshot_revision,omitempty"`
	PolicyVersion    string `json:"policy_version"`
	MaxCandidates    int    `json:"max_candidates"`
	CandidateID      string `json:"candidate_id,omitempty"`
}
type Pair struct {
	PairID        string  `json:"pair_id"`
	MACA          string  `json:"mac_a"`
	MACB          string  `json:"mac_b"`
	Confidence    float64 `json:"confidence"`
	LeftRevision  int64   `json:"left_revision"`
	RightRevision int64   `json:"right_revision"`
	PairRevision  int64   `json:"pair_revision"`
}
type Device struct {
	MAC                string `json:"mac"`
	Registered         bool   `json:"registered"`
	RegisteredDeviceID string `json:"registered_device_id"`
	Revision           int64  `json:"revision"`
}
type Candidate struct {
	CandidateID string  `json:"candidate_id"`
	MACA        string  `json:"mac_a"`
	MACB        string  `json:"mac_b"`
	Confidence  float64 `json:"confidence"`
	Status      string  `json:"status"`
	Revision    int64   `json:"revision"`
}
type Decision struct {
	CandidateID string    `json:"candidate_id"`
	Decision    string    `json:"decision"`
	DecidedAt   time.Time `json:"decided_at"`
}
type Snapshot struct {
	InstallationID       string      `json:"installation_id"`
	Namespace            string      `json:"namespace"`
	AuthorizedScope      string      `json:"authorized_scope"`
	Operation            string      `json:"operation"`
	SourceRevisionDigest string      `json:"source_revision_digest"`
	PolicyVersion        string      `json:"policy_version"`
	Pairs                []Pair      `json:"pairs"`
	Devices              []Device    `json:"devices"`
	Candidates           []Candidate `json:"candidates"`
	Decisions            []Decision  `json:"decisions"`
	CandidateID          string      `json:"candidate_id,omitempty"`
}
type CandidateProposal struct {
	MACA       string  `json:"mac_a"`
	MACB       string  `json:"mac_b"`
	Confidence float64 `json:"confidence"`
	PairID     string  `json:"pair_id"`
}
type Reconciliation struct {
	CandidateID string `json:"candidate_id"`
	Decision    string `json:"decision"`
}
type Summary struct {
	CandidateCount      int `json:"candidate_count"`
	ReconciliationCount int `json:"reconciliation_count"`
}
type Proposal struct {
	RunID                string              `json:"run_id"`
	AttemptID            string              `json:"attempt_id"`
	Fence                int64               `json:"fence"`
	Operation            string              `json:"operation"`
	SourceRevisionDigest string              `json:"source_revision_digest"`
	PolicyVersion        string              `json:"policy_version"`
	Candidates           []CandidateProposal `json:"candidates"`
	Reconciliations      []Reconciliation    `json:"reconciliations"`
	Summary              Summary             `json:"summary"`
}
type Processor struct{}

func canonicalMAC(value string) (string, error) {
	mac, err := net.ParseMAC(value)
	if err != nil || len(mac) != 6 {
		return "", sdk.Permanent("snapshot_invalid_mac")
	}
	return strings.ToLower(mac.String()), nil
}
func CanonicalPair(a, b string) (string, string, error) {
	a, err := canonicalMAC(a)
	if err != nil {
		return "", "", err
	}
	b, err = canonicalMAC(b)
	if err != nil || a == b {
		return "", "", sdk.Permanent("snapshot_invalid_pair")
	}
	if a > b {
		a, b = b, a
	}
	return a, b, nil
}
func validConfidence(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= 0 && value <= 1
}
func validDecision(value string) bool {
	return value == "merge" || value == "not_match" || value == "needs_more_data"
}
func pairKey(a, b string) string { return a + "/" + b }

func Prepare(ctx context.Context, req contract.RunRequest, parameters Parameters, snapshot Snapshot) (Proposal, error) {
	invalid := sdk.Permanent("snapshot_invalid")
	if parameters.AuthorizedScope == "" || parameters.MaxCandidates < 1 || parameters.MaxCandidates > 1000 || parameters.PolicyVersion != req.PolicyVersion {
		return Proposal{}, invalid
	}
	if parameters.SnapshotRevision != "" && parameters.SnapshotRevision != req.SourceRevisionDigest {
		return Proposal{}, sdk.Permanent("source_revision_changed")
	}
	if snapshot.InstallationID != req.InstallationID || snapshot.Namespace != req.Namespace || snapshot.AuthorizedScope != parameters.AuthorizedScope || snapshot.Operation != req.Operation || snapshot.SourceRevisionDigest != req.SourceRevisionDigest || snapshot.PolicyVersion != req.PolicyVersion {
		return Proposal{}, sdk.Permanent("snapshot_scope_mismatch")
	}
	if len(snapshot.Pairs) > 100000 || len(snapshot.Devices) > 100000 || len(snapshot.Candidates) > 100000 || len(snapshot.Decisions) > 100000 {
		return Proposal{}, sdk.Permanent("snapshot_limit")
	}
	devices := map[string]Device{}
	for _, device := range snapshot.Devices {
		mac, err := canonicalMAC(device.MAC)
		if err != nil || device.Revision < 0 {
			return Proposal{}, invalid
		}
		if _, ok := devices[mac]; ok {
			return Proposal{}, invalid
		}
		devices[mac] = device
	}
	candidates := map[string]Candidate{}
	existing := map[string]Candidate{}
	for _, candidate := range snapshot.Candidates {
		a, b, err := CanonicalPair(candidate.MACA, candidate.MACB)
		if err != nil || candidate.CandidateID == "" || !validConfidence(candidate.Confidence) || candidate.Revision < 0 {
			return Proposal{}, invalid
		}
		if _, ok := candidates[candidate.CandidateID]; ok {
			return Proposal{}, invalid
		}
		candidate.MACA = a
		candidate.MACB = b
		candidates[candidate.CandidateID] = candidate
		if _, ok := existing[pairKey(a, b)]; ok {
			return Proposal{}, invalid
		}
		existing[pairKey(a, b)] = candidate
	}
	decisions := map[string]Decision{}
	for _, decision := range snapshot.Decisions {
		if !validDecision(decision.Decision) || decision.DecidedAt.IsZero() {
			return Proposal{}, invalid
		}
		if _, ok := candidates[decision.CandidateID]; !ok {
			return Proposal{}, invalid
		}
		if _, ok := decisions[decision.CandidateID]; ok {
			return Proposal{}, invalid
		}
		decisions[decision.CandidateID] = decision
	}
	proposal := Proposal{RunID: req.RunID, AttemptID: req.AttemptID, Fence: req.Fence, Operation: req.Operation, SourceRevisionDigest: req.SourceRevisionDigest, PolicyVersion: req.PolicyVersion, Candidates: []CandidateProposal{}, Reconciliations: []Reconciliation{}}
	switch req.Operation {
	case "prepare_candidates":
		unique := map[string]CandidateProposal{}
		for _, pair := range snapshot.Pairs {
			if err := ctx.Err(); err != nil {
				return Proposal{}, err
			}
			a, b, err := CanonicalPair(pair.MACA, pair.MACB)
			if err != nil || pair.PairID == "" || !validConfidence(pair.Confidence) || pair.LeftRevision < 0 || pair.RightRevision < 0 || pair.PairRevision < 0 {
				return Proposal{}, invalid
			}
			if _, ok := devices[a]; !ok {
				return Proposal{}, invalid
			}
			if _, ok := devices[b]; !ok {
				return Proposal{}, invalid
			}
			key := pairKey(a, b)
			if candidate, ok := existing[key]; ok {
				if _, decided := decisions[candidate.CandidateID]; decided || candidate.Confidence >= pair.Confidence || candidate.Status != "pending" {
					continue
				}
			}
			next := CandidateProposal{MACA: a, MACB: b, Confidence: pair.Confidence, PairID: pair.PairID}
			if previous, ok := unique[key]; !ok || next.Confidence > previous.Confidence || next.Confidence == previous.Confidence && next.PairID < previous.PairID {
				unique[key] = next
			}
		}
		for _, pair := range unique {
			proposal.Candidates = append(proposal.Candidates, pair)
		}
		sort.Slice(proposal.Candidates, func(i, j int) bool {
			a, b := proposal.Candidates[i], proposal.Candidates[j]
			if a.Confidence != b.Confidence {
				return a.Confidence > b.Confidence
			}
			return pairKey(a.MACA, a.MACB) < pairKey(b.MACA, b.MACB)
		})
		if len(proposal.Candidates) > parameters.MaxCandidates {
			proposal.Candidates = proposal.Candidates[:parameters.MaxCandidates]
		}
	case "reconcile_decision":
		if parameters.CandidateID == "" || snapshot.CandidateID != parameters.CandidateID {
			return Proposal{}, invalid
		}
		decision, ok := decisions[parameters.CandidateID]
		if !ok {
			return Proposal{}, sdk.Permanent("decision_missing")
		}
		proposal.Reconciliations = append(proposal.Reconciliations, Reconciliation{CandidateID: decision.CandidateID, Decision: decision.Decision})
	default:
		return Proposal{}, sdk.Permanent("operation_unsupported")
	}
	proposal.Summary = Summary{CandidateCount: len(proposal.Candidates), ReconciliationCount: len(proposal.Reconciliations)}
	return proposal, nil
}

func (Processor) Run(ctx context.Context, req contract.RunRequest, run sdk.RunContext) (contract.OutputManifest, error) {
	if len(req.InputRefs) != 1 || req.InputRefs[0].SchemaID != SnapshotSchema {
		return contract.OutputManifest{}, sdk.Permanent("snapshot_required")
	}
	var parameters Parameters
	decoder := json.NewDecoder(strings.NewReader(string(req.Parameters)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parameters); err != nil {
		return contract.OutputManifest{}, sdk.Permanent("parameters_invalid")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return contract.OutputManifest{}, sdk.Permanent("parameters_invalid")
	}
	reader, err := run.Artifacts.OpenRead(ctx, "snapshot", req.InputRefs[0])
	if err != nil {
		return contract.OutputManifest{}, err
	}
	defer reader.Close()
	var snapshot Snapshot
	decoder = json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return contract.OutputManifest{}, sdk.Permanent("snapshot_invalid")
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return contract.OutputManifest{}, sdk.Permanent("snapshot_invalid")
	}
	proposal, err := Prepare(ctx, req, parameters, snapshot)
	if err != nil {
		return contract.OutputManifest{}, err
	}
	output, err := run.Artifacts.CreateOutput(ctx, "proposal", "application/json")
	if err != nil {
		return contract.OutputManifest{}, err
	}
	defer output.Close()
	if err = json.NewEncoder(output).Encode(proposal); err != nil {
		return contract.OutputManifest{}, err
	}
	ref, err := output.Commit(ctx)
	if err != nil {
		return contract.OutputManifest{}, err
	}
	summary, err := json.Marshal(proposal.Summary)
	if err != nil {
		return contract.OutputManifest{}, err
	}
	return contract.OutputManifest{SchemaID: ProposalSchema, OutputRefs: []contract.ArtifactRef{ref}, Summary: summary, SourceRevisionDigest: req.SourceRevisionDigest}, nil
}
