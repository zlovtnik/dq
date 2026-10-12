// This command generates a checksummed registration manifest from an approved
// deployment image digest. It never registers or deploys a processor itself.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/dedup"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/schemas"
	"os"
	"strings"
)

type options struct {
	installation, namespace, digest, workerService, policyVersion, remoteURL, remoteDigest string
	concurrency                                                                            int
}

func build(o options) (contract.Manifest, error) {
	if !contract.ValidSegment(o.installation) || !contract.ValidSegment(o.namespace) || !strings.HasPrefix(o.digest, "sha256:") || !contract.ValidSHA256(strings.TrimPrefix(o.digest, "sha256:")) || o.policyVersion == "" || o.workerService == "" || o.concurrency < 1 || o.concurrency > 128 {
		return contract.Manifest{}, errors.New("installation, namespace, approved image digest, worker service, policy, and bounded concurrency required")
	}
	schema, err := schemas.Files.ReadFile("atheros.dedup.parameters.v1.json")
	if err != nil {
		return contract.Manifest{}, err
	}
	schema, err = contract.CanonicalJSON(schema)
	if err != nil {
		return contract.Manifest{}, err
	}
	m := contract.Manifest{InstallationID: o.installation, Namespace: o.namespace, ProcessorID: dedup.ProcessorID, Version: dedup.Version, OwnerTeam: "atheros", ImplementationDigest: o.digest, WorkerService: o.workerService, ProtocolVersion: contract.ProtocolVersion, ParameterSchema: schema, ParameterSchemaSHA256: contract.SHA256(schema), Inputs: []contract.Binding{{Name: "snapshot", Direction: "input", Transport: "minio", Bucket: "dq-artifacts", AllowedPrefix: o.installation + "/" + o.namespace + "/inputs", SchemaID: dedup.SnapshotSchema, Required: true, MaxItems: 1, MaxBytes: 32 * 1024 * 1024, Semantics: "artifact_reference"}}, Outputs: []contract.Binding{{Name: "proposal", Direction: "output", Transport: "minio", Bucket: "dq-artifacts", AllowedPrefix: o.installation + "/" + o.namespace + "/runs", SchemaID: dedup.ProposalSchema, Required: true, MaxItems: 1, MaxBytes: 1024 * 1024, Semantics: "artifact_reference"}}, Resources: contract.ResourceLimits{Concurrency: o.concurrency, TimeoutSeconds: 45, MaxInputBytes: 32 * 1024 * 1024, MaxOutputBytes: 1024 * 1024}, PolicyVersion: o.policyVersion}
	if o.remoteURL != "" || o.remoteDigest != "" {
		if o.remoteURL == "" || !contract.ValidSHA256(o.remoteDigest) {
			return m, errors.New("remote immutable URL and digest must be paired")
		}
		m.UIRemote = &contract.UIRemote{ImmutableURL: o.remoteURL, BundleDigest: o.remoteDigest, HostAPIVersion: "dq.host.v1", ParameterForm: true, RunInspector: true, ArtifactPreview: true}
	}
	m.ManifestSHA256, err = contract.ManifestDigest(m)
	return m, err
}
func run() error {
	var o options
	flag.StringVar(&o.installation, "installation", "", "authorized installation")
	flag.StringVar(&o.namespace, "namespace", "", "authorized namespace")
	flag.StringVar(&o.digest, "implementation-digest", "", "reviewed OCI sha256 digest (required)")
	flag.StringVar(&o.workerService, "worker-service", "ssl-proxy-dq-atheros-dedup-v0-1-0:9091", "approved deployment service")
	flag.StringVar(&o.policyVersion, "policy-version", "", "approved policy version (required)")
	flag.IntVar(&o.concurrency, "concurrency", 4, "warm worker slots")
	flag.StringVar(&o.remoteURL, "remote-url", "", "approved immutable UI URL")
	flag.StringVar(&o.remoteDigest, "remote-digest", "", "UI bundle SHA256")
	flag.Parse()
	manifest, err := build(o)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(manifest)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
