package main

import (
	"bytes"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"strings"
	"testing"
)

func TestGeneratedManifestChecksumsAndBindings(t *testing.T) {
	manifest, err := build(options{installation: "test", namespace: "wireless", digest: "sha256:" + strings.Repeat("a", 64), workerService: "worker:9091", policyVersion: "identity.v1", concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := contract.ManifestDigest(manifest)
	if err != nil || digest != manifest.ManifestSHA256 {
		t.Fatal("manifest checksum mismatch")
	}
	schema, err := contract.CanonicalJSON(manifest.ParameterSchema)
	if err != nil || contract.SHA256(schema) != manifest.ParameterSchemaSHA256 {
		t.Fatal("schema checksum mismatch")
	}
	compiler := jsonschema.NewCompiler()
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		t.Fatal(err)
	}
	if err = compiler.AddResource("urn:test", doc); err != nil {
		t.Fatal(err)
	}
	compiled, err := compiler.Compile("urn:test")
	if err != nil {
		t.Fatal(err)
	}
	parameters, err := jsonschema.UnmarshalJSON(strings.NewReader(`{"authorized_scope":"*","policy_version":"identity.v1","max_candidates":1000}`))
	if err != nil || compiled.Validate(parameters) != nil {
		t.Fatal("parameter schema rejected valid parameters")
	}
	if manifest.Inputs[0].AllowedPrefix != "test/wireless/inputs" || manifest.Outputs[0].AllowedPrefix != "test/wireless/runs" {
		t.Fatal("scope prefixes")
	}
}
func TestManifestRequiresApprovedDigest(t *testing.T) {
	if _, err := build(options{installation: "i", namespace: "n", workerService: "worker", policyVersion: "1", concurrency: 1}); err == nil {
		t.Fatal("missing image digest accepted")
	}
}
