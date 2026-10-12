//go:build integration

package artifact

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// This test is mandatory when the integration tag is requested. It starts a
// real, disposable MinIO with versioning and encryption; unavailable Docker
// fails the test rather than skipping storage compatibility verification.
func TestMinIOExactVersionEncryptedStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	image := os.Getenv("DQ_TEST_MINIO_IMAGE")
	if image == "" {
		image = "minio/minio:RELEASE.2025-09-07T16-13-09Z"
	}
	args := []string{"run", "-d", "--rm", "-p", "127.0.0.1::9000", "-e", "MINIO_ROOT_USER=dqtestuser", "-e", "MINIO_ROOT_PASSWORD=dqtestpassword", "-e", "MINIO_KMS_SECRET_KEY=dq-test:000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f", image, "server", "/data"}
	output, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("mandatory MinIO test container: %v %s", err, output)
	}
	container := strings.TrimSpace(string(output))
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", container).Run() })
	port, err := exec.CommandContext(ctx, "docker", "port", container, "9000/tcp").Output()
	if err != nil {
		t.Fatal(err)
	}
	endpoint := strings.TrimSpace(string(port))
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("dqtestuser", "dqtestpassword", ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "dq-artifacts"
	for attempt := 0; attempt < 100; attempt++ {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = client.SetBucketVersioning(ctx, bucket, minio.BucketVersioningConfiguration{Status: "Enabled"}); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"snapshot":"immutable"}`)
	key := "i/n/inputs/snapshot/" + contract.SHA256(body)
	info, err := client.PutObject(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "application/json", ServerSideEncryption: encrypt.NewSSE()})
	if err != nil {
		t.Fatal(err)
	}
	if info.VersionID == "" || info.VersionID == "null" {
		t.Fatal("MinIO did not return exact input version")
	}
	if _, err = client.PutObject(ctx, bucket, key, strings.NewReader(`{"newer":true}`), -1, minio.PutObjectOptions{ContentType: "application/json", ServerSideEncryption: encrypt.NewSSE()}); err != nil {
		t.Fatal(err)
	}
	ref := contract.ArtifactRef{ArtifactID: "snapshot", InstallationID: "i", Namespace: "n", URI: "s3://" + bucket + "/" + key, VersionID: info.VersionID, ETag: info.ETag, SHA256: contract.SHA256(body), SchemaID: "snapshot", MediaType: "application/json", SizeBytes: int64(len(body))}
	m := contract.Manifest{InstallationID: "i", Namespace: "n", Inputs: []contract.Binding{{Name: "snapshot", Transport: "minio", Bucket: bucket, AllowedPrefix: "i/n/inputs", SchemaID: "snapshot", Required: true, MaxItems: 1, MaxBytes: 1024}}, Outputs: []contract.Binding{{Name: "proposal", Transport: "minio", Bucket: bucket, AllowedPrefix: "i/n/runs", SchemaID: "proposal", Required: true, MaxItems: 1, MaxBytes: 8 * 1024 * 1024}}, Resources: contract.ResourceLimits{MaxInputBytes: 1024, MaxOutputBytes: 8 * 1024 * 1024}}
	factory, err := NewFactory(NewMinIO(client), m)
	if err != nil {
		t.Fatal(err)
	}
	access, err := factory.ForAttempt(contract.RunRequest{Scope: contract.Scope{InstallationID: "i", Namespace: "n"}, RunID: "run", AttemptID: "attempt", InputRefs: []contract.ArtifactRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := access.OpenRead(ctx, "snapshot", ref)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, body) {
		t.Fatal("read latest instead of pinned version")
	}
	writer, err := access.CreateOutput(ctx, "proposal", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	stream := bytes.Repeat([]byte("x"), 6*1024*1024)
	if _, err = io.Copy(writer, bytes.NewReader(stream)); err != nil {
		t.Fatal(err)
	}
	result, err := writer.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.SHA256 != contract.SHA256(stream) || result.SizeBytes != int64(len(stream)) || result.VersionID == "" {
		t.Fatal("multipart commit mismatch")
	}
	_, objectKey, err := parseURI(result.URI)
	if err != nil {
		t.Fatal(err)
	}
	object, err := client.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{VersionID: result.VersionID})
	if err != nil {
		t.Fatal(err)
	}
	defer object.Close()
	returned, err := io.ReadAll(object)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(returned, stream) {
		t.Fatal("committed output content mismatch")
	}
	headers, err := object.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if headers.Metadata.Get("X-Amz-Server-Side-Encryption") != "AES256" {
		t.Fatalf("output was not encrypted: %v", headers.Metadata)
	}
	t.Log(fmt.Sprintf("exact encrypted versioned input and %d-byte bounded multipart output verified", len(stream)))
}
