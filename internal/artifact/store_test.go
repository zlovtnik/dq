package artifact

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"io"
	"strings"
	"sync"
	"testing"
)

type memoryBackend struct {
	mu           sync.Mutex
	objects      map[string][]byte
	info         map[string]ObjectInfo
	fail         error
	unversioned  bool
	short        bool
	readVersions []string
	writeKeys    []string
}

func (b *memoryBackend) Stat(_ context.Context, bucket, key, version string) (ObjectInfo, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return ObjectInfo{}, b.fail
	}
	info, ok := b.info[bucket+"/"+key]
	if !ok {
		return ObjectInfo{}, errors.New("missing object")
	}
	return info, nil
}
func (b *memoryBackend) Read(_ context.Context, bucket, key, version string) (io.ReadCloser, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.readVersions = append(b.readVersions, version)
	data := b.objects[bucket+"/"+key]
	if b.short && len(data) > 0 {
		data = data[:len(data)-1]
	}
	return io.NopCloser(bytes.NewReader(data)), b.fail
}
func (b *memoryBackend) Write(ctx context.Context, bucket, key, media string, reader io.Reader) (ObjectInfo, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err = ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.fail != nil {
		return ObjectInfo{}, b.fail
	}
	version := "version-output"
	if b.unversioned {
		version = "null"
	}
	info := ObjectInfo{VersionID: version, ETag: "output-etag", Size: int64(len(data))}
	b.objects[bucket+"/"+key] = data
	b.info[bucket+"/"+key] = info
	b.writeKeys = append(b.writeKeys, key)
	return info, nil
}
func fixture(t testing.TB) (*access, *memoryBackend, contract.ArtifactRef) {
	t.Helper()
	data := []byte(`{"devices":[]}`)
	ref := contract.ArtifactRef{ArtifactID: "snapshot", InstallationID: "i", Namespace: "n", URI: "s3://dq-artifacts/i/n/inputs/snapshot/" + contract.SHA256(data), VersionID: "snapshot-version", ETag: "etag", SHA256: contract.SHA256(data), SchemaID: "snapshot", MediaType: "application/json", SizeBytes: int64(len(data))}
	key := "dq-artifacts/i/n/inputs/snapshot/" + contract.SHA256(data)
	backend := &memoryBackend{objects: map[string][]byte{key: data}, info: map[string]ObjectInfo{key: {VersionID: ref.VersionID, ETag: ref.ETag, Size: ref.SizeBytes}}}
	manifest := contract.Manifest{InstallationID: "i", Namespace: "n", Inputs: []contract.Binding{{Name: "snapshot", Transport: "minio", Bucket: "dq-artifacts", AllowedPrefix: "i/n/inputs", SchemaID: "snapshot", MaxItems: 1, MaxBytes: 1024}}, Outputs: []contract.Binding{{Name: "proposal", Transport: "minio", Bucket: "dq-artifacts", AllowedPrefix: "i/n/runs", SchemaID: "proposal", MaxItems: 2, MaxBytes: 1024}}, Resources: contract.ResourceLimits{MaxInputBytes: 1024, MaxOutputBytes: 1024}}
	factory, err := NewFactory(backend, manifest)
	if err != nil {
		t.Fatal(err)
	}
	resources, err := factory.ForAttempt(contract.RunRequest{Scope: contract.Scope{InstallationID: "i", Namespace: "n"}, RunID: "run", AttemptID: "attempt", InputRefs: []contract.ArtifactRef{ref}})
	if err != nil {
		t.Fatal(err)
	}
	return resources.(*access), backend, ref
}
func TestExactVersionAndIntegrity(t *testing.T) {
	access, backend, ref := fixture(t)
	reader, err := access.OpenRead(context.Background(), "snapshot", ref)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if contract.SHA256(data) != ref.SHA256 || len(backend.readVersions) != 1 || backend.readVersions[0] != ref.VersionID {
		t.Fatal("exact version not read")
	}
}
func TestReadRejectsReferencesAndCorruption(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*access, *memoryBackend, *contract.ArtifactRef)
	}{{"unlisted", func(_ *access, _ *memoryBackend, r *contract.ArtifactRef) { r.ArtifactID = "different" }}, {"wrong installation", func(_ *access, _ *memoryBackend, r *contract.ArtifactRef) { r.InstallationID = "other" }}, {"wrong prefix", func(a *access, _ *memoryBackend, r *contract.ArtifactRef) {
		r.URI = "s3://dq-artifacts/i/n/runs/run/attempts/attempt/snapshot"
		a.request.InputRefs = []contract.ArtifactRef{*r}
	}}, {"signed URL", func(a *access, _ *memoryBackend, r *contract.ArtifactRef) {
		r.URI += "?X-Amz-Credential=secret"
		a.request.InputRefs = []contract.ArtifactRef{*r}
	}}, {"unversioned", func(a *access, _ *memoryBackend, r *contract.ArtifactRef) {
		r.VersionID = "null"
		a.request.InputRefs = []contract.ArtifactRef{*r}
	}}, {"wrong hash", func(a *access, _ *memoryBackend, r *contract.ArtifactRef) {
		r.SHA256 = strings.Repeat("a", 64)
		a.request.InputRefs = []contract.ArtifactRef{*r}
	}}, {"truncated", func(_ *access, b *memoryBackend, _ *contract.ArtifactRef) { b.short = true }}, {"storage outage", func(_ *access, b *memoryBackend, _ *contract.ArtifactRef) { b.fail = errors.New("outage") }}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b, r := fixture(t)
			tc.mutate(a, b, &r)
			reader, err := a.OpenRead(context.Background(), "snapshot", r)
			if err == nil {
				_, err = io.ReadAll(reader)
				closeErr := reader.Close()
				if err == nil {
					err = closeErr
				}
			}
			if err == nil {
				t.Fatal("invalid artifact accepted")
			}
		})
	}
}
func TestPartialReadCannotVerify(t *testing.T) {
	a, _, r := fixture(t)
	reader, err := a.OpenRead(context.Background(), "snapshot", r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reader.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("partial read verified: %v", err)
	}
}
func TestOutputCommitIsImmutableAndBounded(t *testing.T) {
	a, b, _ := fixture(t)
	ctx := context.Background()
	out, err := a.CreateOutput(ctx, "proposal", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	body := []byte(`{"proposals":[]}`)
	if _, err = out.Write(body); err != nil {
		t.Fatal(err)
	}
	ref, err := out.Commit(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ref.SHA256 != contract.SHA256(body) || ref.VersionID == "" || ref.ProducerAttemptID != "attempt" || !strings.HasPrefix(b.writeKeys[0], "i/n/runs/run/attempts/attempt/") {
		t.Fatal("incorrect output ref")
	}
	if _, err = out.Write(body); err == nil {
		t.Fatal("committed output writable")
	}
	if _, err = out.Commit(ctx); err == nil {
		t.Fatal("duplicate commit accepted")
	}
	second, err := a.CreateOutput(ctx, "proposal", "application/json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = second.Write(make([]byte, 1025)); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	if _, err = second.Commit(ctx); err == nil {
		t.Fatal("oversize commit accepted")
	}
	_ = second.Close()
}
func TestUnversionedAndAbortedUploadsNeverCommit(t *testing.T) {
	for _, mode := range []string{"unversioned", "abort", "outage"} {
		t.Run(mode, func(t *testing.T) {
			a, b, _ := fixture(t)
			b.unversioned = mode == "unversioned"
			if mode == "outage" {
				b.fail = errors.New("outage")
			}
			out, err := a.CreateOutput(context.Background(), "proposal", "application/json")
			if err != nil {
				t.Fatal(err)
			}
			_, _ = out.Write([]byte(`{}`))
			if mode == "abort" {
				_ = out.Close()
			}
			if _, err = out.Commit(context.Background()); err == nil {
				t.Fatal("partial/unversioned upload committed")
			}
			_ = out.Close()
		})
	}
}
func BenchmarkVerifiedStreaming(b *testing.B) {
	data := bytes.Repeat([]byte("d"), 1024*1024)
	ref := contract.ArtifactRef{SizeBytes: int64(len(data)), SHA256: contract.SHA256(data)}
	buffer := make([]byte, 32*1024)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reader := &verifiedReader{reader: io.NopCloser(bytes.NewReader(data)), ref: ref, hash: sha256.New()}
		if _, err := io.CopyBuffer(io.Discard, reader, buffer); err != nil {
			b.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
