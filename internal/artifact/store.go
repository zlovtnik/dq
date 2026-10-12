// Package artifact implements the supplied, attempt-scoped MinIO resource API.
package artifact

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"net/url"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/encrypt"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/sdk"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
)

var ErrIntegrity = sdk.Permanent("artifact_integrity")
var ErrForbidden = sdk.Permanent("artifact_forbidden")
var ErrLimit = sdk.Permanent("artifact_limit")

type ObjectInfo struct {
	VersionID, ETag string
	Size            int64
}

// Backend is implemented by the infrastructure client, never by a processor.
type Backend interface {
	Stat(context.Context, string, string, string) (ObjectInfo, error)
	Read(context.Context, string, string, string) (io.ReadCloser, error)
	Write(context.Context, string, string, string, io.Reader) (ObjectInfo, error)
}
type MinIO struct{ client *minio.Client }

func NewMinIO(client *minio.Client) *MinIO { return &MinIO{client: client} }
func (m *MinIO) Stat(ctx context.Context, bucket, key, version string) (ObjectInfo, error) {
	ctx, span := telemetry.Start(ctx, "storage.stat_exact_version")
	defer span.End()
	info, err := m.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{VersionID: version})
	if err != nil {
		telemetry.Fail(span)
	}
	return ObjectInfo{info.VersionID, info.ETag, info.Size}, err
}
func (m *MinIO) Read(ctx context.Context, bucket, key, version string) (io.ReadCloser, error) {
	ctx, span := telemetry.Start(ctx, "storage.read_exact_version")
	reader, err := m.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		telemetry.Fail(span)
		span.End()
		return nil, err
	}
	return &tracedReader{ReadCloser: reader, span: span}, nil
}

type tracedReader struct {
	io.ReadCloser
	span trace.Span
}

func (r *tracedReader) Read(data []byte) (int, error) {
	n, err := r.ReadCloser.Read(data)
	if err != nil && err != io.EOF {
		telemetry.Fail(r.span)
	}
	return n, err
}
func (r *tracedReader) Close() error {
	err := r.ReadCloser.Close()
	if err != nil {
		telemetry.Fail(r.span)
	}
	r.span.End()
	return err
}
func (m *MinIO) Write(ctx context.Context, bucket, key, media string, reader io.Reader) (ObjectInfo, error) {
	ctx, span := telemetry.Start(ctx, "storage.write_attempt_output")
	defer span.End()
	// Fixed multipart size and single upload thread bound the SDK's buffering.
	opts := minio.PutObjectOptions{ContentType: media, PartSize: 5 * 1024 * 1024, NumThreads: 1, ConcurrentStreamParts: false, ServerSideEncryption: encrypt.NewSSE()}
	opts.SetMatchETagExcept("*")
	info, err := m.client.PutObject(ctx, bucket, key, reader, -1, opts)
	if err != nil {
		telemetry.Fail(span)
	}
	return ObjectInfo{info.VersionID, info.ETag, info.Size}, err
}

type Factory struct {
	backend  Backend
	manifest contract.Manifest
}

func NewFactory(backend Backend, manifest contract.Manifest) (*Factory, error) {
	if backend == nil || !contract.ValidSegment(manifest.InstallationID) || !contract.ValidSegment(manifest.Namespace) || manifest.Resources.MaxInputBytes <= 0 || manifest.Resources.MaxOutputBytes <= 0 {
		return nil, errors.New("artifact prerequisites missing")
	}
	for _, bindings := range [][]contract.Binding{manifest.Inputs, manifest.Outputs} {
		for _, b := range bindings {
			if b.Transport != "minio" || b.Bucket == "" || b.AllowedPrefix == "" || b.MaxItems <= 0 || b.MaxBytes <= 0 {
				return nil, errors.New("invalid MinIO binding")
			}
		}
	}
	return &Factory{backend: backend, manifest: manifest}, nil
}
func (f *Factory) ForAttempt(req contract.RunRequest) (sdk.Artifacts, error) {
	if req.InstallationID != f.manifest.InstallationID || req.Namespace != f.manifest.Namespace || !contract.ValidSegment(req.RunID) || !contract.ValidSegment(req.AttemptID) {
		return nil, ErrForbidden
	}
	var total int64
	counts := map[string]int{}
	seenRefs := map[string]bool{}
	for _, ref := range req.InputRefs {
		if ref.Validate() != nil || ref.InstallationID != req.InstallationID || ref.Namespace != req.Namespace {
			return nil, ErrForbidden
		}
		refKey := ref.URI + "\x00" + ref.VersionID
		if seenRefs[refKey] {
			return nil, ErrForbidden
		}
		seenRefs[refKey] = true
		bucket, key, err := parseURI(ref.URI)
		if err != nil {
			return nil, err
		}
		matched := false
		for _, binding := range f.manifest.Inputs {
			if binding.SchemaID == ref.SchemaID && allowed(binding, bucket, key) {
				if matched {
					return nil, ErrForbidden
				}
				matched = true
				counts[binding.Name]++
				if counts[binding.Name] > binding.MaxItems || ref.SizeBytes > binding.MaxBytes {
					return nil, ErrLimit
				}
			}
		}
		if !matched {
			return nil, ErrForbidden
		}
		if ref.SizeBytes < 0 || ref.SizeBytes > f.manifest.Resources.MaxInputBytes-total {
			return nil, ErrLimit
		}
		total += ref.SizeBytes
	}
	for _, binding := range f.manifest.Inputs {
		if binding.Required && counts[binding.Name] == 0 {
			return nil, ErrForbidden
		}
	}
	return &access{factory: f, request: req, outputCounts: map[string]int{}}, nil
}

type access struct {
	factory      *Factory
	request      contract.RunRequest
	mu           sync.Mutex
	outputCounts map[string]int
	outputBytes  int64
}

func parseURI(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || strings.Contains(u.Path, "%") {
		return "", "", ErrForbidden
	}
	key := strings.TrimPrefix(u.Path, "/")
	for _, segment := range strings.Split(key, "/") {
		if !contract.ValidSegment(segment) {
			return "", "", ErrForbidden
		}
	}
	return u.Host, key, nil
}
func findBinding(bindings []contract.Binding, name string) (contract.Binding, error) {
	for _, b := range bindings {
		if b.Name == name {
			return b, nil
		}
	}
	return contract.Binding{}, ErrForbidden
}
func allowed(b contract.Binding, bucket, key string) bool {
	return b.Bucket == bucket && strings.HasPrefix(key, strings.TrimRight(b.AllowedPrefix, "/")+"/")
}
func (a *access) OpenRead(ctx context.Context, name string, ref contract.ArtifactRef) (io.ReadCloser, error) {
	b, err := findBinding(a.factory.manifest.Inputs, name)
	if err != nil {
		return nil, err
	}
	if ref.Validate() != nil {
		return nil, ErrIntegrity
	}
	if ref.InstallationID != a.request.InstallationID || ref.Namespace != a.request.Namespace || ref.SchemaID != b.SchemaID || ref.SizeBytes > b.MaxBytes {
		return nil, ErrForbidden
	}
	listed := false
	for _, input := range a.request.InputRefs {
		if input == ref {
			listed = true
		}
	}
	if !listed {
		return nil, ErrForbidden
	}
	bucket, key, err := parseURI(ref.URI)
	if err != nil || !allowed(b, bucket, key) || !strings.HasPrefix(key, a.request.InstallationID+"/"+a.request.Namespace+"/inputs/") {
		return nil, ErrForbidden
	}
	info, err := a.factory.backend.Stat(ctx, bucket, key, ref.VersionID)
	if err != nil {
		return nil, err
	}
	if info.VersionID != ref.VersionID || info.ETag != ref.ETag || info.Size != ref.SizeBytes {
		return nil, ErrIntegrity
	}
	reader, err := a.factory.backend.Read(ctx, bucket, key, ref.VersionID)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{reader: reader, ref: ref, hash: sha256.New()}, nil
}

type verifiedReader struct {
	reader   io.ReadCloser
	ref      contract.ArtifactRef
	hash     hash.Hash
	size     int64
	verified bool
	failure  error
}

func (r *verifiedReader) Read(data []byte) (int, error) {
	if r.failure != nil {
		return 0, r.failure
	}
	if r.verified {
		return 0, io.EOF
	}
	remaining := r.ref.SizeBytes - r.size + 1
	if int64(len(data)) > remaining {
		data = data[:remaining]
	}
	n, err := r.reader.Read(data)
	r.size += int64(n)
	_, _ = r.hash.Write(data[:n])
	if r.size > r.ref.SizeBytes {
		r.failure = ErrIntegrity
		return n, r.failure
	}
	if err == io.EOF {
		if r.size != r.ref.SizeBytes || hex.EncodeToString(r.hash.Sum(nil)) != r.ref.SHA256 {
			r.failure = ErrIntegrity
			return n, r.failure
		}
		r.verified = true
	}
	return n, err
}
func (r *verifiedReader) Close() error {
	err := r.reader.Close()
	if r.failure != nil {
		return r.failure
	}
	if !r.verified {
		return ErrIntegrity
	}
	return err
}
func randomID() (string, error) {
	var data [16]byte
	_, err := rand.Read(data[:])
	return hex.EncodeToString(data[:]), err
}
func (a *access) CreateOutput(ctx context.Context, name, media string) (sdk.Output, error) {
	b, err := findBinding(a.factory.manifest.Outputs, name)
	if err != nil {
		return nil, err
	}
	if media != "application/json" {
		return nil, ErrForbidden
	}
	artifactID, err := randomID()
	if err != nil {
		return nil, err
	}
	key := a.request.InstallationID + "/" + a.request.Namespace + "/runs/" + a.request.RunID + "/attempts/" + a.request.AttemptID + "/" + artifactID
	if !allowed(b, b.Bucket, key) {
		return nil, ErrForbidden
	}
	a.mu.Lock()
	if a.outputCounts[name] >= b.MaxItems {
		a.mu.Unlock()
		return nil, ErrLimit
	}
	a.outputCounts[name]++
	a.mu.Unlock()
	uploadCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	out := &output{access: a, binding: b, bucket: b.Bucket, key: key, artifactID: artifactID, media: media, hash: sha256.New(), writer: writer, cancel: cancel, result: make(chan uploadResult, 1)}
	go func() {
		info, err := a.factory.backend.Write(uploadCtx, b.Bucket, key, media, reader)
		_ = reader.CloseWithError(err)
		out.result <- uploadResult{info, err}
	}()
	return out, nil
}

type uploadResult struct {
	info ObjectInfo
	err  error
}
type output struct {
	access                         *access
	binding                        contract.Binding
	bucket, key, artifactID, media string
	hash                           hash.Hash
	size                           int64
	writer                         *io.PipeWriter
	cancel                         context.CancelFunc
	result                         chan uploadResult
	closed, committed              bool
	failure                        error
}

func (o *output) Write(data []byte) (int, error) {
	if o.closed || o.committed {
		return 0, io.ErrClosedPipe
	}
	o.access.mu.Lock()
	if int64(len(data)) > o.binding.MaxBytes-o.size || int64(len(data)) > o.access.factory.manifest.Resources.MaxOutputBytes-o.access.outputBytes {
		o.access.mu.Unlock()
		o.failure = ErrLimit
		_ = o.writer.CloseWithError(ErrLimit)
		o.cancel()
		o.closed = true
		return 0, ErrLimit
	}
	o.access.outputBytes += int64(len(data))
	o.access.mu.Unlock()
	n, err := o.writer.Write(data)
	o.size += int64(n)
	_, _ = o.hash.Write(data[:n])
	if err != nil {
		o.failure = err
	}
	return n, err
}

// Close aborts an uncommitted upload. Commit alone may publish a reference.
func (o *output) Close() error {
	if o.committed {
		return nil
	}
	if !o.closed {
		o.closed = true
		o.cancel()
		return o.writer.CloseWithError(context.Canceled)
	}
	return nil
}
func (o *output) Commit(ctx context.Context) (contract.ArtifactRef, error) {
	if err := ctx.Err(); err != nil {
		return contract.ArtifactRef{}, err
	}
	if o.closed || o.committed || o.failure != nil {
		return contract.ArtifactRef{}, ErrIntegrity
	}
	o.closed = true
	if err := o.writer.Close(); err != nil {
		return contract.ArtifactRef{}, err
	}
	defer o.cancel()
	var result uploadResult
	select {
	case result = <-o.result:
	case <-ctx.Done():
		return contract.ArtifactRef{}, ctx.Err()
	}
	if result.err != nil {
		return contract.ArtifactRef{}, result.err
	}
	if result.info.VersionID == "" || result.info.VersionID == "null" || result.info.Size != o.size || result.info.ETag == "" {
		return contract.ArtifactRef{}, ErrIntegrity
	}
	verified, err := o.access.factory.backend.Stat(ctx, o.bucket, o.key, result.info.VersionID)
	if err != nil {
		return contract.ArtifactRef{}, err
	}
	if verified != result.info {
		return contract.ArtifactRef{}, ErrIntegrity
	}
	ref := contract.ArtifactRef{ArtifactID: o.artifactID, InstallationID: o.access.request.InstallationID, Namespace: o.access.request.Namespace, URI: "s3://" + o.bucket + "/" + o.key, VersionID: result.info.VersionID, ETag: result.info.ETag, SHA256: hex.EncodeToString(o.hash.Sum(nil)), SchemaID: o.binding.SchemaID, MediaType: o.media, SizeBytes: o.size, ProducerRunID: o.access.request.RunID, ProducerAttemptID: o.access.request.AttemptID}
	o.committed = true
	return ref, nil
}
