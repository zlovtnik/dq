// The worker executable wires platform-supplied clients into an isolated
// atheros.dedup processor. Missing prerequisites fail closed at startup.
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/internal/artifact"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/internal/dedup"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/internal/worker"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/broker"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/rpc"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/telemetry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpccredentials "google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func env(name, def string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return def
}
func secret(name string) (string, error) {
	if path := os.Getenv(name + "_FILE"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return "", errors.New("secret file unavailable")
		}
		return strings.TrimSpace(string(data)), nil
	}
	return os.Getenv(name), nil
}
func manifest() (contract.Manifest, error) {
	var m contract.Manifest
	path := os.Getenv("DQ_WORKER_MANIFEST_FILE")
	if path == "" {
		return m, errors.New("worker manifest file required")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) > contract.MaxRecordBytes {
		return m, errors.New("bounded manifest unavailable")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&m); err != nil {
		return m, errors.New("invalid worker manifest")
	}
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(data))
	if err != nil {
		return m, err
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return m, errors.New("manifest object required")
	}
	delete(obj, "manifest_sha256")
	canonical, err := json.Marshal(obj)
	if err != nil {
		return m, err
	}
	canonical, err = contract.CanonicalJSON(canonical)
	if err != nil || contract.SHA256(canonical) != m.ManifestSHA256 {
		return m, errors.New("manifest checksum mismatch")
	}
	if m.ProcessorID != dedup.ProcessorID || m.Version != dedup.Version || m.ImplementationDigest != os.Getenv("DQ_WORKER_IMPLEMENTATION_DIGEST") || !strings.HasPrefix(m.ImplementationDigest, "sha256:") || !contract.ValidSHA256(strings.TrimPrefix(m.ImplementationDigest, "sha256:")) {
		return m, errors.New("deployed processor digest mismatch")
	}
	return m, nil
}
func tlsConfig(certPath, keyPath, caPath string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, errors.New("worker TLS certificate unavailable")
	}
	data, err := os.ReadFile(caPath)
	if err != nil {
		return nil, errors.New("worker TLS trust unavailable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, errors.New("invalid worker TLS trust")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, nil
}
func authorized(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		values := md.Get("authorization")
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte("Bearer "+token)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "service authentication required")
		}
		return next(ctx, req)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := telemetry.Init(ctx, "delip-queve-worker")
	if err != nil {
		return errors.New("tracing configuration invalid")
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(shutdownCtx)
	}()
	m, err := manifest()
	if err != nil {
		return err
	}
	token, err := secret("DQ_WORKER_TOKEN")
	if err != nil || len(token) < 32 {
		return errors.New("worker service token required")
	}
	insecureDevelopment := os.Getenv("DQ_INSECURE_DEVELOPMENT") == "true"
	var tlsCfg *tls.Config
	if !insecureDevelopment {
		tlsCfg, err = tlsConfig(os.Getenv("DQ_WORKER_CERT_PATH"), os.Getenv("DQ_WORKER_KEY_PATH"), os.Getenv("DQ_WORKER_CA_PATH"))
		if err != nil {
			return err
		}
	}
	accessKey, err := secret("MINIO_ACCESS_KEY")
	if err != nil {
		return err
	}
	secretKey, err := secret("MINIO_SECRET_KEY")
	if err != nil {
		return err
	}
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" || accessKey == "" || secretKey == "" {
		return errors.New("MinIO endpoint and scoped credentials required")
	}
	secure := env("MINIO_SECURE", "true") == "true"
	if !secure && env("DQ_INSECURE_DEVELOPMENT", "false") != "true" {
		return errors.New("MinIO TLS required")
	}
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: secure})
	if err != nil {
		return errors.New("invalid MinIO configuration")
	}
	checkCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	seen := map[string]bool{}
	for _, bindings := range [][]contract.Binding{m.Inputs, m.Outputs} {
		for _, binding := range bindings {
			if seen[binding.Bucket] {
				continue
			}
			seen[binding.Bucket] = true
			versioning, err := client.GetBucketVersioning(checkCtx, binding.Bucket)
			if err != nil || !versioning.Enabled() {
				return errors.New("versioned MinIO bindings required")
			}
		}
	}
	if os.Getenv("DQ_BROKERS") == "" {
		return errors.New("Redpanda brokers required")
	}
	options := []kgo.Opt{kgo.SeedBrokers(strings.Split(os.Getenv("DQ_BROKERS"), ",")...), kgo.RequiredAcks(kgo.AllISRAcks()), kgo.RecordRetries(5), kgo.ProducerBatchMaxBytes(contract.MaxRecordBytes + 4096), kgo.MaxBufferedRecords(m.Resources.Concurrency * 8), kgo.MaxBufferedBytes(m.Resources.Concurrency * contract.MaxRecordBytes * 8), kgo.ProducerLinger(0)}
	brokerSecure := env("DQ_BROKER_TLS", "true") == "true"
	if !brokerSecure && !insecureDevelopment {
		return errors.New("Redpanda TLS required")
	}
	brokerTLS := &tls.Config{MinVersion: tls.VersionTLS12}
	if path := os.Getenv("DQ_BROKER_CA_PATH"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return errors.New("broker trust unavailable")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(data) {
			return errors.New("invalid broker trust")
		}
		brokerTLS.RootCAs = roots
	}
	if brokerSecure {
		options = append(options, kgo.DialTLSConfig(brokerTLS))
	}
	username := os.Getenv("DQ_BROKER_SASL_USERNAME")
	password, err := secret("DQ_BROKER_SASL_PASSWORD")
	if err != nil {
		return err
	}
	if (username == "") != (password == "") || !insecureDevelopment && username == "" {
		return errors.New("scoped broker SASL credentials required")
	}
	if username != "" {
		options = append(options, kgo.SASL(scram.Auth{User: username, Pass: password}.AsSha256Mechanism()))
	}
	producer, err := kgo.NewClient(options...)
	if err != nil {
		return errors.New("broker configuration invalid")
	}
	defer producer.Close()
	if err = producer.Ping(checkCtx); err != nil {
		return errors.New("Redpanda unavailable")
	}
	reporter, err := broker.NewReporter(producer)
	if err != nil {
		return err
	}
	resources, err := artifact.NewFactory(artifact.NewMinIO(client), m)
	if err != nil {
		return err
	}
	host, err := worker.New(worker.Config{Manifest: m, InstancePrefix: env("DQ_WORKER_INSTANCE_PREFIX", "atheros-dedup")}, dedup.Processor{}, resources, reporter)
	if err != nil {
		return err
	}
	defer host.Close()
	listener, err := net.Listen("tcp", env("DQ_WORKER_LISTEN", ":9091"))
	if err != nil {
		return err
	}
	serverOptions := []grpc.ServerOption{grpc.UnaryInterceptor(authorized(token)), grpc.StatsHandler(otelgrpc.NewServerHandler()), grpc.MaxRecvMsgSize(contract.MaxRecordBytes + 1024), grpc.MaxSendMsgSize(contract.MaxRecordBytes + 1024)}
	if tlsCfg != nil {
		serverOptions = append(serverOptions, grpc.Creds(grpccredentials.NewTLS(tlsCfg)))
	}
	server := grpc.NewServer(serverOptions...)
	rpc.Register(server, host)
	defer server.Stop()
	errCh := make(chan error, 2)
	go func() { errCh <- server.Serve(listener) }()
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		health, err := host.Health(r.Context())
		if err != nil || !health.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	httpServer := &http.Server{Addr: env("DQ_WORKER_HTTP_LISTEN", ":8091"), Handler: mux, ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second}
	go func() { errCh <- httpServer.ListenAndServe() }()
	slog.Info("worker ready", "processor", m.ProcessorID, "version", m.Version, "concurrency", m.Resources.Concurrency)
	select {
	case <-ctx.Done():
	case err = <-errCh:
		if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, grpc.ErrServerStopped) {
			return err
		}
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownCtx)
	host.Close()
	server.GracefulStop()
	return nil
}
func main() {
	if err := run(); err != nil {
		slog.Error("worker stopped", "reason", err.Error())
		os.Exit(1)
	}
}
