// Local acceptance probe. Credentials come only from its ephemeral environment.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/sse"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func input(value any) error { return json.NewDecoder(io.LimitReader(os.Stdin, 65537)).Decode(value) }
func storage() (*minio.Client, error) {
	return minio.New(os.Getenv("MINIO_ENDPOINT"), &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("MINIO_ACCESS_KEY"), os.Getenv("MINIO_SECRET_KEY"), ""), Secure: false})
}
func run(ctx context.Context, action string) (any, error) {
	switch action {
	case "setup-storage":
		client, err := storage()
		if err != nil {
			return nil, err
		}
		if err = client.MakeBucket(ctx, "dq-artifacts", minio.MakeBucketOptions{}); err != nil {
			return nil, err
		}
		if err = client.SetBucketVersioning(ctx, "dq-artifacts", minio.BucketVersioningConfiguration{Status: "Enabled"}); err != nil {
			return nil, err
		}
		if err = client.SetBucketEncryption(ctx, "dq-artifacts", sse.NewConfigurationSSES3()); err != nil {
			return nil, err
		}
		return map[string]bool{"versioned": true, "encrypted": true}, nil
	case "read-artifact":
		var ref contract.ArtifactRef
		if err := input(&ref); err != nil {
			return nil, err
		}
		if err := ref.Validate(); err != nil {
			return nil, err
		}
		u, err := url.Parse(ref.URI)
		if err != nil || u.Scheme != "s3" {
			return nil, errors.New("invalid artifact URI")
		}
		client, err := storage()
		if err != nil {
			return nil, err
		}
		started := time.Now()
		object, err := client.GetObject(ctx, u.Host, strings.TrimPrefix(u.Path, "/"), minio.GetObjectOptions{VersionID: ref.VersionID})
		if err != nil {
			return nil, err
		}
		defer object.Close()
		stat, err := object.Stat()
		if err != nil {
			return nil, err
		}
		hash := sha256.New()
		count, err := io.Copy(hash, io.LimitReader(object, ref.SizeBytes+1))
		if err != nil || count != ref.SizeBytes || hex.EncodeToString(hash.Sum(nil)) != ref.SHA256 {
			return nil, errors.New("artifact byte integrity mismatch")
		}
		if stat.VersionID != ref.VersionID || stat.ETag != ref.ETag {
			return nil, errors.New("artifact exact version mismatch")
		}
		if stat.Metadata.Get("X-Amz-Server-Side-Encryption") != "AES256" {
			return nil, errors.New("artifact server-side encryption missing")
		}
		elapsed := time.Since(started)
		return map[string]any{"size_bytes": count, "sha256_verified": true, "exact_version_verified": true,
			"server_side_encryption_verified": true, "elapsed_ms": float64(elapsed.Microseconds()) / 1000,
			"throughput_mib_per_second": float64(count) / (1024 * 1024) / elapsed.Seconds()}, nil
	case "health", "run", "cancel":
		connection, err := grpc.NewClient(os.Getenv("DQ_PROBE_WORKER"), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		defer connection.Close()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+os.Getenv("DQ_WORKER_TOKEN"))
		client := rpc.NewClient(connection)
		if action == "health" {
			return client.Health(ctx)
		}
		if action == "cancel" {
			var request contract.CancelRequest
			if err = input(&request); err != nil {
				return nil, err
			}
			return client.Cancel(ctx, request)
		}
		var request contract.RunRequest
		if err = input(&request); err != nil {
			return nil, err
		}
		return client.Run(ctx, request)
	case "produce":
		var record struct {
			Topic   string `json:"topic"`
			Key     string `json:"key"`
			Payload string `json:"payload"`
		}
		if err := input(&record); err != nil {
			return nil, err
		}
		client, err := kgo.NewClient(kgo.SeedBrokers(os.Getenv("DQ_BROKERS")), kgo.RequiredAcks(kgo.AllISRAcks()))
		if err != nil {
			return nil, err
		}
		defer client.Close()
		r := &kgo.Record{Topic: record.Topic, Key: []byte(record.Key), Value: []byte(record.Payload)}
		if err = client.ProduceSync(ctx, r).FirstErr(); err != nil {
			return nil, err
		}
		return map[string]any{"partition": r.Partition, "offset": r.Offset}, nil
	default:
		return nil, errors.New("unknown probe action")
	}
}
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "one probe action required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	value, err := run(ctx, os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(value); err != nil {
		os.Exit(1)
	}
}
