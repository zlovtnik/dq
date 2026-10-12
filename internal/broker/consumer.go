package broker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
)

const CommandTopic = "dq.commands.v1"

type Record struct {
	Topic      string
	Partition  int32
	Offset     int64
	Key, Value []byte
}
type Consumer struct {
	Client  *kgo.Client
	Control control.API
	Scope   contract.Scope
	Group   string
	Metrics *manager.Metrics
}

func (c *Consumer) ProcessRecord(ctx context.Context, record Record, commit func(context.Context) error) error {
	if err := c.Receipt(ctx, record); err != nil {
		return err
	}
	return commit(ctx)
}

func New(c config.Config, api control.API, metrics *manager.Metrics) (*Consumer, error) {
	if len(c.Brokers) == 0 {
		return nil, errors.New("Redpanda endpoints required")
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...), kgo.ConsumerGroup(c.ConsumerGroup), kgo.ConsumeTopics(CommandTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit(), kgo.BlockRebalanceOnPoll(), kgo.FetchMaxPartitionBytes(contract.MaxRecordBytes), kgo.FetchMaxBytes(1024 * 1024), kgo.RebalanceTimeout(5 * time.Minute)}
	if c.BrokerTLS {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
		if c.BrokerCA != "" {
			ca, err := os.ReadFile(c.BrokerCA)
			if err != nil {
				return nil, errors.New("broker CA unavailable")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(ca) {
				return nil, errors.New("invalid broker CA")
			}
			tlsConfig.RootCAs = roots
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	if c.BrokerUsername != "" {
		opts = append(opts, kgo.SASL(scram.Auth{User: c.BrokerUsername, Pass: c.BrokerPassword}.AsSha256Mechanism()))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, errors.New("invalid Redpanda configuration")
	}
	return &Consumer{Client: client, Control: api, Scope: contract.Scope{InstallationID: c.InstallationID, Namespace: c.Namespace}, Group: c.ConsumerGroup, Metrics: metrics}, nil
}

// Receipt never commits offsets itself. The caller may advance an offset only
// after this authenticated durable receipt (including quarantine) succeeds.
func (c *Consumer) Receipt(ctx context.Context, record Record) error {
	if record.Topic != CommandTopic || record.Partition < 0 || record.Offset < 0 {
		return errors.New("invalid broker coordinate")
	}
	receipt := contract.Receipt{Scope: c.Scope, ConsumerGroup: c.Group, Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, PayloadSHA256: contract.SHA256(record.Value)}
	envelope, reason := c.decode(record)
	ctx = telemetry.Extract(ctx, envelope.Traceparent, envelope.Tracestate)
	ctx, span := telemetry.Start(ctx, "dq.receipt")
	defer span.End()
	if reason != "" {
		receipt.QuarantineReason = reason
		receipt.EventID = "quarantine:" + record.Topic + ":" + strconv.Itoa(int(record.Partition)) + ":" + strconv.FormatInt(record.Offset, 10)
	} else {
		receipt.EventID = envelope.EventID
		receipt.Envelope = &envelope
	}
	ctx = control.ServiceContext(ctx)
	if err := c.Control.Do(ctx, http.MethodPost, "/receipts", nil, receipt, nil); err != nil {
		return err
	}
	if c.Metrics != nil {
		c.Metrics.Receipts.Add(1)
		if reason != "" {
			c.Metrics.Quarantines.Add(1)
		}
	}
	return nil
}

func (c *Consumer) quarantineRejection(ctx context.Context, record Record) error {
	receipt := contract.Receipt{
		Scope: c.Scope, ConsumerGroup: c.Group, Topic: record.Topic,
		Partition: record.Partition, Offset: record.Offset,
		EventID:       "quarantine:" + record.Topic + ":" + strconv.Itoa(int(record.Partition)) + ":" + strconv.FormatInt(record.Offset, 10),
		PayloadSHA256: contract.SHA256(record.Value), QuarantineReason: "coordinator_rejected",
	}
	if err := c.Control.Do(control.ServiceContext(ctx), http.MethodPost, "/receipts", nil, receipt, nil); err != nil {
		return err
	}
	if c.Metrics != nil {
		c.Metrics.Receipts.Add(1)
		c.Metrics.Quarantines.Add(1)
	}
	return nil
}

func (c *Consumer) decode(record Record) (contract.Envelope, string) {
	var envelope contract.Envelope
	if len(record.Value) > contract.MaxRecordBytes {
		return envelope, "record_too_large"
	}
	canonical, err := manager.CanonicalJSON(record.Value)
	if err != nil {
		return envelope, "invalid_json"
	}
	d := json.NewDecoder(bytes.NewReader(canonical))
	d.DisallowUnknownFields()
	if d.Decode(&envelope) != nil || d.Decode(new(any)) != io.EOF {
		return envelope, "invalid_envelope"
	}
	if _, err := contract.EncodeEnvelope(envelope); err != nil {
		return envelope, "invalid_envelope"
	}
	if envelope.InstallationID != c.Scope.InstallationID || envelope.Namespace != c.Scope.Namespace {
		return envelope, "forbidden_scope"
	}
	if !contract.ValidSegment(envelope.RunID) || !contract.ValidSegment(envelope.ProcessorID) || !contract.ValidSegment(envelope.ProcessorVersion) || envelope.OccurredAt.IsZero() || envelope.CorrelationID == "" {
		return envelope, "invalid_envelope"
	}
	if string(record.Key) != envelope.InstallationID+"/"+envelope.Namespace+"/"+envelope.RunID {
		return envelope, "invalid_key"
	}
	switch envelope.EventType {
	case "invoke", "dispatch", "retry_dispatch", "cancel":
	default:
		return envelope, "unsupported_command"
	}
	for _, ref := range envelope.Artifacts {
		u, err := url.Parse(ref.URI)
		if err != nil || u.Scheme != "s3" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || ref.InstallationID != c.Scope.InstallationID || ref.Namespace != c.Scope.Namespace {
			return envelope, "forbidden_artifact_reference"
		}
	}
	return envelope, ""
}

func (c *Consumer) Run(ctx context.Context) error {
	defer c.Client.Close()
	for {
		fetches := c.Client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			c.Client.AllowRebalance()
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			c.Client.AllowRebalance()
			if err := wait(ctx, jitter(time.Second)); err != nil {
				return nil
			}
			continue
		}
		for _, record := range fetches.Records() {
			delay := time.Second
			for {
				err := c.Receipt(ctx, Record{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: record.Key, Value: record.Value})
				if err == nil {
					break
				}
				var status *control.StatusError
				if errors.As(err, &status) && status.Status >= 400 && status.Status < 500 && status.Status != http.StatusTooManyRequests {
					err = c.quarantineRejection(ctx, Record{Topic: record.Topic, Partition: record.Partition, Offset: record.Offset, Key: record.Key, Value: record.Value})
					if err == nil {
						break
					}
				}
				if !retryable(err) {
					return err
				}
				if err := wait(ctx, jitter(delay)); err != nil {
					c.Client.AllowRebalance()
					return nil
				}
				if delay < time.Minute {
					delay *= 2
					if delay > time.Minute {
						delay = time.Minute
					}
				}
			}
			if err := c.Client.CommitRecords(ctx, record); err != nil {
				c.Client.AllowRebalance()
				return errors.New("broker offset commit unavailable")
			}
		}
		c.Client.AllowRebalance()
	}
}

func retryable(err error) bool {
	if errors.Is(err, control.ErrUnavailable) {
		return true
	}
	var status *control.StatusError
	return errors.As(err, &status) && (status.Status == http.StatusTooManyRequests || status.Status >= 500)
}

func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
func jitter(max time.Duration) time.Duration {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return max / 2
	}
	return time.Duration(binary.LittleEndian.Uint64(b[:]) % uint64(max))
}
