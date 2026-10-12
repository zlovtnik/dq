// Package broker provides the declared worker-report transport.
package broker

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
)

const WorkerReportsTopic = "dq.worker-reports.v1"

type Reporter struct{ client *kgo.Client }

func NewReporter(client *kgo.Client) (*Reporter, error) {
	if client == nil {
		return nil, errors.New("broker client required")
	}
	return &Reporter{client: client}, nil
}
func (r *Reporter) Report(ctx context.Context, event contract.Envelope) error {
	ctx, span := telemetry.Start(telemetry.Extract(ctx, event.Traceparent, event.Tracestate), "broker.worker_report")
	defer span.End()
	event.Traceparent, event.Tracestate = telemetry.Fields(ctx)
	switch event.EventType {
	case "started", "progress", "output_ready", "attempt_failed":
	default:
		return errors.New("worker report event forbidden")
	}
	data, err := contract.EncodeEnvelope(event)
	if err != nil {
		return err
	}
	key := event.InstallationID + "/" + event.Namespace + "/" + event.RunID
	err = r.client.ProduceSync(ctx, &kgo.Record{Topic: WorkerReportsTopic, Key: []byte(key), Value: data, Headers: []kgo.RecordHeader{{Key: "traceparent", Value: []byte(event.Traceparent)}, {Key: "tracestate", Value: []byte(event.Tracestate)}}}).FirstErr()
	if err != nil {
		telemetry.Fail(span)
	}
	return err
}
