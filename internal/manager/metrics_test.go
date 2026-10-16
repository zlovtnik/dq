package manager

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDispatchHistogramHasCumulativeBucketsAndThroughputCount(t *testing.T) {
	metrics := &Metrics{}
	var workers sync.WaitGroup
	for range 100 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			metrics.ObserveDispatch(5 * time.Millisecond)
		}()
	}
	workers.Wait()
	metrics.ObserveDispatch(11 * time.Second)
	var body bytes.Buffer
	if err := metrics.WritePrometheus(&body); err != nil {
		t.Fatal(err)
	}
	for _, line := range []string{
		"# TYPE dq_dispatch_duration_seconds histogram\n",
		"dq_dispatch_total 101\n",
		"dq_dispatch_duration_seconds_bucket{le=\"0.002\"} 0\n",
		"dq_dispatch_duration_seconds_bucket{le=\"0.005\"} 100\n",
		"dq_dispatch_duration_seconds_bucket{le=\"10\"} 100\n",
		"dq_dispatch_duration_seconds_bucket{le=\"+Inf\"} 101\n",
		"dq_dispatch_duration_seconds_sum 11.500000000\n",
		"dq_dispatch_duration_seconds_count 101\n",
	} {
		if !strings.Contains(body.String(), line) {
			t.Fatalf("missing histogram line %q in %s", line, body.String())
		}
	}
}
