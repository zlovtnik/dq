package manager

import (
	"fmt"
	"io"
	"strings"
	"time"
)

var dispatchDurationBuckets = [...]time.Duration{
	time.Millisecond, 2 * time.Millisecond, 5 * time.Millisecond,
	10 * time.Millisecond, 20 * time.Millisecond, 50 * time.Millisecond,
	100 * time.Millisecond, 200 * time.Millisecond, 500 * time.Millisecond,
	time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second,
}

// ObserveDispatch measures the complete successful dispatch, including the
// durable reservation and acceptance. Its counter supports throughput rates.
func (m *Metrics) ObserveDispatch(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	m.Dispatches.Add(1)
	m.dispatchNanoseconds += uint64(duration)
	for index, upper := range dispatchDurationBuckets {
		if duration <= upper {
			m.dispatchBuckets[index]++
		}
	}
}

func (m *Metrics) WritePrometheus(w io.Writer) error {
	var output strings.Builder
	m.dispatchMu.Lock()
	count := m.Dispatches.Load()
	_, _ = fmt.Fprintf(&output, "# TYPE dq_dispatch_total counter\ndq_dispatch_total %d\n# TYPE dq_dispatch_duration_seconds histogram\n", count)
	for index, upper := range dispatchDurationBuckets {
		_, _ = fmt.Fprintf(&output, "dq_dispatch_duration_seconds_bucket{le=\"%g\"} %d\n", upper.Seconds(), m.dispatchBuckets[index])
	}
	_, _ = fmt.Fprintf(&output, "dq_dispatch_duration_seconds_bucket{le=\"+Inf\"} %d\ndq_dispatch_duration_seconds_sum %.9f\ndq_dispatch_duration_seconds_count %d\n", count, float64(m.dispatchNanoseconds)/float64(time.Second), count)
	m.dispatchMu.Unlock()
	_, _ = fmt.Fprintf(&output, "dq_dispatch_errors_total %d\ndq_receipts_total %d\ndq_quarantines_total %d\ndq_lease_renewals_total %d\ndq_poll_errors_total %d\n", m.DispatchErrors.Load(), m.Receipts.Load(), m.Quarantines.Load(), m.Renewals.Load(), m.PollErrors.Load())
	_, _ = fmt.Fprintf(&output, "# HELP dq_consumer_observed_lag_records Last fetched high watermarks minus acknowledged committed offsets for this manager's observed assigned partitions.\n# TYPE dq_consumer_observed_lag_records gauge\ndq_consumer_observed_lag_records %d\n# TYPE dq_consumer_lag_observed_timestamp_seconds gauge\ndq_consumer_lag_observed_timestamp_seconds %d\n", m.ConsumerLagRecords.Load(), m.ConsumerLagObserved.Load())
	_, err := io.WriteString(w, output.String())
	return err
}
