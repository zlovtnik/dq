package broker

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
)

func TestObservedConsumerLagFollowsCommitsAndRebalances(t *testing.T) {
	metrics := &manager.Metrics{}
	lag := &consumerLag{metrics: metrics, partitions: map[int32]partitionLag{}}
	fetches := kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: CommandTopic, Partitions: []kgo.FetchPartition{
		{Partition: 0, HighWatermark: 20, LogStartOffset: 7},
		{Partition: 1, HighWatermark: 12, LogStartOffset: 0},
	}}}}}
	lag.observe(fetches, map[string]map[int32]kgo.EpochOffset{CommandTopic: {0: {Offset: -1}, 1: {Offset: 10}}})
	if got := metrics.ConsumerLagRecords.Load(); got != 15 {
		t.Fatalf("lag before durable receipt/commit: %d", got)
	}
	if metrics.ConsumerLagObserved.Load() == 0 {
		t.Fatal("missing observation timestamp")
	}
	// A failed receipt or commit does not call commit and cannot lower lag.
	lag.commit(0, 8)
	if got := metrics.ConsumerLagRecords.Load(); got != 14 {
		t.Fatalf("lag after acknowledged commit: %d", got)
	}
	lag.revoke([]int32{0})
	if got := metrics.ConsumerLagRecords.Load(); got != 2 {
		t.Fatalf("revoked partition still contributed lag: %d", got)
	}
	lag.commit(1, 12)
	if got := metrics.ConsumerLagRecords.Load(); got != 0 {
		t.Fatalf("fully committed partition still lagged: %d", got)
	}
	lag.revoke([]int32{1})
	if metrics.ConsumerLagObserved.Load() != 0 || len(lag.partitions) != 0 {
		t.Fatal("lost assignments retained observations")
	}
}
