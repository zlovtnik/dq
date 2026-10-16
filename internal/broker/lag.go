package broker

import (
	"math"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
)

type partitionLag struct {
	highWatermark int64
	committed     int64
}

// consumerLag stores only this consumer's observed command partitions. Broker
// high watermarks come from existing fetches; successful commits follow durable
// receipts. Rebalance callbacks remove partitions that belong to another member.
type consumerLag struct {
	mu         sync.Mutex
	metrics    *manager.Metrics
	partitions map[int32]partitionLag
}

func (l *consumerLag) observe(fetches kgo.Fetches, committed map[string]map[int32]kgo.EpochOffset) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
		if partition.Topic != CommandTopic || partition.Err != nil || partition.Partition < 0 || partition.HighWatermark < 0 {
			return
		}
		next := partition.LogStartOffset
		if offset, ok := committed[CommandTopic][partition.Partition]; ok && offset.Offset > next {
			next = offset.Offset
		}
		l.partitions[partition.Partition] = partitionLag{highWatermark: partition.HighWatermark, committed: next}
		if l.metrics != nil {
			l.metrics.ConsumerLagObserved.Store(time.Now().Unix())
		}
	})
	l.publish()
}

func (l *consumerLag) commit(partition int32, next int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry, ok := l.partitions[partition]; ok {
		entry.committed = next
		l.partitions[partition] = entry
	}
	l.publish()
}

func (l *consumerLag) revoke(partitions []int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, partition := range partitions {
		delete(l.partitions, partition)
	}
	if len(l.partitions) == 0 && l.metrics != nil {
		l.metrics.ConsumerLagObserved.Store(0)
	}
	l.publish()
}

func (l *consumerLag) publish() {
	if l.metrics == nil {
		return
	}
	var total int64
	for _, entry := range l.partitions {
		lag := entry.highWatermark - entry.committed
		if lag <= 0 {
			continue
		}
		if lag > math.MaxInt64-total {
			total = math.MaxInt64
			break
		}
		total += lag
	}
	l.metrics.ConsumerLagRecords.Store(total)
}
