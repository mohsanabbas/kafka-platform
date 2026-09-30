// Package workload generates lab traffic: a producer that can plant a poison
// record, and a strict consumer that crashes on it the way real apps do.
package workload

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mohsanabbas/kafka-platform/internal/config"
	"github.com/mohsanabbas/kafka-platform/internal/kafka"
)

const poison = "POISON"

// ErrPoison is returned by Consume when it reaches a poison record.
var ErrPoison = errors.New("workload: poison record")

// Produce writes count records to topic. Record poisonAt (1-based) is poison; 0 means none.
func Produce(ctx context.Context, cfg config.Kafka, out io.Writer, topic string, count, poisonAt int) error {
	if count < 1 || poisonAt < 0 || poisonAt > count {
		return fmt.Errorf("workload: need count >= 1 and 0 <= poison-at <= count")
	}
	cl, err := kafka.NewClient(cfg, kgo.RequiredAcks(kgo.AllISRAcks()))
	if err != nil {
		return err
	}
	defer cl.Close()

	recs := make([]*kgo.Record, 0, count)
	for i := range count {
		val := fmt.Sprintf("order-%d", i+1)
		if i+1 == poisonAt {
			val = poison
		}
		recs = append(recs, &kgo.Record{Topic: topic, Value: []byte(val)})
	}
	results := cl.ProduceSync(ctx, recs...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("workload: produce: %w", err)
	}
	fmt.Fprintf(out, "produced %d records to %s\n", count, topic)
	if poisonAt > 0 {
		r := results[poisonAt-1].Record
		fmt.Fprintf(out, "poison record at %s-%d offset %d\n", r.Topic, r.Partition, r.Offset)
	}
	return nil
}

// Consume reads topic as group and commits what it processed. On a poison
// record it commits the records before it and exits, so the group's commit
// points at the poison record and every restart crashes on it again.
func Consume(ctx context.Context, cfg config.Kafka, out io.Writer, group, topic string) error {
	cl, err := kafka.NewClient(cfg,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return err
	}
	defer cl.Close()

	fmt.Fprintf(out, "consuming %s as %s, a poison record crashes the process\n", topic, group)
	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err := fetches.Err(); err != nil {
			return fmt.Errorf("workload: poll: %w", err)
		}
		var batch []*kgo.Record
		for rec := range fetches.RecordsAll() {
			if string(rec.Value) == poison {
				if len(batch) > 0 {
					if err := cl.CommitRecords(ctx, batch...); err != nil {
						return fmt.Errorf("workload: commit: %w", err)
					}
				}
				fmt.Fprintf(out, "poison at %s-%d offset %d, crashing. the group's commit stays on this record\n", rec.Topic, rec.Partition, rec.Offset)
				return ErrPoison
			}
			batch = append(batch, rec)
		}
		if len(batch) == 0 {
			continue
		}
		if err := cl.CommitRecords(ctx, batch...); err != nil {
			return fmt.Errorf("workload: commit: %w", err)
		}
		last := batch[len(batch)-1]
		fmt.Fprintf(out, "committed %d records through %s-%d offset %d\n", len(batch), last.Topic, last.Partition, last.Offset)
	}
}
