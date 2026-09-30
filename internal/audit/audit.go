// Package audit records who changed what on the cluster, and why.
//
// Events go to the structured log and to a Kafka topic, so the trail survives
// the process and shows up in Kafka UI next to the incident it belongs to.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultTopic is created by infra/kafka/create-topics.sh and the Terraform module.
const DefaultTopic = "platform.audit"

// Outcome is how an attempted change ended.
type Outcome string

const (
	OutcomeApplied Outcome = "applied"
	// OutcomeRefused means a guardrail stopped the change before it reached Kafka.
	OutcomeRefused Outcome = "refused"
	// OutcomeFailed means Kafka or the network rejected the change.
	OutcomeFailed Outcome = "failed"
)

// Event is one attempted change. Target is the record key, so every event for
// the same group and topic lands on one partition in order.
type Event struct {
	Time    time.Time `json:"time"`
	Actor   string    `json:"actor"`
	Action  string    `json:"action"`
	Target  string    `json:"target"`
	Reason  string    `json:"reason"`
	Outcome Outcome   `json:"outcome"`
	Error   string    `json:"error,omitempty"`
	Detail  any       `json:"detail,omitempty"`
}

// Producer is the part of *kgo.Client the log writes through.
type Producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// Log writes audit events. It is safe for concurrent use if producer is.
type Log struct {
	logger   *slog.Logger
	producer Producer
	topic    string
}

// New returns a Log that writes to logger and produces to topic.
func New(logger *slog.Logger, producer Producer, topic string) *Log {
	return &Log{logger: logger, producer: producer, topic: topic}
}

// Record writes e to the log and the audit topic. The log line is written
// even when the produce fails, and the produce error is returned.
func (l *Log) Record(ctx context.Context, e Event) error {
	if e.Time.IsZero() {
		e.Time = time.Now().UTC()
	}
	l.logger.LogAttrs(ctx, slog.LevelInfo, "audit",
		slog.String("actor", e.Actor),
		slog.String("action", e.Action),
		slog.String("target", e.Target),
		slog.String("reason", e.Reason),
		slog.String("outcome", string(e.Outcome)),
		slog.String("error", e.Error),
	)
	value, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("audit: marshal: %w", err)
	}
	rec := &kgo.Record{Topic: l.topic, Key: []byte(e.Target), Value: value}
	if err := l.producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("audit: produce to %s: %w", l.topic, err)
	}
	return nil
}
