// Package cluster reports broker and partition health, the first thing to check in any Kafka incident.
package cluster

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Admin is the slice of the Kafka admin API this package needs. *kadm.Client satisfies it.
type Admin interface {
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
}

// Broker is one broker as advertised in metadata. Rack is empty when broker.rack is unset.
type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack"`
}

// Partition is one partition's replica state. Leader is -1 when it has no leader.
// MinISR is 0 when the topic config could not be read.
type Partition struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Leader    int32   `json:"leader"`
	Replicas  []int32 `json:"replicas"`
	ISR       []int32 `json:"isr"`
	MinISR    int     `json:"minIsr,omitempty"`
	Error     string  `json:"error,omitempty"`
}

// Health is a point-in-time view of the cluster.
//
// UnderMinISR is the one that pages: producers with acks=all get
// NOT_ENOUGH_REPLICAS on those partitions.
type Health struct {
	ClusterID       string      `json:"clusterId"`
	Brokers         []Broker    `json:"brokers"`
	Topics          int         `json:"topics"`
	Partitions      int         `json:"partitions"`
	Offline         []Partition `json:"offline"`
	UnderMinISR     []Partition `json:"underMinIsr"`
	UnderReplicated []Partition `json:"underReplicated"`
}

// IsHealthy reports whether every partition has a leader and a full ISR.
func (h Health) IsHealthy() bool {
	return len(h.Offline) == 0 && len(h.UnderMinISR) == 0 && len(h.UnderReplicated) == 0
}

// Service computes cluster health from the admin API.
type Service struct {
	admin Admin
}

// New returns a Service that reads through admin.
func New(admin Admin) *Service {
	return &Service{admin: admin}
}

// Health reads metadata for every topic, including internal ones, and each
// topic's min.insync.replicas. Each unhealthy partition is listed once, under
// the worst condition that applies: offline, then under min ISR, then under-replicated.
func (s *Service) Health(ctx context.Context) (Health, error) {
	meta, err := s.admin.Metadata(ctx)
	if err != nil {
		return Health{}, fmt.Errorf("cluster: metadata: %w", err)
	}
	configs, err := s.admin.DescribeTopicConfigs(ctx, slices.Sorted(maps.Keys(meta.Topics))...)
	if err != nil {
		return Health{}, fmt.Errorf("cluster: topic configs: %w", err)
	}
	return assess(meta, minISRByTopic(configs)), nil
}

func minISRByTopic(configs kadm.ResourceConfigs) map[string]int {
	out := make(map[string]int, len(configs))
	for _, rc := range configs {
		if rc.Err != nil {
			continue
		}
		for _, c := range rc.Configs {
			if c.Key != "min.insync.replicas" || c.Value == nil {
				continue
			}
			if n, err := strconv.Atoi(*c.Value); err == nil {
				out[rc.Name] = n
			}
		}
	}
	return out
}

func assess(meta kadm.Metadata, minISR map[string]int) Health {
	h := Health{
		ClusterID:       meta.Cluster,
		Brokers:         make([]Broker, 0, len(meta.Brokers)),
		Offline:         []Partition{},
		UnderMinISR:     []Partition{},
		UnderReplicated: []Partition{},
	}
	for _, b := range meta.Brokers {
		br := Broker{ID: b.NodeID, Host: b.Host, Port: b.Port}
		if b.Rack != nil {
			br.Rack = *b.Rack
		}
		h.Brokers = append(h.Brokers, br)
	}
	slices.SortFunc(h.Brokers, func(a, b Broker) int { return cmp.Compare(a.ID, b.ID) })

	for _, t := range meta.Topics.Sorted() {
		h.Topics++
		for _, p := range t.Partitions.Sorted() {
			h.Partitions++
			part := Partition{
				Topic:     p.Topic,
				Partition: p.Partition,
				Leader:    p.Leader,
				Replicas:  p.Replicas,
				ISR:       p.ISR,
				MinISR:    minISR[p.Topic],
			}
			switch {
			case p.Err != nil || p.Leader < 0:
				if p.Err != nil {
					part.Error = p.Err.Error()
				}
				h.Offline = append(h.Offline, part)
			case part.MinISR > 0 && len(p.ISR) < part.MinISR:
				h.UnderMinISR = append(h.UnderMinISR, part)
			case len(p.ISR) < len(p.Replicas):
				h.UnderReplicated = append(h.UnderReplicated, part)
			}
		}
	}
	return h
}
