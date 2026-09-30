package cluster

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestAssess(t *testing.T) {
	rack := "use1-az1"
	meta := kadm.Metadata{
		Cluster: "lab",
		Brokers: kadm.BrokerDetails{{NodeID: 2, Host: "kafka-2"}, {NodeID: 1, Host: "kafka-1", Rack: &rack}},
		Topics: kadm.TopicDetails{
			"orders": {
				Topic: "orders",
				Partitions: kadm.PartitionDetails{
					0: {Topic: "orders", Partition: 0, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
					1: {Topic: "orders", Partition: 1, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2}},
					2: {Topic: "orders", Partition: 2, Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1}},
					3: {Topic: "orders", Partition: 3, Leader: -1, Replicas: []int32{1, 2, 3}, Err: errors.New("leader not available")},
				},
			},
		},
	}

	h := assess(meta, map[string]int{"orders": 2})

	if h.IsHealthy() {
		t.Fatal("expected unhealthy")
	}
	if h.Brokers[0].ID != 1 || h.Brokers[0].Rack != rack {
		t.Errorf("brokers not sorted with rack: %+v", h.Brokers)
	}
	if h.Topics != 1 || h.Partitions != 4 {
		t.Errorf("counts = %d topics %d partitions", h.Topics, h.Partitions)
	}
	check := func(name string, got []Partition, expected int32) {
		t.Helper()
		if len(got) != 1 || got[0].Partition != expected {
			t.Errorf("%s = %+v, expected partition %d only", name, got, expected)
		}
	}
	check("under replicated", h.UnderReplicated, 1)
	check("under min isr", h.UnderMinISR, 2)
	check("offline", h.Offline, 3)
}

func TestAssessHealthy(t *testing.T) {
	meta := kadm.Metadata{Topics: kadm.TopicDetails{
		"orders": {Topic: "orders", Partitions: kadm.PartitionDetails{
			0: {Topic: "orders", Leader: 1, Replicas: []int32{1, 2, 3}, ISR: []int32{1, 2, 3}},
		}},
	}}
	if h := assess(meta, nil); !h.IsHealthy() {
		t.Fatalf("expected healthy, got %+v", h)
	}
}
