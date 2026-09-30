package consumergroup

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func TestPlanMoves(t *testing.T) {
	// Partition 0 is stuck on a poison record at 10, partition 1 is caught up,
	// partition 2 has never been committed.
	parts := []partitionOffsets{
		{partition: 1, committed: 20, start: 0, end: 20, atTime: 15},
		{partition: 0, committed: 10, start: 0, end: 20, atTime: 5},
		{partition: 2, committed: -1, start: 3, end: 8, atTime: 8},
	}
	tests := []struct {
		mode     Mode
		expected []Move
	}{
		{ModeSkip, []Move{{Partition: 0, From: 10, To: 11, End: 20}}},
		{ModeLatest, []Move{{Partition: 0, From: 10, To: 20, End: 20}, {Partition: 2, From: -1, To: 8, End: 8}}},
		{ModeEarliest, []Move{
			{Partition: 0, From: 10, To: 0, End: 20},
			{Partition: 1, From: 20, To: 0, End: 20},
			{Partition: 2, From: -1, To: 3, End: 8},
		}},
		{ModeTimestamp, []Move{
			{Partition: 0, From: 10, To: 5, End: 20},
			{Partition: 1, From: 20, To: 15, End: 20},
			{Partition: 2, From: -1, To: 8, End: 8},
		}},
	}
	for _, tt := range tests {
		t.Run(string(tt.mode), func(t *testing.T) {
			got := planMoves(tt.mode, parts)
			if !slices.Equal(got, tt.expected) {
				t.Errorf("got %+v, expected %+v", got, tt.expected)
			}
		})
	}
}

func TestRequestValidate(t *testing.T) {
	tests := []struct {
		name    string
		req     Request
		isValid bool
	}{
		{"skip", Request{Group: "g", Topic: "t", Mode: ModeSkip}, true},
		{"timestamp with time", Request{Group: "g", Topic: "t", Mode: ModeTimestamp, Timestamp: time.Now()}, true},
		{"timestamp without time", Request{Group: "g", Topic: "t", Mode: ModeTimestamp}, false},
		{"missing group", Request{Topic: "t", Mode: ModeSkip}, false},
		{"missing topic", Request{Group: "g", Mode: ModeSkip}, false},
		{"unknown mode", Request{Group: "g", Topic: "t", Mode: "rewind"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.req.validate()
			if tt.isValid != (err == nil) {
				t.Fatalf("validate() = %v, expected valid %v", err, tt.isValid)
			}
			if err != nil && !errors.Is(err, ErrInvalid) {
				t.Errorf("error %v does not wrap ErrInvalid", err)
			}
		})
	}
}
