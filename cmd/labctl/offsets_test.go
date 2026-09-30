package main

import (
	"slices"
	"testing"
	"time"
)

func TestParseAt(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		input    string
		expected time.Time
		isValid  bool
	}{
		{"30m", now.Add(-30 * time.Minute), true},
		{"-2h", now.Add(-2 * time.Hour), true},
		{"2026-09-27T10:30:00Z", time.Date(2026, 9, 27, 10, 30, 0, 0, time.UTC), true},
		{"yesterday", time.Time{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseAt(tt.input, now)
			if tt.isValid != (err == nil) {
				t.Fatalf("parseAt() error = %v, expected valid %v", err, tt.isValid)
			}
			if !got.Equal(tt.expected) {
				t.Errorf("got %s, expected %s", got, tt.expected)
			}
		})
	}
}

func TestParsePartitions(t *testing.T) {
	got, err := parsePartitions("0, 3,5")
	if err != nil || !slices.Equal(got, []int32{0, 3, 5}) {
		t.Fatalf("parsePartitions() = %v, %v", got, err)
	}
	if _, err := parsePartitions("0,x"); err == nil {
		t.Error("expected an error for a non-numeric partition")
	}
	if got, _ := parsePartitions(""); got != nil {
		t.Errorf("empty input = %v, expected nil", got)
	}
}
