package consumergroup

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"time"
)

// Errors returned by Plan and Apply, for errors.Is. All four are refusals:
// nothing was committed.
var (
	ErrInvalid     = errors.New("consumergroup: invalid request")
	ErrEmptyPlan   = errors.New("consumergroup: plan has no moves")
	ErrGroupActive = errors.New("consumergroup: group has active members, stop the app first")
	ErrStalePlan   = errors.New("consumergroup: committed offsets changed since the plan, plan again")
)

// Mode is where a group's committed offsets are moved.
type Mode string

const (
	// ModeSkip moves one record past the committed offset: the poison record the app keeps crashing on.
	ModeSkip Mode = "skip"
	// ModeLatest parks the group at the log end. Everything unread is dropped.
	ModeLatest Mode = "latest"
	// ModeEarliest replays everything still in retention.
	ModeEarliest Mode = "earliest"
	// ModeTimestamp replays from the first record at or after a time, for example the start of a bad deploy.
	ModeTimestamp Mode = "timestamp"
)

// Request selects the partitions to move and where to move them.
type Request struct {
	Group string `json:"group"`
	Topic string `json:"topic"`
	Mode  Mode   `json:"mode"`
	// Partitions limits the plan. Empty means every partition of Topic.
	Partitions []int32 `json:"partitions,omitempty"`
	// Timestamp is required for ModeTimestamp.
	Timestamp time.Time `json:"timestamp,omitzero"`
}

func (r Request) validate() error {
	switch {
	case r.Group == "":
		return fmt.Errorf("%w: group is required", ErrInvalid)
	case r.Topic == "":
		return fmt.Errorf("%w: topic is required", ErrInvalid)
	}
	switch r.Mode {
	case ModeSkip, ModeLatest, ModeEarliest:
	case ModeTimestamp:
		if r.Timestamp.IsZero() {
			return fmt.Errorf("%w: timestamp mode needs a timestamp", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: unknown mode %q", ErrInvalid, r.Mode)
	}
	return nil
}

// Move is one partition's offset change. From is the commit seen at plan time, -1 if none.
// End is the log end offset at plan time, so End-To is the lag right after apply.
type Move struct {
	Partition int32 `json:"partition"`
	From      int64 `json:"from"`
	To        int64 `json:"to"`
	End       int64 `json:"end"`
}

// Plan is a reviewed, replayable offset change. Apply refuses it if any From
// no longer matches the live commit.
type Plan struct {
	Request    Request   `json:"request"`
	GroupState string    `json:"groupState"`
	Members    int       `json:"members"`
	Moves      []Move    `json:"moves"`
	CreatedAt  time.Time `json:"createdAt"`
}

// partitionOffsets is everything known about one partition at plan time.
type partitionOffsets struct {
	partition int32
	committed int64
	start     int64
	end       int64
	atTime    int64
}

func target(mode Mode, p partitionOffsets) int64 {
	switch mode {
	case ModeLatest:
		return p.end
	case ModeEarliest:
		return p.start
	case ModeTimestamp:
		return p.atTime
	case ModeSkip:
		if p.committed >= p.start && p.committed < p.end {
			return p.committed + 1
		}
	}
	// Skip with no commit, or with nothing left to read, leaves the partition alone.
	return p.committed
}

func planMoves(mode Mode, parts []partitionOffsets) []Move {
	moves := []Move{}
	for _, p := range parts {
		to := target(mode, p)
		if to == p.committed {
			continue
		}
		moves = append(moves, Move{Partition: p.partition, From: p.committed, To: to, End: p.end})
	}
	slices.SortFunc(moves, func(a, b Move) int { return cmp.Compare(a.Partition, b.Partition) })
	return moves
}
