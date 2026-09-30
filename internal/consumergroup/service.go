// Package consumergroup reads consumer lag and moves committed offsets with guardrails.
//
// Offset changes follow plan and apply, like Terraform: a plan records the
// commits it saw, and apply refuses to run if the group is still active or if
// any commit moved since the plan.
package consumergroup

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/mohsanabbas/kafka-platform/internal/audit"
)

// Admin is the slice of the Kafka admin API this package needs. *kadm.Client satisfies it.
type Admin interface {
	Lag(ctx context.Context, groups ...string) (kadm.DescribedGroupLags, error)
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	FetchOffsets(ctx context.Context, group string) (kadm.OffsetResponses, error)
	ListStartOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)
	CommitOffsets(ctx context.Context, group string, os kadm.Offsets) (kadm.OffsetResponses, error)
}

// Auditor records offset changes. *audit.Log satisfies it.
type Auditor interface {
	Record(ctx context.Context, e audit.Event) error
}

// PartitionLag is one partition's position. Committed is -1 when the group has
// no commit there. Member is client ID and host of the current owner, if any.
type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"`
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
	Member    string `json:"member,omitempty"`
}

// Lag is a group's lag across every partition it has committed to or is assigned.
type Lag struct {
	Group      string         `json:"group"`
	State      string         `json:"state"`
	Members    int            `json:"members"`
	Total      int64          `json:"total"`
	Partitions []PartitionLag `json:"partitions"`
}

// Service reads lag and moves committed offsets. It is safe for concurrent use.
type Service struct {
	admin   Admin
	auditor Auditor
	now     func() time.Time
}

// New returns a Service. Every Apply attempt is recorded through auditor.
func New(admin Admin, auditor Auditor) *Service {
	return &Service{admin: admin, auditor: auditor, now: time.Now}
}

// Lag returns lag for group, sorted by topic and partition.
func (s *Service) Lag(ctx context.Context, group string) (Lag, error) {
	lags, err := s.admin.Lag(ctx, group)
	if err != nil {
		return Lag{}, fmt.Errorf("consumergroup: lag %s: %w", group, err)
	}
	gl, ok := lags[group]
	if !ok {
		return Lag{}, fmt.Errorf("consumergroup: group %s missing from lag response", group)
	}
	if err := gl.Error(); err != nil {
		return Lag{}, fmt.Errorf("consumergroup: lag %s: %w", group, err)
	}
	out := Lag{
		Group:      group,
		State:      gl.State,
		Members:    len(gl.Members),
		Total:      gl.Lag.Total(),
		Partitions: []PartitionLag{},
	}
	for _, l := range gl.Lag.Sorted() {
		pl := PartitionLag{Topic: l.Topic, Partition: l.Partition, Committed: l.Commit.At, End: l.End.Offset, Lag: l.Lag}
		if l.Member != nil {
			pl.Member = l.Member.ClientID + "/" + l.Member.ClientHost
		}
		out.Partitions = append(out.Partitions, pl)
	}
	return out, nil
}

// Plan computes offset moves without changing anything. It returns ErrInvalid
// for a bad request or a partition the topic does not have. A plan with no
// moves is valid, but Apply refuses it.
func (s *Service) Plan(ctx context.Context, req Request) (Plan, error) {
	if err := req.validate(); err != nil {
		return Plan{}, err
	}
	state, members, err := s.describe(ctx, req.Group)
	if err != nil {
		return Plan{}, err
	}
	parts, err := s.partitionOffsets(ctx, req)
	if err != nil {
		return Plan{}, err
	}
	return Plan{
		Request:    req,
		GroupState: state,
		Members:    members,
		Moves:      planMoves(req.Mode, parts),
		CreatedAt:  s.now().UTC(),
	}, nil
}

// Apply commits plan after re-checking it against the live group. It returns
// ErrGroupActive while the group has members and ErrStalePlan if any commit
// moved since the plan. Every attempt with an actor and reason is audited,
// including refusals. Without both it returns ErrInvalid and records nothing,
// because an audit entry with no owner is not useful.
func (s *Service) Apply(ctx context.Context, plan Plan, actor, reason string) error {
	if actor == "" || reason == "" {
		return fmt.Errorf("%w: actor and reason are required", ErrInvalid)
	}
	err := s.apply(ctx, plan)
	e := audit.Event{
		Actor:   actor,
		Action:  "consumergroup.offsets.apply",
		Target:  plan.Request.Group + "/" + plan.Request.Topic,
		Reason:  reason,
		Outcome: audit.OutcomeApplied,
		Detail:  plan,
	}
	switch {
	case errors.Is(err, ErrGroupActive), errors.Is(err, ErrStalePlan), errors.Is(err, ErrEmptyPlan), errors.Is(err, ErrInvalid):
		e.Outcome, e.Error = audit.OutcomeRefused, err.Error()
	case err != nil:
		e.Outcome, e.Error = audit.OutcomeFailed, err.Error()
	}
	if auditErr := s.auditor.Record(ctx, e); auditErr != nil {
		return errors.Join(err, auditErr)
	}
	return err
}

func (s *Service) apply(ctx context.Context, plan Plan) error {
	req := plan.Request
	if err := req.validate(); err != nil {
		return err
	}
	if len(plan.Moves) == 0 {
		return ErrEmptyPlan
	}
	_, members, err := s.describe(ctx, req.Group)
	if err != nil {
		return err
	}
	if members > 0 {
		return fmt.Errorf("%w: %s has %d members", ErrGroupActive, req.Group, members)
	}

	live, err := s.admin.FetchOffsets(ctx, req.Group)
	if err != nil {
		return fmt.Errorf("consumergroup: fetch offsets: %w", err)
	}
	var stale []error
	offsets := kadm.Offsets{}
	for _, m := range plan.Moves {
		if now := committedAt(live, req.Topic, m.Partition); now != m.From {
			stale = append(stale, fmt.Errorf("%s-%d planned from %d, now %d", req.Topic, m.Partition, m.From, now))
		}
		offsets.Add(kadm.Offset{Topic: req.Topic, Partition: m.Partition, At: m.To, LeaderEpoch: -1})
	}
	if len(stale) > 0 {
		return fmt.Errorf("%w: %w", ErrStalePlan, errors.Join(stale...))
	}

	resp, err := s.admin.CommitOffsets(ctx, req.Group, offsets)
	if err != nil {
		return fmt.Errorf("consumergroup: commit: %w", err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("consumergroup: commit: %w", err)
	}
	return nil
}

func (s *Service) describe(ctx context.Context, group string) (state string, members int, err error) {
	described, err := s.admin.DescribeGroups(ctx, group)
	if err != nil {
		return "", 0, fmt.Errorf("consumergroup: describe %s: %w", group, err)
	}
	g, ok := described[group]
	if !ok {
		return "", 0, fmt.Errorf("consumergroup: group %s missing from describe response", group)
	}
	if g.Err != nil {
		return "", 0, fmt.Errorf("consumergroup: describe %s: %w", group, g.Err)
	}
	return g.State, len(g.Members), nil
}

func (s *Service) partitionOffsets(ctx context.Context, req Request) ([]partitionOffsets, error) {
	commits, err := s.admin.FetchOffsets(ctx, req.Group)
	if err != nil {
		return nil, fmt.Errorf("consumergroup: fetch offsets: %w", err)
	}
	starts, err := s.admin.ListStartOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("consumergroup: start offsets: %w", err)
	}
	ends, err := s.admin.ListEndOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("consumergroup: end offsets: %w", err)
	}
	var atTime kadm.ListedOffsets
	if req.Mode == ModeTimestamp {
		atTime, err = s.admin.ListOffsetsAfterMilli(ctx, req.Timestamp.UnixMilli(), req.Topic)
		if err != nil {
			return nil, fmt.Errorf("consumergroup: offsets at %s: %w", req.Timestamp, err)
		}
	}
	for _, l := range []kadm.ListedOffsets{starts, ends, atTime} {
		if err := l.Error(); err != nil {
			return nil, fmt.Errorf("consumergroup: list offsets %s: %w", req.Topic, err)
		}
	}

	var parts []partitionOffsets
	ends.Each(func(end kadm.ListedOffset) {
		if len(req.Partitions) > 0 && !slices.Contains(req.Partitions, end.Partition) {
			return
		}
		start, _ := starts.Lookup(req.Topic, end.Partition)
		at, _ := atTime.Lookup(req.Topic, end.Partition)
		parts = append(parts, partitionOffsets{
			partition: end.Partition,
			committed: committedAt(commits, req.Topic, end.Partition),
			start:     start.Offset,
			end:       end.Offset,
			atTime:    at.Offset,
		})
	})
	if len(req.Partitions) > 0 && len(parts) != len(req.Partitions) {
		return nil, fmt.Errorf("%w: %s does not have all of partitions %v", ErrInvalid, req.Topic, req.Partitions)
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("%w: topic %s has no partitions", ErrInvalid, req.Topic)
	}
	return parts, nil
}

func committedAt(commits kadm.OffsetResponses, topic string, partition int32) int64 {
	if o, ok := commits.Lookup(topic, partition); ok && o.Err == nil {
		return o.At
	}
	return -1
}
