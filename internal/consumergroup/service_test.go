package consumergroup

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/mohsanabbas/kafka-platform/internal/audit"
)

type fakeAdmin struct {
	members   int
	commits   map[int32]int64
	start     map[int32]int64
	end       map[int32]int64
	committed kadm.Offsets
}

func (f *fakeAdmin) Lag(context.Context, ...string) (kadm.DescribedGroupLags, error) {
	return nil, errors.New("not used")
}

func (f *fakeAdmin) DescribeGroups(_ context.Context, groups ...string) (kadm.DescribedGroups, error) {
	g := kadm.DescribedGroup{Group: groups[0], State: "Empty"}
	if f.members > 0 {
		g.State = "Stable"
		g.Members = make([]kadm.DescribedGroupMember, f.members)
	}
	return kadm.DescribedGroups{groups[0]: g}, nil
}

func (f *fakeAdmin) FetchOffsets(context.Context, string) (kadm.OffsetResponses, error) {
	out := kadm.OffsetResponses{"orders": {}}
	for p, at := range f.commits {
		out["orders"][p] = kadm.OffsetResponse{Offset: kadm.Offset{Topic: "orders", Partition: p, At: at}}
	}
	return out, nil
}

func listed(m map[int32]int64) kadm.ListedOffsets {
	out := kadm.ListedOffsets{"orders": {}}
	for p, o := range m {
		out["orders"][p] = kadm.ListedOffset{Topic: "orders", Partition: p, Offset: o}
	}
	return out
}

func (f *fakeAdmin) ListStartOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return listed(f.start), nil
}

func (f *fakeAdmin) ListEndOffsets(context.Context, ...string) (kadm.ListedOffsets, error) {
	return listed(f.end), nil
}

func (f *fakeAdmin) ListOffsetsAfterMilli(context.Context, int64, ...string) (kadm.ListedOffsets, error) {
	return listed(f.start), nil
}

func (f *fakeAdmin) CommitOffsets(_ context.Context, _ string, os kadm.Offsets) (kadm.OffsetResponses, error) {
	f.committed = os
	for _, ps := range os {
		for p, o := range ps {
			f.commits[p] = o.At
		}
	}
	return kadm.OffsetResponses{}, nil
}

type fakeAuditor struct{ events []audit.Event }

func (f *fakeAuditor) Record(_ context.Context, e audit.Event) error {
	f.events = append(f.events, e)
	return nil
}

func poisonedGroup() *fakeAdmin {
	return &fakeAdmin{
		commits: map[int32]int64{0: 10, 1: 20},
		start:   map[int32]int64{0: 0, 1: 0},
		end:     map[int32]int64{0: 20, 1: 20},
	}
}

var skipOrders = Request{Group: "payments-worker", Topic: "orders", Mode: ModeSkip}

func TestApplySkipsPoisonRecord(t *testing.T) {
	admin, auditor := poisonedGroup(), &fakeAuditor{}
	svc := New(admin, auditor)

	plan, err := svc.Plan(t.Context(), skipOrders)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Apply(t.Context(), plan, "alice", "INC-42 poison record"); err != nil {
		t.Fatal(err)
	}

	if got := admin.commits[0]; got != 11 {
		t.Errorf("partition 0 committed at %d, expected 11", got)
	}
	if _, ok := admin.committed.Lookup("orders", 1); ok {
		t.Error("partition 1 was caught up and must not be committed")
	}
	if len(auditor.events) != 1 || auditor.events[0].Outcome != audit.OutcomeApplied {
		t.Errorf("audit events = %+v, expected one applied", auditor.events)
	}
}

func TestApplyRefusals(t *testing.T) {
	tests := []struct {
		name     string
		change   func(*fakeAdmin, *Plan)
		expected error
	}{
		{"app still running", func(a *fakeAdmin, _ *Plan) { a.members = 2 }, ErrGroupActive},
		{"commit moved after plan", func(a *fakeAdmin, _ *Plan) { a.commits[0] = 12 }, ErrStalePlan},
		{"nothing to move", func(_ *fakeAdmin, p *Plan) { p.Moves = nil }, ErrEmptyPlan},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			admin, auditor := poisonedGroup(), &fakeAuditor{}
			svc := New(admin, auditor)
			plan, err := svc.Plan(t.Context(), skipOrders)
			if err != nil {
				t.Fatal(err)
			}
			tt.change(admin, &plan)

			err = svc.Apply(t.Context(), plan, "alice", "drill")

			if !errors.Is(err, tt.expected) {
				t.Fatalf("Apply() = %v, expected %v", err, tt.expected)
			}
			if admin.committed != nil {
				t.Error("offsets were committed despite the refusal")
			}
			if len(auditor.events) != 1 || auditor.events[0].Outcome != audit.OutcomeRefused {
				t.Errorf("audit events = %+v, expected one refused", auditor.events)
			}
		})
	}
}

func TestApplyNeedsActorAndReason(t *testing.T) {
	svc := New(poisonedGroup(), &fakeAuditor{})
	plan, err := svc.Plan(t.Context(), skipOrders)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Apply(t.Context(), plan, "alice", ""); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Apply() without reason = %v, expected ErrInvalid", err)
	}
}
