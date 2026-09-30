package httpapi

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mohsanabbas/kafka-platform/internal/cluster"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
	"github.com/mohsanabbas/kafka-platform/internal/cruisecontrol"
)

type fakeGroups struct {
	applyErr    error
	actor       string
	reason      string
	plannedMode consumergroup.Mode
}

func (f *fakeGroups) Lag(context.Context, string) (consumergroup.Lag, error) {
	return consumergroup.Lag{}, nil
}

func (f *fakeGroups) Plan(_ context.Context, req consumergroup.Request) (consumergroup.Plan, error) {
	f.plannedMode = req.Mode
	return consumergroup.Plan{Request: req}, nil
}

func (f *fakeGroups) Apply(_ context.Context, _ consumergroup.Plan, actor, reason string) error {
	f.actor, f.reason = actor, reason
	return f.applyErr
}

type nopCluster struct{}

func (nopCluster) Health(context.Context) (cluster.Health, error) { return cluster.Health{}, nil }

type nopCruiseControl struct{}

func (nopCruiseControl) State(context.Context) (cruisecontrol.State, error) {
	return cruisecontrol.State{}, nil
}

func (nopCruiseControl) Proposals(context.Context) (cruisecontrol.Proposals, error) {
	return cruisecontrol.Proposals{}, nil
}

func (nopCruiseControl) Reviews(context.Context) ([]cruisecontrol.Review, error) { return nil, nil }

func newTestHandler(groups GroupOperator) http.Handler {
	return New(slog.New(slog.DiscardHandler), nopCluster{}, groups, nopCruiseControl{})
}

func TestOffsetsPlanUsesPathGroup(t *testing.T) {
	groups := &fakeGroups{}
	h := newTestHandler(groups)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/groups/payments-worker/offsets/plan",
		strings.NewReader(`{"topic":"orders","mode":"skip"}`))
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), `"group": "payments-worker"`) || groups.plannedMode != consumergroup.ModeSkip {
		t.Errorf("unexpected plan response %s", rec.Body)
	}
}

func TestOffsetsApplyStatus(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		applyErr error
		expected int
	}{
		{"applied", "/v1/groups/g/offsets/apply", nil, http.StatusOK},
		{"group active", "/v1/groups/g/offsets/apply", consumergroup.ErrGroupActive, http.StatusConflict},
		{"stale plan", "/v1/groups/g/offsets/apply", consumergroup.ErrStalePlan, http.StatusConflict},
		{"plan for other group", "/v1/groups/other/offsets/apply", nil, http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := &fakeGroups{applyErr: tt.applyErr}
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tt.path,
				strings.NewReader(`{"plan":{"request":{"group":"g","topic":"orders","mode":"skip"}},"reason":"INC-7"}`))
			req.Header.Set(ActorHeader, "alice")
			rec := httptest.NewRecorder()

			newTestHandler(groups).ServeHTTP(rec, req)

			if rec.Code != tt.expected {
				t.Fatalf("status = %d, expected %d, body %s", rec.Code, tt.expected, rec.Body)
			}
			if tt.expected == http.StatusOK && (groups.actor != "alice" || groups.reason != "INC-7") {
				t.Errorf("apply got actor %q reason %q", groups.actor, groups.reason)
			}
		})
	}
}

func TestUnknownFieldsRejected(t *testing.T) {
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/groups/g/offsets/plan", strings.NewReader(`{"topic":"orders","mode":"skip","force":true}`))
	rec := httptest.NewRecorder()

	newTestHandler(&fakeGroups{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, expected 400", rec.Code)
	}
}
