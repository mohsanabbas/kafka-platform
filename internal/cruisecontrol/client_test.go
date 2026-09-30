package cruisecontrol

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/config"
)

func newTestClient(t *testing.T, h http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	c := New(config.CruiseControl{URL: u})
	c.pollInterval = time.Millisecond
	return c
}

func TestProposalsWaitsForAsyncTask(t *testing.T) {
	calls := 0
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/kafkacruisecontrol/proposals" || r.URL.Query().Get("json") != "true" {
			t.Errorf("unexpected request %s", r.URL)
		}
		calls++
		if calls == 1 {
			w.Header().Set(taskHeader, "task-1")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"progress":[]}`))
			return
		}
		if got := r.Header.Get(taskHeader); got != "task-1" {
			t.Errorf("poll sent %s=%q, expected task-1", taskHeader, got)
		}
		_, _ = w.Write([]byte(`{"summary":{"numReplicaMovements":4,"numLeaderMovements":1,"dataToMoveMB":12},
			"goalSummary":[{"goal":"RackAwareGoal","status":"NO-ACTION"}]}`))
	})

	p, err := c.Proposals(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Errorf("calls = %d, expected 2", calls)
	}
	if !p.NeedsAction() || p.Summary.DataToMoveMB != 12 || len(p.Goals) != 1 {
		t.Errorf("unexpected proposals %+v", p)
	}
}

func TestErrorMessageSurfaces(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"errorMessage":"NotEnoughValidWindowsException: 0 windows"}`))
	})

	_, err := c.State(t.Context())
	if err == nil || !strings.Contains(err.Error(), "NotEnoughValidWindowsException") {
		t.Fatalf("State() = %v, expected cruise control error message", err)
	}
}

func TestNonJSONErrorBodyIsTruncated(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("<html>" + strings.Repeat("x", 10_000) + "</html>"))
	})

	_, err := c.State(t.Context())
	if err == nil {
		t.Fatal("State() succeeded, expected an error")
	}
	if msg := err.Error(); len(msg) > 1024 || !strings.HasSuffix(msg, "...") {
		t.Fatalf("State() error is %d bytes, expected a truncated message", len(msg))
	}
}
