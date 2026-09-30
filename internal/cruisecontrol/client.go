// Package cruisecontrol is a read-only client for the Cruise Control REST API.
//
// Writes (rebalance, add_broker, remove_broker) stay in the Cruise Control UI or
// curl, where two-step verification puts them on the review board first.
package cruisecontrol

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/config"
)

// Long operations answer 202 with a User-Task-ID. Asking again with that
// header returns progress until the result is ready.
const taskHeader = "User-Task-ID"

// Client reads Cruise Control. It is safe for concurrent use.
type Client struct {
	base         *url.URL
	http         *http.Client
	pollInterval time.Duration
}

// New returns a Client for the server at cfg.URL. Each HTTP round trip times
// out after 30 seconds. A long-running task is polled until ctx ends.
func New(cfg config.CruiseControl) *Client {
	return &Client{
		base:         cfg.URL.JoinPath("kafkacruisecontrol"),
		http:         &http.Client{Timeout: 30 * time.Second},
		pollInterval: time.Second,
	}
}

// State is the subset of GET /state that answers "can Cruise Control make a proposal yet?".
type State struct {
	Monitor  MonitorState  `json:"MonitorState"`
	Executor ExecutorState `json:"ExecutorState"`
	Analyzer AnalyzerState `json:"AnalyzerState"`
}

// MonitorState is load-sampling progress. Proposals need enough valid windows and partition coverage.
type MonitorState struct {
	State                 string  `json:"state"`
	NumMonitoredWindows   int     `json:"numMonitoredWindows"`
	MonitoringCoveragePct float64 `json:"monitoringCoveragePct"`
	NumValidPartitions    int     `json:"numValidPartitions"`
	NumTotalPartitions    int     `json:"numTotalPartitions"`
	NumFlawedPartitions   int     `json:"numFlawedPartitions"`
}

// ExecutorState is NO_TASK_IN_PROGRESS unless a reassignment is running.
type ExecutorState struct {
	State string `json:"state"`
}

// AnalyzerState reports whether a cached proposal exists and which goals it satisfies.
type AnalyzerState struct {
	IsProposalReady bool     `json:"isProposalReady"`
	ReadyGoals      []string `json:"readyGoals"`
}

// Proposals is Cruise Control's cached optimization plan: what a rebalance
// would do right now. Reading it moves nothing and needs no review.
type Proposals struct {
	Summary Summary      `json:"summary"`
	Goals   []GoalResult `json:"goalSummary"`
}

// NeedsAction reports whether a rebalance would move any replica or leader.
func (p Proposals) NeedsAction() bool {
	return p.Summary.NumReplicaMovements > 0 || p.Summary.NumLeaderMovements > 0
}

// Summary is the proposal's cost and effect. Balancedness is 0 to 100, higher is more even.
type Summary struct {
	NumReplicaMovements           int      `json:"numReplicaMovements"`
	NumLeaderMovements            int      `json:"numLeaderMovements"`
	DataToMoveMB                  int64    `json:"dataToMoveMB"`
	MonitoredPartitionsPercentage float64  `json:"monitoredPartitionsPercentage"`
	RecentWindows                 int      `json:"recentWindows"`
	ProvisionStatus               string   `json:"provisionStatus"`
	ProvisionRecommendation       string   `json:"provisionRecommendation"`
	BalancednessBefore            float64  `json:"onDemandBalancednessScoreBefore"`
	BalancednessAfter             float64  `json:"onDemandBalancednessScoreAfter"`
	ExcludedTopics                []string `json:"excludedTopics"`
}

// GoalResult is one goal's outcome in the proposal: FIXED, NO-ACTION, or VIOLATED.
type GoalResult struct {
	Goal   string `json:"goal"`
	Status string `json:"status"`
}

// Review is one request on the two-step verification board.
type Review struct {
	ID          int    `json:"Id"`
	Status      string `json:"Status"`
	Endpoint    string `json:"EndpointWithParams"`
	Reason      string `json:"Reason"`
	Submitter   string `json:"SubmitterAddress"`
	SubmittedMs int64  `json:"SubmissionTimeMs"`
}

// State returns monitor, executor, and analyzer state.
func (c *Client) State(ctx context.Context) (State, error) {
	var s State
	err := c.get(ctx, "state", &s)
	return s, err
}

// Proposals returns the cached proposal. It is a dry run: nothing moves and no review is needed.
func (c *Client) Proposals(ctx context.Context) (Proposals, error) {
	var p Proposals
	err := c.get(ctx, "proposals", &p)
	return p, err
}

// Reviews returns every request on the two-step verification board, in any status.
func (c *Client) Reviews(ctx context.Context) ([]Review, error) {
	var board struct {
		Requests []Review `json:"RequestInfo"`
	}
	if err := c.get(ctx, "review_board", &board); err != nil {
		return nil, err
	}
	return board.Requests, nil
}

func (c *Client) get(ctx context.Context, endpoint string, out any) error {
	u := c.base.JoinPath(endpoint)
	u.RawQuery = url.Values{"json": {"true"}}.Encode()

	var task string
	for {
		status, header, body, err := c.do(ctx, u.String(), task)
		if err != nil {
			return err
		}
		switch status {
		case http.StatusOK:
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("cruisecontrol: decode %s: %w", endpoint, err)
			}
			return nil
		case http.StatusAccepted:
			task = header.Get(taskHeader)
			if task == "" {
				return fmt.Errorf("cruisecontrol: %s returned 202 without %s", endpoint, taskHeader)
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("cruisecontrol: waiting for %s task %s: %w", endpoint, task, ctx.Err())
			case <-time.After(c.pollInterval):
			}
		default:
			return fmt.Errorf("cruisecontrol: %s: http %d: %s", endpoint, status, errorMessage(body))
		}
	}
}

func (c *Client) do(ctx context.Context, target, task string) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("cruisecontrol: %w", err)
	}
	if task != "" {
		req.Header.Set(taskHeader, task)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("cruisecontrol: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		return 0, nil, nil, fmt.Errorf("cruisecontrol: read body: %w", err)
	}
	return resp.StatusCode, resp.Header, body, nil
}

// errorMessage extracts Cruise Control's errorMessage. Anything else, such as a
// proxy's HTML error page, is truncated so it cannot flood logs and API responses.
func errorMessage(body []byte) string {
	var e struct {
		ErrorMessage string `json:"errorMessage"`
	}
	if json.Unmarshal(body, &e) == nil && e.ErrorMessage != "" {
		return e.ErrorMessage
	}
	const limit = 512
	if len(body) > limit {
		return string(body[:limit]) + "..."
	}
	return string(body)
}
