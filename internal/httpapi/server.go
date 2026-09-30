// Package httpapi exposes platform operations over HTTP for opsd.
//
// Handlers are thin: decode, call one service method, encode. Guardrails live
// in the services so the CLI and the API enforce the same rules.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/cluster"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
	"github.com/mohsanabbas/kafka-platform/internal/cruisecontrol"
)

// ActorHeader names the operator on write requests. In production an auth
// proxy sets it from the SSO identity and strips any client-supplied value.
const ActorHeader = "X-Actor"

// HealthChecker is satisfied by *cluster.Service.
type HealthChecker interface {
	Health(ctx context.Context) (cluster.Health, error)
}

// GroupOperator is satisfied by *consumergroup.Service.
type GroupOperator interface {
	Lag(ctx context.Context, group string) (consumergroup.Lag, error)
	Plan(ctx context.Context, req consumergroup.Request) (consumergroup.Plan, error)
	Apply(ctx context.Context, plan consumergroup.Plan, actor, reason string) error
}

// CruiseControl is satisfied by *cruisecontrol.Client.
type CruiseControl interface {
	State(ctx context.Context) (cruisecontrol.State, error)
	Proposals(ctx context.Context) (cruisecontrol.Proposals, error)
	Reviews(ctx context.Context) ([]cruisecontrol.Review, error)
}

type server struct {
	logger  *slog.Logger
	cluster HealthChecker
	groups  GroupOperator
	cc      CruiseControl
}

// New returns the opsd handler. Errors map to status codes: 400 for a bad
// request or empty plan, 409 for an active group or stale plan, 504 when a
// dependency times out, and 502 for other dependency failures. Every error body
// is {"error": "..."}.
func New(logger *slog.Logger, health HealthChecker, groups GroupOperator, cc CruiseControl) http.Handler {
	s := &server{logger: logger, cluster: health, groups: groups, cc: cc}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("GET /v1/cluster/health", s.clusterHealth)
	mux.HandleFunc("GET /v1/groups/{group}/lag", s.groupLag)
	mux.HandleFunc("POST /v1/groups/{group}/offsets/plan", s.offsetsPlan)
	mux.HandleFunc("POST /v1/groups/{group}/offsets/apply", s.offsetsApply)
	mux.HandleFunc("GET /v1/cruise-control/state", s.ccState)
	mux.HandleFunc("GET /v1/cruise-control/proposals", s.ccProposals)
	mux.HandleFunc("GET /v1/cruise-control/reviews", s.ccReviews)
	return s.logRequests(mux)
}

func (s *server) clusterHealth(w http.ResponseWriter, r *http.Request) {
	h, err := s.cluster.Health(r.Context())
	s.respond(w, r, h, err)
}

func (s *server) groupLag(w http.ResponseWriter, r *http.Request) {
	lag, err := s.groups.Lag(r.Context(), r.PathValue("group"))
	s.respond(w, r, lag, err)
}

func (s *server) offsetsPlan(w http.ResponseWriter, r *http.Request) {
	var req consumergroup.Request
	if err := decode(w, r, &req); err != nil {
		s.respond(w, r, nil, err)
		return
	}
	req.Group = r.PathValue("group")
	plan, err := s.groups.Plan(r.Context(), req)
	s.respond(w, r, plan, err)
}

type applyRequest struct {
	Plan   consumergroup.Plan `json:"plan"`
	Reason string             `json:"reason"`
}

func (s *server) offsetsApply(w http.ResponseWriter, r *http.Request) {
	var req applyRequest
	if err := decode(w, r, &req); err != nil {
		s.respond(w, r, nil, err)
		return
	}
	if group := r.PathValue("group"); req.Plan.Request.Group != group {
		s.respond(w, r, nil, fmt.Errorf("%w: plan is for group %q, not %q", consumergroup.ErrInvalid, req.Plan.Request.Group, group))
		return
	}
	err := s.groups.Apply(r.Context(), req.Plan, r.Header.Get(ActorHeader), req.Reason)
	s.respond(w, r, map[string]any{"applied": err == nil, "moves": req.Plan.Moves}, err)
}

func (s *server) ccState(w http.ResponseWriter, r *http.Request) {
	st, err := s.cc.State(r.Context())
	s.respond(w, r, st, err)
}

func (s *server) ccProposals(w http.ResponseWriter, r *http.Request) {
	p, err := s.cc.Proposals(r.Context())
	s.respond(w, r, p, err)
}

func (s *server) ccReviews(w http.ResponseWriter, r *http.Request) {
	rv, err := s.cc.Reviews(r.Context())
	s.respond(w, r, rv, err)
}

func decode(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%w: body: %w", consumergroup.ErrInvalid, err)
	}
	return nil
}

func (s *server) respond(w http.ResponseWriter, r *http.Request, v any, err error) {
	status := http.StatusOK
	if err != nil {
		status = statusFor(err)
		if status >= http.StatusInternalServerError {
			s.logger.ErrorContext(r.Context(), "request failed", slog.String("path", r.URL.Path), slog.Any("error", err))
		}
		v = map[string]string{"error": err.Error()}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, consumergroup.ErrInvalid), errors.Is(err, consumergroup.ErrEmptyPlan):
		return http.StatusBadRequest
	case errors.Is(err, consumergroup.ErrGroupActive), errors.Is(err, consumergroup.ErrStalePlan):
		return http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer for Flush and deadlines.
func (r *statusRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (s *server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" {
			return
		}
		s.logger.InfoContext(r.Context(), "http",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", rec.status),
			slog.Duration("duration", time.Since(start)),
		)
	})
}
