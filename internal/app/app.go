// Package app is the composition root shared by labctl and opsd.
//
// It is the only package that knows about dig. Services take their
// dependencies as constructor arguments and never see the container.
package app

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/dig"

	"github.com/mohsanabbas/kafka-platform/internal/audit"
	"github.com/mohsanabbas/kafka-platform/internal/cluster"
	"github.com/mohsanabbas/kafka-platform/internal/config"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
	"github.com/mohsanabbas/kafka-platform/internal/cruisecontrol"
	"github.com/mohsanabbas/kafka-platform/internal/httpapi"
	"github.com/mohsanabbas/kafka-platform/internal/kafka"
)

// App owns the container and the cleanup of whatever it built.
type App struct {
	c       *dig.Container
	closers *closers
}

// New registers every provider. Nothing is built or dialed until Invoke asks
// for it, so config errors surface on the first Invoke.
func New() (*App, error) {
	return newApp()
}

func newApp(opts ...dig.Option) (*App, error) {
	a := &App{c: dig.New(opts...), closers: &closers{}}
	providers := []struct {
		constructor any
		opts        []dig.ProvideOption
	}{
		{constructor: config.Load},
		{constructor: splitConfig},
		{constructor: newLogger},
		{constructor: a.newKafkaClient},
		{constructor: kadm.NewClient, opts: []dig.ProvideOption{dig.As(new(cluster.Admin), new(consumergroup.Admin))}},
		{constructor: newAuditLog, opts: []dig.ProvideOption{dig.As(new(consumergroup.Auditor))}},
		{constructor: cluster.New},
		{constructor: consumergroup.New},
		{constructor: cruisecontrol.New},
		{constructor: newHandler},
	}
	for _, p := range providers {
		if err := a.c.Provide(p.constructor, p.opts...); err != nil {
			return nil, fmt.Errorf("app: provide: %w", err)
		}
	}
	return a, nil
}

// Invoke runs fn with its arguments resolved from the container.
func (a *App) Invoke(fn any) error {
	return a.c.Invoke(fn)
}

// Decorate replaces a provided value, for example a CLI-friendly logger.
func (a *App) Decorate(fn any) error {
	return a.c.Decorate(fn)
}

// Close releases everything the container built, newest first.
func (a *App) Close() {
	a.closers.close()
}

type configOut struct {
	dig.Out

	Kafka         config.Kafka
	CruiseControl config.CruiseControl
	HTTP          config.HTTP
}

func splitConfig(cfg config.Config) configOut {
	return configOut{
		Kafka:         cfg.Kafka,
		CruiseControl: cfg.CruiseControl,
		HTTP:          cfg.HTTP,
	}
}

func newLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, nil))
}

func (a *App) newKafkaClient(cfg config.Kafka) (*kgo.Client, error) {
	cl, err := kafka.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	a.closers.add(cl.Close)
	return cl, nil
}

func newAuditLog(logger *slog.Logger, producer *kgo.Client) *audit.Log {
	return audit.New(logger, producer, audit.DefaultTopic)
}

func newHandler(logger *slog.Logger, health *cluster.Service, groups *consumergroup.Service, cc *cruisecontrol.Client) http.Handler {
	return httpapi.New(logger, health, groups, cc)
}

type closers struct {
	mu  sync.Mutex
	fns []func()
}

func (c *closers) add(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fns = append(c.fns, fn)
}

func (c *closers) close() {
	c.mu.Lock()
	fns := slices.Clone(c.fns)
	c.fns = nil
	c.mu.Unlock()
	for _, fn := range slices.Backward(fns) {
		fn()
	}
}
