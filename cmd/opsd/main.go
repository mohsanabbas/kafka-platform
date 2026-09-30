// Command opsd is the platform operations API: cluster health, consumer lag,
// audited offset resets, and Cruise Control summaries over HTTP.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/app"
	"github.com/mohsanabbas/kafka-platform/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "opsd:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New()
	if err != nil {
		return err
	}
	defer a.Close()

	return a.Invoke(func(handler http.Handler, cfg config.HTTP, logger *slog.Logger) error {
		srv := &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
			IdleTimeout:       time.Minute,
		}
		errc := make(chan error, 1)
		go func() {
			logger.Info("opsd listening", slog.String("addr", cfg.Addr))
			errc <- srv.ListenAndServe()
		}()

		select {
		case err := <-errc:
			if errors.Is(err, syscall.EADDRINUSE) {
				return fmt.Errorf("%s is already in use, often by the opsd container from make up. Set OPSD_ADDR, for example OPSD_ADDR=127.0.0.1:8091: %w", cfg.Addr, err)
			}
			return err
		case <-ctx.Done():
		}
		logger.Info("opsd shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
}
