// Command labctl is the break-glass CLI for the Kafka platform. It talks to
// Kafka and Cruise Control directly, with the same guardrails as opsd.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/mohsanabbas/kafka-platform/internal/app"
)

// errUnhealthy exits 3 so scripts can tell "cluster is degraded" from "labctl failed".
var errUnhealthy = errors.New("cluster is not healthy")

type command struct {
	summary string
	run     func(ctx context.Context, a *app.App, args []string) error
	subs    map[string]command
}

func commands() map[string]command {
	return map[string]command{
		"health": {summary: "brokers, racks, offline and under-replicated partitions", run: runHealth},
		"lag":    {summary: "per-partition lag for a consumer group", run: runLag},
		"offsets": {summary: "move committed offsets with plan and apply", subs: map[string]command{
			"plan":  {summary: "compute offset moves and write a plan file", run: runOffsetsPlan},
			"apply": {summary: "apply a reviewed plan file", run: runOffsetsApply},
		}},
		"cc": {summary: "read Cruise Control", subs: map[string]command{
			"state":     {summary: "monitor, executor, and analyzer state", run: runCCState},
			"proposals": {summary: "what a rebalance would move right now (dry run)", run: runCCProposals},
			"reviews":   {summary: "two-step verification review board", run: runCCReviews},
		}},
		"produce": {summary: "write lab records, optionally with a poison record", run: runProduce},
		"consume": {summary: "strict consumer that crashes on a poison record", run: runConsume},
	}
}

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	a, err := app.New()
	if err != nil {
		fmt.Fprintln(os.Stderr, "labctl:", err)
		return 1
	}
	defer a.Close()
	err = a.Decorate(func(*slog.Logger) *slog.Logger {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "labctl:", err)
		return 1
	}

	err = dispatch(ctx, a, "labctl", commands(), os.Args[1:])
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		return 2
	case errors.Is(err, errUnhealthy):
		fmt.Fprintln(os.Stderr, "labctl:", err)
		return 3
	default:
		fmt.Fprintln(os.Stderr, "labctl:", err)
		return 1
	}
}

var errUsage = errors.New("usage")

func dispatch(ctx context.Context, a *app.App, path string, table map[string]command, args []string) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		usage(path, table)
		return errUsage
	}
	cmd, ok := table[args[0]]
	if !ok {
		fmt.Fprintf(os.Stderr, "%s: unknown command %q\n\n", path, args[0])
		usage(path, table)
		return errUsage
	}
	if cmd.subs != nil {
		return dispatch(ctx, a, path+" "+args[0], cmd.subs, args[1:])
	}
	return cmd.run(ctx, a, args[1:])
}

func usage(path string, table map[string]command) {
	fmt.Fprintf(os.Stderr, "usage: %s <command> [flags]\n\ncommands:\n", path)
	for _, name := range slices.Sorted(maps.Keys(table)) {
		fmt.Fprintf(os.Stderr, "  %-10s %s\n", name, table[name].summary)
	}
	if !strings.Contains(path, " ") {
		fmt.Fprintf(os.Stderr, "\nrun '%s <command> -h' for flags\n", path)
	}
}
