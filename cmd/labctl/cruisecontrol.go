package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/app"
	"github.com/mohsanabbas/kafka-platform/internal/cruisecontrol"
)

func runCCState(ctx context.Context, a *app.App, args []string) error {
	asJSON, err := parseJSONFlag("cc state", args)
	if err != nil {
		return err
	}
	return a.Invoke(func(cc *cruisecontrol.Client) error {
		st, err := cc.State(ctx)
		if err != nil {
			return err
		}
		if asJSON {
			return printJSON(st)
		}
		m := st.Monitor
		tw := newTable()
		fmt.Fprintf(tw, "monitor\t%s\n", m.State)
		fmt.Fprintf(tw, "windows\t%d\n", m.NumMonitoredWindows)
		fmt.Fprintf(tw, "coverage\t%.1f%% (%d/%d partitions valid, %d flawed)\n",
			m.MonitoringCoveragePct, m.NumValidPartitions, m.NumTotalPartitions, m.NumFlawedPartitions)
		fmt.Fprintf(tw, "executor\t%s\n", st.Executor.State)
		fmt.Fprintf(tw, "proposal ready\t%t\n", st.Analyzer.IsProposalReady)
		return tw.Flush()
	})
}

func runCCProposals(ctx context.Context, a *app.App, args []string) error {
	asJSON, err := parseJSONFlag("cc proposals", args)
	if err != nil {
		return err
	}
	return a.Invoke(func(cc *cruisecontrol.Client) error {
		p, err := cc.Proposals(ctx)
		if err != nil {
			return err
		}
		if asJSON {
			return printJSON(p)
		}
		s := p.Summary
		tw := newTable()
		fmt.Fprintf(tw, "replica moves\t%d\n", s.NumReplicaMovements)
		fmt.Fprintf(tw, "leader moves\t%d\n", s.NumLeaderMovements)
		fmt.Fprintf(tw, "data to move\t%d MB\n", s.DataToMoveMB)
		fmt.Fprintf(tw, "balancedness\t%.1f -> %.1f\n", s.BalancednessBefore, s.BalancednessAfter)
		fmt.Fprintf(tw, "provision\t%s %s\n", s.ProvisionStatus, s.ProvisionRecommendation)
		fmt.Fprintf(tw, "monitored\t%.1f%% over %d windows\n\n", s.MonitoredPartitionsPercentage, s.RecentWindows)
		fmt.Fprintln(tw, "GOAL\tSTATUS")
		for _, g := range p.Goals {
			fmt.Fprintf(tw, "%s\t%s\n", g.Goal, g.Status)
		}
		_ = tw.Flush()
		if p.NeedsAction() {
			fmt.Println("\nthe cluster is out of balance. submit a rebalance from the Cruise Control UI, then approve it on the review board")
		} else {
			fmt.Println("\nbalanced: a rebalance would move nothing")
		}
		return nil
	})
}

func runCCReviews(ctx context.Context, a *app.App, args []string) error {
	asJSON, err := parseJSONFlag("cc reviews", args)
	if err != nil {
		return err
	}
	return a.Invoke(func(cc *cruisecontrol.Client) error {
		reviews, err := cc.Reviews(ctx)
		if err != nil {
			return err
		}
		if asJSON {
			return printJSON(reviews)
		}
		if len(reviews) == 0 {
			fmt.Println("review board is empty")
			return nil
		}
		tw := newTable()
		fmt.Fprintln(tw, "ID\tSTATUS\tSUBMITTED\tENDPOINT\tREASON")
		for _, r := range reviews {
			submitted := time.UnixMilli(r.SubmittedMs).Format(time.DateTime)
			reason, _, _ := strings.Cut(r.Reason, " (Client:")
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n", r.ID, r.Status, submitted, r.Endpoint, reason)
		}
		return tw.Flush()
	})
}

func parseJSONFlag(name string, args []string) (bool, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return false, errUsage
	}
	return *asJSON, nil
}
