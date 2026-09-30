package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mohsanabbas/kafka-platform/internal/app"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
)

func runOffsetsPlan(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("offsets plan", flag.ContinueOnError)
	group := fs.String("group", "payments-worker", "consumer group")
	topic := fs.String("topic", "orders", "topic")
	to := fs.String("to", "skip", "skip, latest, earliest, or timestamp")
	at := fs.String("at", "", "for --to timestamp: RFC 3339 time, or a duration ago such as 30m")
	partitions := fs.String("partitions", "", "comma-separated partitions, default all")
	out := fs.String("out", "offsets.plan.json", "plan file to write")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}

	req := consumergroup.Request{Group: *group, Topic: *topic, Mode: consumergroup.Mode(*to)}
	var err error
	if *at != "" {
		if req.Timestamp, err = parseAt(*at, time.Now()); err != nil {
			return err
		}
	}
	if req.Partitions, err = parsePartitions(*partitions); err != nil {
		return err
	}

	return a.Invoke(func(svc *consumergroup.Service) error {
		plan, err := svc.Plan(ctx, req)
		if err != nil {
			return err
		}
		fmt.Printf("group %s state %s members %d, %s on %s\n\n", *group, plan.GroupState, plan.Members, req.Mode, *topic)
		if len(plan.Moves) == 0 {
			fmt.Println("no changes: every selected partition is already where this mode would put it")
			return nil
		}
		tw := newTable()
		fmt.Fprintln(tw, "PARTITION\tFROM\tTO\tEND\tLAG AFTER")
		for _, m := range plan.Moves {
			fmt.Fprintf(tw, "%s-%d\t%d\t%d\t%d\t%d\n", *topic, m.Partition, m.From, m.To, m.End, m.End-m.To)
		}
		_ = tw.Flush()

		data, err := json.MarshalIndent(plan, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0o600); err != nil {
			return err
		}
		fmt.Printf("\nplan written to %s\n", *out)
		if plan.Members > 0 {
			fmt.Printf("the group still has %d members: stop or scale the app to zero before apply\n", plan.Members)
		}
		fmt.Printf("apply with: labctl offsets apply --plan %s --reason \"INC-123 why\"\n", *out)
		return nil
	})
}

func runOffsetsApply(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("offsets apply", flag.ContinueOnError)
	path := fs.String("plan", "offsets.plan.json", "plan file from labctl offsets plan")
	reason := fs.String("reason", "", "why, for the audit trail (ticket id)")
	actor := fs.String("actor", os.Getenv("USER"), "who, for the audit trail")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	data, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	var plan consumergroup.Plan
	if err := json.Unmarshal(data, &plan); err != nil {
		return fmt.Errorf("read plan %s: %w", *path, err)
	}
	return a.Invoke(func(svc *consumergroup.Service) error {
		if err := svc.Apply(ctx, plan, *actor, *reason); err != nil {
			return err
		}
		fmt.Printf("applied %d moves to %s on %s. start the app and watch: labctl lag --group %s\n",
			len(plan.Moves), plan.Request.Group, plan.Request.Topic, plan.Request.Group)
		return nil
	})
}

// parseAt accepts an RFC 3339 time or a duration meaning that long ago.
func parseAt(s string, now time.Time) (time.Time, error) {
	if d, err := time.ParseDuration(strings.TrimPrefix(s, "-")); err == nil {
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("--at %q: want RFC 3339 or a duration like 30m", s)
	}
	return t, nil
}

func parsePartitions(s string) ([]int32, error) {
	if s == "" {
		return nil, nil
	}
	var out []int32
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(f), 10, 32)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("--partitions %q: want comma-separated partition numbers", s)
		}
		out = append(out, int32(n))
	}
	return out, nil
}
