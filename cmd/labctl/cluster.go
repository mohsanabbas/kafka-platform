package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/mohsanabbas/kafka-platform/internal/app"
	"github.com/mohsanabbas/kafka-platform/internal/cluster"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
)

func runHealth(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	return a.Invoke(func(svc *cluster.Service) error {
		h, err := svc.Health(ctx)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(h)
		}
		tw := newTable()
		fmt.Fprintf(tw, "cluster\t%s\n", h.ClusterID)
		fmt.Fprintf(tw, "topics\t%d\npartitions\t%d\n\n", h.Topics, h.Partitions)
		fmt.Fprintln(tw, "BROKER\tHOST\tRACK")
		for _, b := range h.Brokers {
			fmt.Fprintf(tw, "%d\t%s:%d\t%s\n", b.ID, b.Host, b.Port, b.Rack)
		}
		_ = tw.Flush()

		printPartitions("OFFLINE, no leader, reads and writes fail", h.Offline)
		printPartitions("UNDER MIN ISR, acks=all producers get NOT_ENOUGH_REPLICAS", h.UnderMinISR)
		printPartitions("UNDER-REPLICATED, still writable, one more failure from trouble", h.UnderReplicated)
		if !h.IsHealthy() {
			return errUnhealthy
		}
		fmt.Println("\nhealthy: every partition has a leader and a full ISR")
		return nil
	})
}

// printPartitions summarizes per topic. One broker down touches every topic,
// and a row per partition buries the one topic that matters.
func printPartitions(title string, parts []cluster.Partition) {
	if len(parts) == 0 {
		return
	}
	fmt.Printf("\n%s: %d partitions\n", title, len(parts))
	tw := newTable()
	fmt.Fprintln(tw, "TOPIC\tPARTITIONS\tMIN ISR\tEXAMPLE")
	for i := 0; i < len(parts); {
		j := i
		for j < len(parts) && parts[j].Topic == parts[i].Topic {
			j++
		}
		p := parts[i]
		example := fmt.Sprintf("p%d leader %d replicas %v isr %v %s", p.Partition, p.Leader, p.Replicas, p.ISR, p.Error)
		fmt.Fprintf(tw, "%s\t%d\t%d\t%s\n", p.Topic, j-i, p.MinISR, example)
		i = j
	}
	_ = tw.Flush()
}

func runLag(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("lag", flag.ContinueOnError)
	group := fs.String("group", "payments-worker", "consumer group")
	asJSON := fs.Bool("json", false, "print JSON")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	return a.Invoke(func(svc *consumergroup.Service) error {
		lag, err := svc.Lag(ctx, *group)
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(lag)
		}
		fmt.Printf("group %s state %s members %d total lag %d\n\n", lag.Group, lag.State, lag.Members, lag.Total)
		tw := newTable()
		fmt.Fprintln(tw, "PARTITION\tCOMMITTED\tEND\tLAG\tMEMBER")
		for _, p := range lag.Partitions {
			fmt.Fprintf(tw, "%s-%d\t%d\t%d\t%d\t%s\n", p.Topic, p.Partition, p.Committed, p.End, p.Lag, p.Member)
		}
		return tw.Flush()
	})
}

func newTable() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
