package main

import (
	"context"
	"flag"
	"os"

	"github.com/mohsanabbas/kafka-platform/internal/app"
	"github.com/mohsanabbas/kafka-platform/internal/config"
	"github.com/mohsanabbas/kafka-platform/internal/workload"
)

func runProduce(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("produce", flag.ContinueOnError)
	topic := fs.String("topic", "orders", "topic")
	count := fs.Int("count", 20, "records to write")
	poisonAt := fs.Int("poison-at", 11, "1-based index of the poison record, 0 for none")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	return a.Invoke(func(cfg config.Kafka) error {
		return workload.Produce(ctx, cfg, os.Stdout, *topic, *count, *poisonAt)
	})
}

func runConsume(ctx context.Context, a *app.App, args []string) error {
	fs := flag.NewFlagSet("consume", flag.ContinueOnError)
	group := fs.String("group", "payments-worker", "consumer group")
	topic := fs.String("topic", "orders", "topic")
	if err := fs.Parse(args); err != nil {
		return errUsage
	}
	return a.Invoke(func(cfg config.Kafka) error {
		return workload.Consume(ctx, cfg, os.Stdout, *group, *topic)
	})
}
