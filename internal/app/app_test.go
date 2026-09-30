package app

import (
	"net/http"
	"testing"

	"go.uber.org/dig"

	"github.com/mohsanabbas/kafka-platform/internal/cluster"
	"github.com/mohsanabbas/kafka-platform/internal/config"
	"github.com/mohsanabbas/kafka-platform/internal/consumergroup"
	"github.com/mohsanabbas/kafka-platform/internal/cruisecontrol"
)

// TestGraphResolves catches a missing provider at test time instead of at the
// first labctl command or opsd request. DryRun skips the constructors.
func TestGraphResolves(t *testing.T) {
	a, err := newApp(dig.DryRun(true))
	if err != nil {
		t.Fatal(err)
	}
	err = a.Invoke(func(
		http.Handler,
		config.HTTP,
		config.Kafka,
		*cluster.Service,
		*consumergroup.Service,
		*cruisecontrol.Client,
	) {
	})
	if err != nil {
		t.Fatal(err)
	}
}
