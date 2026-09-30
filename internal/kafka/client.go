// Package kafka builds franz-go clients from platform config.
//
// It is the only place that knows how TLS and SASL map to client options, so
// the lab (PLAINTEXT) and MSK (TLS with SCRAM or IAM) differ only in env.
package kafka

import (
	"context"
	"crypto/tls"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/mohsanabbas/kafka-platform/internal/config"
)

// NewClient returns a client for cfg. extra options are applied last.
func NewClient(cfg config.Kafka, extra ...kgo.Opt) (*kgo.Client, error) {
	opts, err := Options(cfg)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return cl, nil
}

// Options maps cfg to client options.
func Options(cfg config.Kafka) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID("kafka-platform"),
	}
	if cfg.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	m, err := mechanism(cfg.SASL)
	if err != nil {
		return nil, err
	}
	if m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	return opts, nil
}

func mechanism(s config.SASL) (sasl.Mechanism, error) {
	switch s.Mechanism {
	case config.MechanismNone:
		return nil, nil
	case config.MechanismSCRAMSHA256:
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
	case config.MechanismSCRAMSHA512:
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
	case config.MechanismAWSMSKIAM:
		auth := aws.Auth{
			AccessKey:    s.AWSAccessKeyID,
			SecretKey:    s.AWSSecretAccessKey,
			SessionToken: s.AWSSessionToken,
		}
		return aws.ManagedStreamingIAM(func(context.Context) (aws.Auth, error) { return auth, nil }), nil
	default:
		return nil, fmt.Errorf("kafka: unsupported sasl mechanism %q", s.Mechanism)
	}
}
