// Package config reads platform settings from the process environment.
//
// At work Vault agent renders these variables. The lab sets them in compose or
// falls back to the localhost defaults, so the same binary runs against both.
package config

import (
	"cmp"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// Mechanism is a Kafka SASL mechanism.
type Mechanism string

const (
	MechanismNone        Mechanism = ""
	MechanismSCRAMSHA256 Mechanism = "SCRAM-SHA-256"
	MechanismSCRAMSHA512 Mechanism = "SCRAM-SHA-512"
	MechanismAWSMSKIAM   Mechanism = "AWS_MSK_IAM"
)

// Config is everything labctl and opsd read at startup.
type Config struct {
	Kafka         Kafka
	CruiseControl CruiseControl
	HTTP          HTTP
}

// Kafka is how to reach the cluster. Load guarantees TLS is on whenever SASL is set.
type Kafka struct {
	// Brokers is KAFKA_BOOTSTRAP_SERVERS, comma-separated host:port.
	Brokers []string
	// TLS is KAFKA_TLS. MSK uses TLS on 9094, 9096 (SCRAM), and 9098 (IAM).
	TLS  bool
	SASL SASL
}

// SASL holds credentials. Never log it.
type SASL struct {
	Mechanism Mechanism
	Username  string
	Password  string

	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
}

// CruiseControl is CRUISE_CONTROL_URL, the server root without /kafkacruisecontrol.
type CruiseControl struct {
	URL *url.URL
}

// HTTP is where opsd listens, OPSD_ADDR. The default is loopback only; the
// container sets :8090. A wildcard bind would sit beside the lab's published
// 127.0.0.1:8090 without an error and split traffic between two servers, so a
// local opsd next to the container must use another port.
type HTTP struct {
	Addr string
}

const (
	defaultBrokers          = "localhost:19092,localhost:19093,localhost:19094"
	defaultCruiseControlURL = "http://localhost:9090"
	defaultHTTPAddr         = "127.0.0.1:8090"
)

// Load reads the configuration from the environment.
func Load() (Config, error) {
	return load(os.Getenv)
}

func load(getenv func(string) string) (Config, error) {
	var cfg Config

	for b := range strings.SplitSeq(cmp.Or(getenv("KAFKA_BOOTSTRAP_SERVERS"), defaultBrokers), ",") {
		if b = strings.TrimSpace(b); b != "" {
			cfg.Kafka.Brokers = append(cfg.Kafka.Brokers, b)
		}
	}
	if len(cfg.Kafka.Brokers) == 0 {
		return Config{}, errors.New("config: KAFKA_BOOTSTRAP_SERVERS has no brokers")
	}

	if v := getenv("KAFKA_TLS"); v != "" {
		tls, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("config: KAFKA_TLS: %w", err)
		}
		cfg.Kafka.TLS = tls
	}

	cfg.Kafka.SASL = SASL{
		Mechanism:          Mechanism(strings.ToUpper(getenv("KAFKA_SASL_MECHANISM"))),
		Username:           getenv("KAFKA_SASL_USERNAME"),
		Password:           getenv("KAFKA_SASL_PASSWORD"),
		AWSAccessKeyID:     getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey: getenv("AWS_SECRET_ACCESS_KEY"),
		AWSSessionToken:    getenv("AWS_SESSION_TOKEN"),
	}
	if err := cfg.Kafka.validate(); err != nil {
		return Config{}, err
	}

	raw := cmp.Or(getenv("CRUISE_CONTROL_URL"), defaultCruiseControlURL)
	u, err := url.Parse(raw)
	if err != nil {
		return Config{}, fmt.Errorf("config: CRUISE_CONTROL_URL: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return Config{}, fmt.Errorf("config: CRUISE_CONTROL_URL %q must be an absolute url", raw)
	}
	cfg.CruiseControl.URL = u

	cfg.HTTP.Addr = cmp.Or(getenv("OPSD_ADDR"), defaultHTTPAddr)
	return cfg, nil
}

func (k Kafka) validate() error {
	s := k.SASL
	switch s.Mechanism {
	case MechanismNone:
		return nil
	case MechanismSCRAMSHA256, MechanismSCRAMSHA512:
		if s.Username == "" || s.Password == "" {
			return fmt.Errorf("config: %s needs KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD", s.Mechanism)
		}
	case MechanismAWSMSKIAM:
		if s.AWSAccessKeyID == "" || s.AWSSecretAccessKey == "" {
			return errors.New("config: AWS_MSK_IAM needs AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
		}
	default:
		return fmt.Errorf("config: unsupported KAFKA_SASL_MECHANISM %q", s.Mechanism)
	}
	if !k.TLS {
		return fmt.Errorf("config: %s sends credentials, set KAFKA_TLS=true", s.Mechanism)
	}
	return nil
}
