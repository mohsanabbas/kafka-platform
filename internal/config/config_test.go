package config

import (
	"slices"
	"testing"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := load(env(nil))
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"localhost:19092", "localhost:19093", "localhost:19094"}
	if !slices.Equal(cfg.Kafka.Brokers, expected) {
		t.Errorf("brokers = %v, expected %v", cfg.Kafka.Brokers, expected)
	}
	if got := cfg.CruiseControl.URL.String(); got != "http://localhost:9090" {
		t.Errorf("cruise control url = %s", got)
	}
	if cfg.HTTP.Addr != "127.0.0.1:8090" {
		t.Errorf("http addr = %s", cfg.HTTP.Addr)
	}
}

func TestLoadValidation(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		isValid bool
	}{
		{"brokers are trimmed", map[string]string{"KAFKA_BOOTSTRAP_SERVERS": " b-1:9096 , b-2:9096 "}, true},
		{"only commas", map[string]string{"KAFKA_BOOTSTRAP_SERVERS": " , "}, false},
		{"scram with tls", map[string]string{"KAFKA_SASL_MECHANISM": "scram-sha-512", "KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p", "KAFKA_TLS": "true"}, true},
		{"scram without tls", map[string]string{"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512", "KAFKA_SASL_USERNAME": "u", "KAFKA_SASL_PASSWORD": "p"}, false},
		{"scram without password", map[string]string{"KAFKA_SASL_MECHANISM": "SCRAM-SHA-512", "KAFKA_SASL_USERNAME": "u", "KAFKA_TLS": "true"}, false},
		{"iam with keys", map[string]string{"KAFKA_SASL_MECHANISM": "AWS_MSK_IAM", "AWS_ACCESS_KEY_ID": "a", "AWS_SECRET_ACCESS_KEY": "s", "KAFKA_TLS": "1"}, true},
		{"iam without keys", map[string]string{"KAFKA_SASL_MECHANISM": "AWS_MSK_IAM", "KAFKA_TLS": "true"}, false},
		{"unknown mechanism", map[string]string{"KAFKA_SASL_MECHANISM": "GSSAPI", "KAFKA_TLS": "true"}, false},
		{"bad tls flag", map[string]string{"KAFKA_TLS": "yes please"}, false},
		{"relative cruise control url", map[string]string{"CRUISE_CONTROL_URL": "cruise-control:9090"}, false},
		{"unparsable cruise control url", map[string]string{"CRUISE_CONTROL_URL": "http://[::1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(env(tt.env))
			if tt.isValid && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.isValid && err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
