terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "0.7.1"
    }
  }
}

# Point this at the lab the same way a workspace points at MSK bootstrap brokers.
# The compose topic-init job already creates these topics so the drills run without Terraform.
provider "kafka" {
  bootstrap_servers = ["localhost:19092", "localhost:19093", "localhost:19094"]
  tls_enabled       = false
}

resource "kafka_topic" "orders" {
  name               = "orders"
  replication_factor = 3
  partitions         = 6

  config = {
    "min.insync.replicas" = "2"
    "retention.ms"        = "86400000"
  }
}

resource "kafka_topic" "payments" {
  name               = "payments"
  replication_factor = 3
  partitions         = 6

  config = {
    "min.insync.replicas" = "2"
    "retention.ms"        = "86400000"
  }
}

# Offset resets and other operator actions from labctl and opsd.
resource "kafka_topic" "platform_audit" {
  name               = "platform.audit"
  replication_factor = 3
  partitions         = 3

  config = {
    "min.insync.replicas" = "2"
    "retention.ms"        = "2592000000"
  }
}
