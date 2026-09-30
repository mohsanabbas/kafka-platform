#!/bin/bash
# Mirrors infra/terraform/topics. At work Terraform owns topics. Here a one-shot container does.
set -eu
bootstrap="kafka-1:19092"

create() {
  local topic=$1 partitions=$2 retention_ms=$3
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$bootstrap" \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config retention.ms="$retention_ms"
  echo "topic $topic ready"
}

day=86400000
create orders 6 "$day"
create payments 6 "$day"
create platform.audit 3 $((30 * day))
