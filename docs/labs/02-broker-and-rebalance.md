# Lab 2. Broker loss, then Cruise Control

Cruise Control is a separate process that watches broker and partition load and proposes replica moves. Each broker runs a metrics reporter that publishes samples to `__CruiseControlMetrics`. Cruise Control reads them, builds a model, and checks it against goals.

Self-healing is off. Two-step verification is on, so every write request, including a dry run, waits on the review board until someone approves it.

On MSK Standard you cannot install the reporter, so Cruise Control does not run there. You make the same decisions with other tools. AWS replaces a dead broker. You watch under-replicated partitions. When leaders or disk are skewed, you add brokers in Terraform and run `kafka-reassign-partitions` with a throttle. This lab is where you learn to read a proposal before you are holding a throttle flag on a real cluster.

## Broker down

```bash
make up
bin/labctl health
make broker-stop BROKER=3
bin/labctl health
```

```text
UNDER-REPLICATED, still writable, one more failure from trouble: 66 partitions
TOPIC                   PARTITIONS  MIN ISR  EXAMPLE
__consumer_offsets      50          2        p0 leader 1 replicas [3 1 2] isr [1 2]
orders                  6           2        p0 leader 1 replicas [1 2 3] isr [1 2]
...
```

Every RF 3 topic is under-replicated but still writable. Two replicas remain in sync, and `min.insync.replicas` is 2, so `acks=all` producers keep working. `labctl health` exits 3, so a script or an alert can tell the difference between degraded and down.

Things to try while the broker is down:

- `bin/labctl produce --topic orders --count 5 --poison-at 0` still succeeds. Say why.
- Kafka UI, Brokers, shows two brokers. Topics, `orders`, shows the ISR shrink.
- Stop a second broker with `make broker-stop BROKER=2` and write down what fails: produce, `labctl health`, Kafka UI. With two of three controllers gone, the KRaft quorum is lost too. Start both brokers again afterwards.

```bash
make broker-start BROKER=3
bin/labctl health
```

Wait until health is clean. That recovery is the usual MSK broker-replacement story.

This lab once shipped with Cruise Control's sample topics at RF 2 and min ISR 2. With one broker down, those topics appeared under "UNDER MIN ISR", and Cruise Control could not write its own samples. RF must exceed min ISR. It is RF 3 now, and it is worth checking the same thing on your own internal topics.

## Read Cruise Control

```bash
bin/labctl cc state
bin/labctl cc proposals
```

`cc state` needs the monitor `RUNNING`, a few windows, and high coverage. One window is 60 seconds. If proposals are not ready, wait a minute.

`cc proposals` is the dry run. It reads the plan Cruise Control keeps ready, moves nothing, and needs no review. Goals in this lab, in order:

1. **RackAwareGoal.** One replica of each partition per rack. The racks are `use1-az1`, `use1-az2`, and `use1-az3`. On MSK, racks are Availability Zones.
2. **ReplicaCapacityGoal and DiskCapacityGoal.** Hard limits. A hard goal is never broken to satisfy a later goal.
3. **ReplicaDistributionGoal, TopicReplicaDistributionGoal, LeaderReplicaDistributionGoal.** Spread replicas, topic replicas, and leaders evenly.

`NO-ACTION` on every goal means the cluster already meets them. That is a successful check, not a broken tool.

## Walk a change through the review board

Open http://localhost:8081, cluster `lab`.

1. Kafka Cluster Administration: submit a rebalance. The response is a review id, not a move.
2. Peer Reviews: the request is `PENDING_REVIEW`. Read what it asks for, then approve it with a reason.
3. Submit it again with only the review id. Cruise Control runs the original request.

The same flow with curl:

```bash
B=http://localhost:9090/kafkacruisecontrol
curl -s -X POST "$B/rebalance?dryrun=true&json=true"            # returns Id N, PENDING_REVIEW
curl -s -X POST "$B/review?approve=N&reason=LAB-2&json=true"    # APPROVED
curl -s -X POST "$B/rebalance?review_id=N"                      # runs it; review_id must be the only parameter
bin/labctl cc reviews
```

Use `dryrun=true` until you can name the goal behind every move in the result.

## Add a broker

You cannot keep RF 3 and remove one of three brokers without breaking RackAwareGoal. Add capacity first.

```bash
make add-broker
bin/labctl health
curl -s -X POST "$B/add_broker?brokerid=4&dryrun=true&json=true"
```

Approve that request on the review board, resubmit it with `review_id`, and read which replicas would move onto broker 4. On MSK, the matching change is a Terraform broker-count update, then waiting for the new broker, then a throttled reassignment if the leaders did not spread the way you need.

## Pass

You can point at a proposal and name the goal behind a move. You can explain why losing one broker is fine and losing two is not. You can say why this lab runs Cruise Control and production MSK does not.
