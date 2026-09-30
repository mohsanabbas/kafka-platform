# Lab 1. Take a consumer out of an incident

`payments-worker` commits what it processed and crashes on a poison record. Its commit now points at that record, so every restart crashes on it again. The app is the incident. You stop it, move the group past the record, and only then start it.

Kafka UI, `labctl`, and opsd all use the same Admin API. On MSK you run the same moves against the cluster bootstrap brokers.

## Break it

```bash
make up
bin/labctl produce --topic orders --count 20 --poison-at 11
bin/labctl consume --group payments-worker --topic orders
bin/labctl consume --group payments-worker --topic orders
```

Both runs print the same line, for example `poison at orders-0 offset 10, crashing`. The partition can differ on your machine. That is a crash loop. In production this is where the pod restarts every few seconds and lag climbs.

```bash
bin/labctl lag --group payments-worker
```

The group is `Empty` because the app is down. The commit on the poison partition is the poison offset. The lag is everything behind it.

## Plan

```bash
bin/labctl offsets plan --group payments-worker --topic orders --to skip
```

```text
PARTITION  FROM  TO  END  LAG AFTER
orders-0   10    11  20   9
```

`skip` moves one record past the commit, and only on partitions that have something to skip. The plan is written to `offsets.plan.json`. At work, this file goes in the incident channel before anyone applies it.

Other modes:

- `--to latest` parks the group at the end and drops the backlog. Use it when the business says the backlog is worthless.
- `--to earliest` replays everything still in retention.
- `--to timestamp --at 30m` (or an RFC 3339 time) replays from a point in time, for example the start of a bad deploy.
- `--partitions 0` limits the plan to the partitions you name.

## Apply

```bash
bin/labctl offsets apply --plan offsets.plan.json --reason "LAB-1 poison record on orders-0"
bin/labctl consume --group payments-worker --topic orders
```

The consumer processes the remaining nine records. Stop it with Ctrl-C, then run `bin/labctl lag --group payments-worker` and confirm the lag is 0.

Apply refuses a plan, and records the refusal, in these cases. Trigger each one:

- **The commit moved since the plan.** Apply the same file a second time. The first apply already moved the commit, so the second is refused as stale. This is what happens when two people work the same incident.
- **The app is still running.** Leave `bin/labctl consume` running. In a second terminal, plan `--to earliest` and apply it. The group has a member, so apply is refused.
- **There is no `--reason`.**

## Read the audit trail

Open Kafka UI at http://localhost:8080, cluster `lab`, Topics, `platform.audit`, Messages. Each apply attempt is there with the actor, reason, outcome, and the moves. Refused attempts are there too.

## Same move through opsd

```bash
curl -s -X POST localhost:8090/v1/groups/payments-worker/offsets/plan \
  -d '{"topic":"orders","mode":"skip"}' > plan.json

jq -n --slurpfile p plan.json '{plan: $p[0], reason: "LAB-1 via api"}' |
  curl -s -X POST localhost:8090/v1/groups/payments-worker/offsets/apply \
    -H 'X-Actor: alice' -d @-
```

A running group returns 409 with `group has active members`. A stale plan returns 409 with `plan again`.

## Same move in Kafka UI

Cluster `lab`, Consumers, `payments-worker`. The reset control is disabled while the group is Stable, so stop the consumer first. Reset `orders` partition 0 to the offset after the poison record.

Kafka UI records nothing about who did it or why. Compare that with the audit topic and decide which path you would allow on a production cluster.

## Pass

You can say why the group had to be empty, show the committed offset before and after, and explain what the stale-plan check protects you from.
