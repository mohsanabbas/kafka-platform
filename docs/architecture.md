# Architecture

The lab has two jobs: be close enough to MSK that the incident muscle memory transfers, and model the platform tooling a team would put in front of a shared Kafka cluster.

## Shape

- **Infra** lives in `infra/`, one directory per service, with its own image and config. `infra/compose.yaml` is the single entry point, and `make up` is the only command you need.
- **Go** lives in `cmd/` and `internal/`. Two binaries share one set of services:
  - `opsd` is the ops API a platform team would run next to the cluster.
  - `labctl` is the break-glass CLI for when that API is down or you are on a bastion.

Both build the same dependency graph in `internal/app`. That is the only package that imports dig. Services take interfaces in their constructors and never see the container.

```text
cmd/labctl ─┐
            ├─ internal/app (dig) ─┬─ cluster.Service ───────┐
cmd/opsd ───┘        │             ├─ consumergroup.Service ─┼─ kadm.Client ─ kgo.Client ─ Kafka
                     │             │        └─ audit.Log ────┘
                     │             └─ cruisecontrol.Client ─── Cruise Control REST
                     └─ httpapi (opsd only)
```

## Decisions

### Cruise Control UI and API share one origin

The UI is a static Vue app. Cruise Control's CORS filter answers preflights with `Access-Control-Allow-Headers: User-Task-ID,Date` and cannot allow `Content-Type`. Every POST from the UI sends a JSON body, so the browser blocked every write and reported "Network Error". Reads worked, which is why only some screens failed.

The UI's nginx now proxies `/kafkacruisecontrol/` to Cruise Control, and CORS is off in Cruise Control. There is no cross-origin request left to break. The nginx config also forwards bare endpoint paths such as `/review_board`, because the Peer Reviews page builds its URL before the config loads when opened by deep link.

### Offset resets are plan, then apply

A reset is the most common consumer incident action, and the easiest to get wrong. `consumergroup.Service` splits it the way Terraform does:

- `Plan` reads the group state, the commits, and the log start and end offsets. It returns the exact moves and changes nothing. The CLI writes the plan to a file, so it can go into the incident channel for a second pair of eyes.
- `Apply` takes that plan and refuses it when:
  - the group still has members, since Kafka would reject the commit anyway and the app would overwrite it;
  - any committed offset moved since the plan was made, so you never apply a plan built on a stale view;
  - there is no actor or reason.

The modes are `skip` (one record past the commit, for a poison record), `latest` (park the group and drop the backlog), `earliest` (replay retention), and `timestamp` (replay from a time, for example the start of a bad deploy).

Call flow, decision diagrams, and the audit path are diagrammed in [offset-reset-design.md](offset-reset-design.md).

### Every change is audited, including refusals

`audit.Log` writes each apply attempt to the structured log and to the `platform.audit` topic, keyed by group and topic. Refusals are recorded too. During a postmortem, "someone tried to reset while the app was still running" is as useful as the reset itself. The topic has RF 3, 30-day retention, and is readable in Kafka UI.

### Config comes from the environment

`internal/config` reads the same variable names Vault agent renders at work. `internal/kafka` is the only place that turns config into franz-go options, including TLS, SCRAM, and MSK IAM. SASL is refused without TLS. The lab and MSK differ only in environment, not in code. Everything used here, including SASL, ships with franz-go, so the module has three direct dependencies: franz-go, kadm, and dig.

### dig at the composition root, with a small lifecycle

dig builds the graph lazily and memoizes it. `labctl cc state` never dials Kafka, and `labctl health` never calls Cruise Control. dig has no lifecycle, so `app.App` keeps a list of closers that constructors register (for example `kgo.Client.Close`), and the binary calls `Close` on exit. The narrow admin interfaces (`cluster.Admin`, `consumergroup.Admin`) are bound to `*kadm.Client` with `dig.As`. `TestGraphResolves` runs the whole graph in dry-run mode, so a missing provider fails in CI rather than on the first request.

### Cruise Control writes stay on the review board

The Go client is read-only: state, proposals, and the review board. Rebalances, adding brokers, and removing brokers go through the Cruise Control UI or curl, where two-step verification holds them for approval. Reading `GET /proposals` gives the dry-run plan without a review.

## Lessons baked into the config

- Cruise Control's sample-store topics started at RF 2 while the brokers default to `min.insync.replicas=2`. With one broker down, Cruise Control could not write its own samples. RF must exceed min ISR. The lab now uses RF 3, and `labctl health` reports "under min ISR" separately from "under-replicated" for this reason.
- The metrics reporter jar is baked into the broker image with a pinned checksum and must match the Cruise Control version.
- Cruise Control refuses to start if the metadata refresh or the reporter interval is longer than its sampling interval. All three are 10 to 15 seconds here.
- Broker log dirs must outlive the container. With a fixed `CLUSTER_ID` and logs in `/tmp`, recreating the brokers built a new, empty cluster under the same ID, with partition leader epochs back at 0. Kafka UI's long-lived admin client kept its higher cached epochs and rejected the new metadata as stale. The topics page then failed with "Timed out waiting for a node assignment" on `listOffsets`, while the cluster list still looked online. Each broker now has a named volume.
- nginx resolves a static `upstream` once, at startup. After `cruise-control` was recreated, the CC UI proxied to its old IP, which by then belonged to `opsd`, and every API call failed with 502. The proxy now uses Docker DNS (`resolver 127.0.0.11`) and a variable in `proxy_pass`, so it looks the name up again.

## Deliberately not built

- **Authentication on opsd.** In production it sits behind an auth proxy that sets `X-Actor` from SSO. The lab trusts the header.
- **Metrics and tracing.** The next step would be Prometheus metrics on opsd and JMX from the brokers.
- **Cruise Control writes from Go.** Keeping them on the review board is the point of the drill.

The full inventory of lab-versus-production gaps, and what to verify before using these tools at work, is in [production.md](production.md).
