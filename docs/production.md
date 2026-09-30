# The lab versus production

The lab trades production mechanics for one-command bring-up. This page is the honest inventory of what it does not model, why each gap is open, and what to verify before running the same tools against a real MSK cluster. The design rationale is in [architecture.md](architecture.md).

## Transport security

| | Lab | MSK |
| --- | --- | --- |
| Listener | PLAINTEXT | TLS |
| Client auth | none | SCRAM (9096) or IAM (9098) |
| Bind | 127.0.0.1 only | VPC |

The Go tools already carry the production path: [internal/kafka](../internal/kafka/client.go) builds clients with TLS 1.2 minimum, SCRAM-SHA-256/512, and MSK IAM, and `internal/config` refuses SASL without TLS. Config validation is unit-tested; the client paths have never dialed a real MSK cluster. Run `labctl health` at work with the Vault-rendered variables once, outside an incident, before you need it inside one. See [README: Pointing the tools at MSK](../README.md#pointing-the-tools-at-msk).

Not modeled here, deliberately: server certificates and SASL_SSL wiring for every container (brokers, the Cruise Control reporter and client, Kafka UI, opsd) would make the lab about certificate management instead of Kafka.

## opsd has no authentication

The design assumes an auth proxy that sets `X-Actor` from SSO and strips client-supplied values. Until that proxy exists, keep opsd on loopback; compose already binds every published port to 127.0.0.1. The smallest real fix is a bearer-token middleware, which needs no new dependencies.

## Kafka UI is a cluster admin

`DYNAMIC_CONFIG_ENABLED=true` is what makes the offset-reset drill possible in the UI, and it is exactly what makes the UI a production hazard: anyone who can open the page can reset offsets, edit topic configs, and delete topics. In production, disable dynamic config or put the UI behind the same auth proxy.

## One host, one failure domain

`broker.rack` models MSK's AZ spread and feeds Cruise Control's rack-aware goal, but every broker runs on one Docker host: one VM failure takes all three. The lab can model broker loss (`make broker-stop`), not host loss. Broker data is on local named volumes; `make reset` deletes it; there are no backups. Nothing here survives the host, and neither does the audit trail.

## Combined broker and controller nodes

Each broker is a combined broker+controller KRaft node. The client view matches MSK, which hides controllers entirely. Quorum-loss and controller-isolation drills are not possible here: the controller is the broker you stopped.

## No metrics pipeline

opsd logs every request as JSON, and visibility comes from `labctl`, Kafka UI, and Cruise Control. There is no Prometheus, no JMX exporter, no dashboards. Working an incident without graphs is part of what the drill trains; a metrics endpoint on opsd is the natural next build step.

## Cruise Control cannot exist on MSK Standard

MSK Standard does not allow the metrics reporter on brokers. Cruise Control is here to train the decisions you would make at work with Terraform (broker count, storage) and `kafka-reassign-partitions` with throttles. See the README section "What Cruise Control is, and why MSK does not have it".

## Before running these tools at work

- [ ] `labctl health` once against MSK with Vault-rendered SCRAM or IAM environment, outside an incident.
- [ ] opsd behind the auth proxy; `X-Actor` set server-side and client values stripped; loopback until then.
- [ ] Kafka UI dynamic config disabled, or the UI behind auth.
- [ ] `platform.audit` exists before the first apply: RF 3, min ISR 2, 30-day retention. It is in `infra/terraform/topics`.
- [ ] One full poison-record drill against a staging topic before the first production apply.
