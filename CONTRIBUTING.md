# Contributing

Prerequisites: Go 1.27+, Docker (Rancher Desktop, Docker Desktop, or
colima), and `make`.

## Before opening a PR

```
go build ./...
go test -race ./...
golangci-lint run ./...
govulncheck ./...
```

Unit tests need no Kafka and no Docker; they run anywhere. Install the
scanners once if you do not have them:

```
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest
go install golang.org/x/vuln/cmd/govulncheck@latest
```

## Ground rules

- Keep the stdlib-first bias. A new module dependency needs a stronger
  reason than "it is convenient."
- Preserve the guardrails: offset apply refuses active groups and stale
  plans, and every apply is audited with an actor and reason.
- Go changes ship with a test that fails without them.
- Docs live in `docs/`; keep them short and current rather than long.

## Lab changes

Anything that alters `infra/compose.yaml` or the broker configuration
should state which failure modes it is exercising, since the whole point
of the lab is rehearsing them.
