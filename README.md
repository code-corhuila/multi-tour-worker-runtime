# multi-tour-worker-runtime

> Shared runner of the worker jobs: schedule, batch limits, retries, correlation and service token (ADR-004 D3)

Part of the **Multi Tour** distributed system — team `multi-tour`, Grupo 2.
Governance and documentation live in [`multi-tour-docs`](https://github.com/code-corhuila/multi-tour-docs).

## What it is

A Go module shared by the eight `multi-tour-worker-<job>` repositories (ADR-004 D3). It gives them the
scheduler, batch limits, correlation id, HTTP client with retries and the health handler. It holds no
business rule and exposes no API.

## How to start it

It does not run by itself. A worker imports it: `go get github.com/code-corhuila/multi-tour-worker-runtime@<commit>`
(public repository, no token). Packages: `runner`, `correlation`, `httpclient`, `config`, `health`, `logging`.

## Where the data is

Nowhere: it has no database and stores nothing.

## How to test it

`gofmt -l .` must print nothing, then `go vet ./...` and `go test ./...` (Go 1.26). The same three run in `ci.yml` on pull requests to `develop` and `qa`.

## What is missing

The `Job` port, the packages' code and their tests (tasks T-03 to T-06 of the piece); today they are empty packages. The first tag `v0.1.0` is set when a release reaches `main`.
## Branching

Three permanent branches. **None of them accepts a direct commit** — you enter through a child
branch and leave through a Pull Request.

```
develop  <--PR--  feat/... fix/... chore/...
qa       <--PR--  qa/...
main     <--PR--  release/...  hotfix/...
```

Promotion happens **by re-application** (`git cherry-pick -x`), never by merging one permanent
branch into another: `merge develop -> qa` and `merge qa -> main` do not exist in this model.

`main` requires **1 approval from `ariel5253`**. On `develop` and `qa` the team sets its own review
rule.

Full policy: `00-governance/branching-policy.md` in `multi-tour-docs`.
