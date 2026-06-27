# Contributing to go-replayer

Thanks for your interest in improving go-replayer. This guide covers the local
development loop, quality gates, and conventions used in this repository.

## Prerequisites

- Go toolchain 1.24+ (`go version`)
- A POSIX shell (Linux/macOS)
- Optional: [`golangci-lint`](https://golangci-lint.run/) v1.64.x for linting

## Development loop

```bash
make fmt        # gofmt -w .
make vet        # go vet ./...
make test       # go test ./...
make race       # go test -race ./...
make cover      # coverage summary
make lint       # golangci-lint run
make build      # build all four binaries into bin/
```

`make all` runs the full local gate (`fmt-check`, `vet`, `test`, `build`) and is
the quickest way to reproduce CI before pushing.

## Quality gates

Every pull request must pass the [CI workflow](.github/workflows/ci.yml):

1. **Formatting** — `gofmt -l .` must be empty.
2. **Vet** — `go vet ./...` clean.
3. **Build** — `go build ./...` succeeds.
4. **Tests** — `go test -race ./...` passes.
5. **Lint** — `golangci-lint run` clean (see [`.golangci.yml`](.golangci.yml)).

New behavior should ship with tests. Pure logic (parsing, scheduling, metrics,
proto comparison) is unit-tested directly; the network target servers under
`cmd/` are validated through the workflows documented in `SKILL.md`. CI enforces
a total statement-coverage floor of 40% (`go tool cover -func`), so substantial
new code must come with tests.

## Conventions

- **Branches**: `feature-<description>` or `fix-<description>`.
- **Commits**: imperative mood, concise subject, body explaining *why*.
- **Style**: standard `gofmt`; prefer table-driven tests; keep exported symbols
  documented with godoc comments.
- **No vendored binaries**: build artifacts live in `bin/` and are git-ignored.

## Submitting changes

1. Branch from `master`.
2. Make the change with tests and updated docs (`README.md` / `SKILL.md`).
3. Run `make all && make lint`.
4. Open a PR; ensure CI is green before requesting review.
