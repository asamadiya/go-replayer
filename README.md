# go-replayer

[![Go Reference](https://pkg.go.dev/badge/github.com/asamadiya/go-replayer.svg)](https://pkg.go.dev/github.com/asamadiya/go-replayer)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A high-throughput gRPC replay tool with Poisson pacing, plus local target servers for validation and burstiness diagnostics.

This repo is intended for replay benchmarking, replay pod takeover workflows, and reproducible replay-quality testing (especially tail-latency-sensitive workloads).

## Repository layout

```text
.
├── main.go                          # go-replayer sender binary (entrypoint + flags)
├── doc.go                           # package documentation
├── scheduler.go                     # deadline-driven Poisson scheduler + replica streams + burst overlay
├── metrics.go                       # NDJSON metrics + sender-side window analysis
├── main_test.go                     # parser/takeover unit tests
├── scheduler_test.go                # scheduler + burst + replica tests
├── metrics_test.go                  # metrics / window-analysis tests
├── hardening_test.go                # reservoir, absorbing precision, parser-bounds, writer-error tests
├── cmd/
│   ├── grpc-dummy-target/
│   │   ├── main.go                  # lightweight TLS target for replay smoke tests
│   │   └── main_test.go
│   ├── grpc-gap-target/
│   │   ├── main.go                  # target that measures inter-arrival/burst Poisson fit
│   │   └── main_test.go             # quantile / Poisson-tail / window-analysis tests
│   └── grpc-parity-check/
│       ├── main.go                  # compares responses from two gRPC targets
│       ├── proto_compare.go         # protobuf float extraction/comparison helpers
│       ├── proto_compare_test.go    # comparison-logic tests
│       └── hardening_test.go        # strict-parser + false-pass-guard tests
├── ci/ci.yml                        # GitHub Actions workflow (move to .github/workflows/ to enable)
├── .golangci.yml                    # linter configuration
├── Makefile                         # build, test, lint, cover targets
├── CONTRIBUTING.md                  # development guide and quality gates
├── CHANGELOG.md
├── SKILL.md                         # operational guide (loadable skill)
├── go.mod
└── go.sum
```

## Architecture

### 1) Replay sender (`main.go`)

- Reads length-prefixed protobuf request payloads from a replay file.
- Sends unary gRPC requests to a target method (default:
  `/example.replay.v1.ReplayService/Score`).
- Uses Poisson arrivals with bounded workers/queue for high-QPS stability.
- Can split total QPS across multiple independent upstream-replica Poisson streams.
- Supports an optional **micro-burst overlay** for intentional, parameterised burstiness.
- Supports takeover mode (`--take-over-replay`) to auto-discover an external replay deployment's artifacts.

Key behavior:

- Deadline-driven Poisson scheduler (to reduce pacing drift), with optional independent replica streams
- Optional burst overlay (size × window × period × shape × jitter × mode) — see below
- Bounded worker dispatch path
- Rich per-second telemetry (`drift`, `sched_lag`, `queue`, `inflight`, latency quantiles, `bursts`/`spikes`/`base` when bursts are on)
- Sender-side end-of-run window analyser (Fano factor, max/p99 in 20ms bins, threshold-crossing bins) — proves the *emitted* shape independent of any receive-side analyser
- Optional NDJSON metrics file (`--metrics-jsonl`) with per-second ticks, burst-onset events, and the final summary

#### Burst overlay flags

| Flag | Meaning | Default |
|---|---|---|
| `--burst-size N` | Requests per burst (0 disables) | `0` |
| `--burst-window D` | Window over which the N requests fire | `20ms` |
| `--burst-period D` | Mean interval between burst onsets | `1s` |
| `--burst-shape uniform\|spike\|random` | How the N requests are placed inside the window | `uniform` |
| `--burst-jitter fixed\|poisson` | Spacing between successive burst onsets | `fixed` |
| `--burst-mode additive\|absorbing` | `additive`: bursts ride on top of base λ. `absorbing`: base λ is reduced so long-run mean QPS = `--qps` | `additive` |
| `--analyzer-window D` | Bin width for sender-side window analysis | `20ms` |
| `--metrics-jsonl PATH` | Write per-second ticks, burst events, and summary as NDJSON | unset |

In `absorbing` mode the base Poisson rate is reduced by the burst average so the
long-run mean stays at `--qps`. A configuration whose burst average
(`--burst-size / --burst-period`) exceeds `--qps` cannot be absorbed and is
rejected at startup.

#### Replica Poisson streams

Use `--replicas R` to model traffic coming from `R` independent upstream replicas (maximum 10000). The sender keeps `--qps` as the aggregate target and splits it equally across replicas, so `--qps 300 --replicas 30` runs 30 independent Poisson streams at 10 QPS each.

```bash
./bin/go-replayer \
  -file /tmp/replay.bin -target host:28826 \
  -qps 300 -replicas 30 -duration 60s \
  -tls -insecure
```

The end-of-run sender-side window analysis remains over the aggregate emitted stream. For ideal Poisson traffic, superposing independent replica streams should remain Poisson-like; the value of `--replicas` models independent sources rather than changing total mean QPS.

Representative 60s simulation at 300 aggregate QPS with 20ms analysis bins:

| Run | Per-replica QPS | Fano | max/bin | p99 bin count | bins ≥14 | bins ≥16 |
|---|---:|---:|---:|---:|---:|---:|
| `--replicas 1` | 300.0 | 0.997 | 15 | 12 | 6 | 0 |
| `--replicas 30` | 10.0 | 1.017 | 16 | 12 | 11 | 3 |

Example: 30-request spike packed into a 20ms window every 1s, with the base
Poisson rate reduced so the long-run mean stays at `--qps`:

```bash
./bin/go-replayer \
  -file /tmp/replay.bin -target host:28826 \
  -qps 200 -duration 60s -tls -insecure \
  -burst-size 30 -burst-window 20ms -burst-period 1s \
  -burst-shape spike -burst-mode absorbing \
  -metrics-jsonl /tmp/run.jsonl
```

### 2) Dummy target (`cmd/grpc-dummy-target`)

- Minimal TLS gRPC target for functional replay validation.
- Accepts unknown service/method and responds quickly.
- Optional client-cert *presentation* requirement (presence-only diagnostic logging; the cert is not trust-verified, so this is not client authentication).

### 3) Gap target (`cmd/grpc-gap-target`)

- Measurement-focused target used to evaluate arrival process quality.
- Computes inter-arrival stats, burst windows, and Poisson expectation comparisons.
- Useful for proving whether replay traffic shape is actually Poisson-like.

### 4) Parity checker (`cmd/grpc-parity-check`)

- Sends the same replayed requests to two targets (`--target-a`, `--target-b`).
- Compares returned bytes and float values within configurable tolerance.
- Byte-different responses with no comparable floats (or a differing number of
  floats) are treated as mismatches, not silent passes.
- Exits non-zero on any mismatch, and on any RPC error unless `--allow-errors`.
- Useful for validating batching/no-batching response parity during rollout.

## Build guide

### Prerequisites

- Go toolchain (1.24+ recommended)
- Linux/macOS shell

### Build all binaries

```bash
# from repo root
go build -o bin/go-replayer ./
go build -o bin/grpc-dummy-target ./cmd/grpc-dummy-target
go build -o bin/grpc-gap-target ./cmd/grpc-gap-target
go build -o bin/grpc-parity-check ./cmd/grpc-parity-check
```

Binaries are built locally into `bin/` (git-ignored); the repo ships source only.

### Run unit tests

```bash
go test ./...          # unit tests
go test -race ./...    # with the race detector
make cover             # coverage summary
```

A `Makefile` wraps the common loops: `make all` (fmt-check, vet, test, build),
`make lint`, `make race`, and `make cover`.

## Development guide

### Common local loop

```bash
# 1) edit
$EDITOR main.go

# 2) test
go test ./...

# 3) build
go build ./...
```

### Example replay run

```bash
./bin/go-replayer \
  -file /path/to/replay_2gb.bin \
  -target host:28826 \
  -qps 300 \
  -duration 60s \
  -tls
```

### Example takeover run (external replay deployment)

```bash
./bin/go-replayer \
  -take-over-replay \
  -target host:28826 \
  -qps 300 \
  -duration 60s
```

## Notes for performance work

- For p99-sensitive systems, average QPS is insufficient.
- Always pair replay tests with burst-shape checks (Fano, max requests/20ms, tail-window ratios).
- Use `cmd/grpc-gap-target` (or equivalent) as a regression gate in CI/nightly perf jobs.
- The sender's burst overlay lets you reproduce a known-bad burst signature (e.g. a "30 requests/20ms" pattern) deterministically against any target, then verify whether a SUT change moves the SLO needle.
- The sender-side window analyser (printed at end of run, also serialised into `--metrics-jsonl`) reports Fano factor and threshold-bin counts for the **emitted** stream, so you can prove the load generator's shape without trusting the receiver to be honest.

### Worked example: baseline vs burst (sender-side window analysis, 20ms bins)

| Run | Fano | max/bin | p99 bin count | bins ≥30 |
|---|---|---|---|---|
| Poisson only, 200 QPS, 10s | 1.156 | 14 | 9 | 0 |
| `--burst-size 30 --burst-shape spike --burst-period 1s --burst-mode absorbing`, same target QPS | 4.598 | 37 | 32 | 9 |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development loop, quality gates,
and conventions. In short: branch from `master`, add tests and doc updates, and
ensure `make all && make lint` is green before opening a PR.

## License

Released under the [MIT License](LICENSE).
