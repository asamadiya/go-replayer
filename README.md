# go-replayer

A high-throughput gRPC replay tool with Poisson pacing, plus local target servers for validation and burstiness diagnostics.

This repo is intended for replay benchmarking, replay pod takeover workflows, and reproducible replay-quality testing (especially tail-latency-sensitive workloads).

## Repository layout

```text
.
├── main.go                          # go-replayer binary
├── main_test.go                     # parser/takeover unit tests
├── cmd/
│   ├── grpc-dummy-target/
│   │   └── main.go                  # lightweight TLS target for replay smoke tests
│   └── grpc-gap-target/
│       └── main.go                  # target that measures inter-arrival/burst Poisson fit
│   └── grpc-parity-check/
│       ├── main.go                  # compares responses from two gRPC targets
│       └── proto_compare.go         # protobuf float extraction/comparison helpers
├── go.mod
└── go.sum
```

## Architecture

### 1) Replay sender (`main.go`)

- Reads length-prefixed protobuf request payloads from a replay file.
- Sends unary gRPC requests to a target method (default:
  `/example.replay.v1.ReplayService/Score`).
- Uses Poisson arrivals with bounded workers/queue for high-QPS stability.
- Supports an optional **micro-burst overlay** for intentional, parameterised burstiness.
- Supports takeover mode (`--take-over-replay`) to auto-discover an external replay deployment's artifacts.

Key behavior:

- Deadline-driven Poisson scheduler (to reduce pacing drift)
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
- Optional client-cert requirement for identity verification.

### 3) Gap target (`cmd/grpc-gap-target`)

- Measurement-focused target used to evaluate arrival process quality.
- Computes inter-arrival stats, burst windows, and Poisson expectation comparisons.
- Useful for proving whether replay traffic shape is actually Poisson-like.

### 4) Parity checker (`cmd/grpc-parity-check`)

- Sends the same replayed requests to two targets (`--target-a`, `--target-b`).
- Compares returned bytes and float values within configurable tolerance.
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
go test ./...
```

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
