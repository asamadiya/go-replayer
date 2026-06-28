---
name: go-replayer
description: Drive the go-replayer gRPC load generator for replay benchmarking, intentional micro-burst injection, and burst-shape / parity diagnostics. Use this skill whenever the user mentions go-replayer, gRPC replay, replay file, replay deployment takeover, burst injection on a gRPC target, microburst stress test, Poisson replay, Fano factor, gap-target, parity-check, or any task involving recorded `[len][method][len][payload]` replay files. Also trigger when the user asks to reproduce a bursty replay signature against a SUT.
---

# go-replayer Operational Skill

Operate the go-replayer toolchain: Poisson + burst gRPC replay sender, dummy / gap / parity target servers, NDJSON metrics, and replay-pod takeover. This is the operational sibling of the README — meant to be loaded as a skill, not read top-to-bottom.

## Quick reference

| Item | Value |
|------|------:|
| Default method | `/example.replay.v1.ReplayService/Score` |
| Replay file format | `[uint32 BE method_len][method][uint32 BE payload_len][payload]` repeated |
| Default replayer connections | 4 (auto-scales to `max(32, NCPU*4)` in takeover mode) |
| Default request timeout | 30s |
| Default Poisson pacing | deadline-driven, `rate = --qps` |
| Burst overlay default | disabled (`--burst-size 0`) |
| Sender-side analyser bin | 20ms |
| JSONL output | `--metrics-jsonl PATH` |

## Binaries (under `bin/`)

| Binary | Role |
|---|---|
| `go-replayer` | Replay sender (Poisson + optional burst overlay) |
| `grpc-dummy-target` | Minimal TLS gRPC target — accepts unknown methods, responds quickly. Use for functional smoke tests. |
| `grpc-gap-target` | TLS gRPC target that records arrival timestamps and prints Fano / micro-burst histogram on SIGTERM. Use for receive-side burst-shape verification. |
| `grpc-parity-check` | Sends the same replayed requests to two targets and compares responses with float tolerance. |

Build everything from the repo root:

```bash
go build -o bin/go-replayer ./
go build -o bin/grpc-dummy-target ./cmd/grpc-dummy-target
go build -o bin/grpc-gap-target ./cmd/grpc-gap-target
go build -o bin/grpc-parity-check ./cmd/grpc-parity-check
```

Binaries are built locally into `bin/` (git-ignored); the repo ships source only.

## go-replayer flags

### Core (always relevant)

| Flag | Required | Meaning |
|---|---|---|
| `-file PATH` | yes (unless `-take-over-replay`) | Replay file (length-prefixed format above) |
| `-target HOST:PORT` | yes | gRPC target. Also accepts `(host,port)` and `["host","port"]` |
| `-qps N` | no (default 10) | Target average QPS (Poisson rate, aggregate across replicas) |
| `-replicas N` | no (default 1) | Split `-qps` across `N` independent Poisson streams (max 10000) |
| `-duration D` | no (default 1m) | Run length |
| `-method PATH` | no | Override method (default `/example.replay.v1.ReplayService/Score`) |
| `-tls` / `-insecure` | no | TLS on by default; `-insecure` skips cert verify |
| `-cert / -key / -ca` | no | mTLS materials |
| `-connections N` | no | Number of gRPC connections |
| `-workers N` | no (auto) | Sender workers |
| `-queue-capacity N` | no (auto) | Send queue capacity |
| `-request-timeout D` | no (default 30s) | Per-request deadline |
| `-output PATH` | no | Latency CSV (`timestamp_ns,latency_ms,status`) |

### Burst overlay (new)

| Flag | Default | Meaning |
|---|---|---|
| `-burst-size N` | `0` (disabled) | Requests per burst |
| `-burst-window D` | `20ms` | Window over which `N` requests fire |
| `-burst-period D` | `1s` | Mean interval between burst onsets |
| `-burst-shape SHAPE` | `uniform` | `uniform` (evenly spaced), `spike` (all at onset, 1µs apart), `random` (uniform-random offsets in window) |
| `-burst-jitter J` | `fixed` | `fixed` (exact period) or `poisson` (exponential gap, mean = period) |
| `-burst-mode M` | `additive` | `additive`: bursts ride on top of base λ. `absorbing`: base λ reduced so long-run mean QPS == `-qps` |
| `-metrics-jsonl PATH` | unset | NDJSON tick / burst / summary output |
| `-analyzer-window D` | `20ms` | Bin width for sender-side end-of-run analysis |

### Replica Poisson streams

| Flag | Default | Meaning |
|---|---|---|
| `-replicas N` | `1` | Model traffic from `N` independent upstream replicas. `-qps` stays the aggregate target and is split equally, so `-qps 300 -replicas 30` runs 30 streams at 10 QPS each (max 10000). |

Superposing independent replica streams stays Poisson-like in aggregate; use
`-replicas` to model independent sources without changing total mean QPS. The
end-of-run window analysis still covers the aggregate emitted stream. Combine
with the burst overlay to layer bursts on top of a replica-fanned base.

### Takeover mode

| Flag | Meaning |
|---|---|
| `-take-over-replay` | Auto-discover an external replay deployment's artefacts (replay file, identity cert/key, CA) and run high-throughput Poisson replay against `-target` |
| `-install-root PATH` | Override install root (defaults search `/opt/replay/install`, `/var/lib/replay/install`) |
| `-replay-file PATH` | Override replay file under install root |
| `-takeover-cert / -takeover-key / -takeover-ca` | Override identity materials |

Takeover mode forces TLS, sets GOMAXPROCS to NCPU, and bumps connections / workers / queue automatically.

## Workflow 1 — Baseline Poisson replay against a SUT

```bash
./bin/go-replayer \
  -file /path/to/replay.bin \
  -target host:28826 \
  -qps 300 \
  -duration 60s \
  -tls
```

Per-second stdout columns when bursts are off:

```
[   1s] target=300  sent=298   ok=298   err=0   drift=  -2.0 p50=1.2ms p90=2.1ms p99=4.8ms inflight=2 queue=0 sched_block=0.0ms sched_lag=0.5ms
```

End-of-run prints `SUMMARY`, latency quantiles, and a sender-side window analysis (Fano + bin counts at 20ms by default). For pure Poisson, expect Fano ≈ 1.0 and bins ≥30 == 0.

## Workflow 2 — Inject configurable micro-bursts

Reproduce a canonical "30 reqs in 20ms once per second" burst signature while preserving mean QPS:

```bash
./bin/go-replayer \
  -file /path/to/replay.bin \
  -target host:28826 \
  -qps 200 -duration 60s -tls -insecure \
  -burst-size 30 \
  -burst-window 20ms \
  -burst-period 1s \
  -burst-shape spike \
  -burst-mode absorbing \
  -metrics-jsonl /tmp/run.jsonl
```

Per-second stdout adds `bursts=N spikes=N base=N` columns. End-of-run window analysis reports elevated Fano, max bin count, and `>=30:N` threshold crosses. NDJSON emits one `burst_onset` row per onset and a `summary` row with the embedded `window_summary` object.

### Choosing burst shape

| Shape | Spike spacing | When to use |
|---|---|---|
| `uniform` | even across window (`window/N` apart) | Default. Models a bursty *rate window* without infinitesimal stacking. |
| `spike` | all at onset, 1µs apart | Worst-case: stress connection / batcher accept paths with a near-instant clump. |
| `random` | uniform-random offsets in window | Models stochastic clumping on top of a periodic onset trigger. |

### Choosing jitter

| Jitter | Onset spacing | When to use |
|---|---|---|
| `fixed` | exact `--burst-period` | Deterministic — easiest to reason about, easiest to correlate with latency spikes downstream. |
| `poisson` | exponential gap, mean = period | Models naturally jittered burst arrivals (e.g. periodic-but-not-cron-aligned producers). |

### Choosing mode

| Mode | Long-run mean QPS | Effective base λ | When to use |
|---|---|---|---|
| `additive` | `qps + size/period` | `qps` | A/B "what if I just add bursts" — total load increases. |
| `absorbing` | `qps` | `qps - size/period` (floored at 0) | A/B "shape vs no-shape at fixed mean QPS" — isolates burstiness from rate. |

`absorbing` is the right default for SLO regression: the SUT sees the same average load with and without bursts. A config whose burst average (`--burst-size / --burst-period`) exceeds `--qps` cannot be absorbed and is rejected at startup.

## Workflow 3 — Verify burst shape with grpc-gap-target (loopback)

Validate that the sender produces the burst shape you asked for, without trusting the SUT to be honest about arrivals.

```bash
# 1. self-signed cert (one-time)
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout /tmp/key.pem -out /tmp/cert.pem -days 1 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"

# 2. analyser
./bin/grpc-gap-target --addr 127.0.0.1:28890 \
  --cert /tmp/cert.pem --key /tmp/key.pem &
GAP_PID=$!

# 3. replay against loopback
./bin/go-replayer \
  -file /path/to/replay.bin -target 127.0.0.1:28890 \
  -qps 200 -duration 60s -tls -insecure \
  -burst-size 30 -burst-window 20ms -burst-period 1s \
  -burst-shape spike -burst-mode absorbing

# 4. trigger summary print
kill -TERM $GAP_PID
```

`grpc-gap-target` prints:
- Inter-arrival CV (Poisson ideal ≈ 1.0)
- Empirical vs expected exponential quantiles
- `window=Xms threshold>=K observed=A/B (...) expected=... ratio=N maxInWindow=M`

Cross-check the receive-side `maxInWindow` against the sender-side `max` from go-replayer's own end-of-run window analysis. They should agree within a small fraction (some events drift across bin boundaries).

## Workflow 4 — A/B parity between two targets

Use `grpc-parity-check` (not the replayer) for response-byte / float comparison:

```bash
./bin/grpc-parity-check \
  -file /path/to/replay.bin \
  -target-a host-a:28826 -target-b host-b:28826 \
  -qps 100 -duration 60s
```

Reports per-request mismatches and float-diff distribution. Exits non-zero on
any mismatch, and on any RPC error unless `-allow-errors` is set. Byte-different
responses with no comparable floats are counted as mismatches, never silent
passes.

## Workflow 5 — Replay-deployment takeover

When you want to replace an external replay sender with go-replayer on the *same* host (same identity, same replay file):

```bash
./bin/go-replayer -take-over-replay \
  -target $(hostname -f):28826 \
  -qps 300 -duration 60s
```

The replayer auto-discovers `var/identity.cert`, `var/identity.key`, `var/identity-ca.crt`, and `replay_2gb.bin` (or `recorded_requests.bin`) under the configured install roots. Bumps GOMAXPROCS and connection / worker counts for high-throughput hosts.

## Observability surface

### Per-second stdout (always on)

```
[   3s] target=200  sent=197   ok=198   err=0   drift=  -3.0 p50=0.2ms p90=0.3ms p99=0.6ms inflight=8 queue=22 sched_block=0.1ms sched_lag=21.9ms bursts=1 spikes=30 base=167
```

| Column | Meaning |
|---|---|
| `target` | `--qps` setting |
| `sent` | Requests dispatched in the last 1s |
| `ok` / `err` | Successful / failed completions in the last 1s |
| `drift` | `sent - target` — instantaneous pacing drift |
| `p50/90/99` | Latency quantiles for completions in the last 1s (ms) |
| `inflight` | Currently-in-flight requests at sample time |
| `queue` | Send-queue depth at sample time |
| `sched_block` | Total ms the scheduler spent blocked on the send queue this second |
| `sched_lag` | Total ms the scheduler ran behind its scheduled time this second |
| `bursts` | Burst onsets fired in the last 1s (only when bursts are on) |
| `spikes` | Burst spikes dispatched in the last 1s (only when bursts are on) |
| `base` | Poisson-base requests dispatched in the last 1s (only when bursts are on) |

Note: `sent ≈ spikes + base` per second. Drift can be positive when a burst lands inside a 1s window, negative in the second following it (especially with absorbing mode).

### End-of-run sender-side window analysis

```
=== Sender-side window analysis (emitted dispatches) ===
window=20ms bins=499 events=1999 duration=9.98s observed_qps=200.33
mean=4.006 var=18.419 fano=4.598 (Poisson ideal=1.0)
bin_count_quantiles: p50=3 p90=6 p99=32 max=37
tail_bins >=8:18 >=10:10 >=12:9 >=30:9
```

| Metric | Interpretation |
|---|---|
| `fano` | Variance / mean of bin counts. **Poisson ideal ≈ 1.0**. >2 indicates burstiness. |
| `mean` | Average bin count. Equals `qps × analyzer_window / 1s`. |
| `max` | Largest bin in the run — the worst microburst the sender emitted. |
| `p99` (bin counts) | The 99th percentile of bin counts — typical worst-case burst observed. |
| `>=K:N` | N bins held at least K events. Use to check tail-shock policies (`>=30:0` is a clean SLO gate for typical p99-sensitive workloads). |

### NDJSON (`--metrics-jsonl`)

Three event kinds, one per line.

`tick` — per-second snapshot:

```json
{"event":"tick","sec":1,"target_qps":200,"sent":161,"ok":161,"err":0,
 "drift":-39,"p50_ms":0.21,"p90_ms":0.32,"p99_ms":0.63,"inflight":0,
 "queue":0,"sched_blocked_ms":0.10,"sched_lag_ms":4.84,"bursts_fired":1,
 "spikes_fired":1,"base_fired":161,"ts":"2026-05-07T02:53:05.466Z"}
```

`burst_onset` — emitted at each burst materialisation:

```json
{"event":"burst_onset","ts":"2026-05-07T02:53:05.465Z","size":30,
 "window_ms":20,"period_ms":1000,"shape":"spike","jitter":"fixed",
 "mode":"absorbing"}
```

`summary` — final row with `window_summary` embedded:

```json
{"event":"summary","target_qps":200,"effective_base_qps":170,
 "duration":"10s","total_sent":1999,"bursts_fired":9,"spikes_fired":270,
 "base_fired":1729,"window_summary":{"fano":4.60,"max":37,"p99":32,
 "threshold_crosses":[{"threshold":30,"bins":9}]},"latency_ms":{...}}
```

Useful jq incantations:

```bash
# Per-second p99 latency timeseries
jq -r 'select(.event=="tick")|[.sec,.p99_ms]|@tsv' run.jsonl

# All burst onset times (epoch ms)
jq -r 'select(.event=="burst_onset")|.ts' run.jsonl

# Final summary, single row
jq 'select(.event=="summary")' run.jsonl
```

## Generating a synthetic replay file

For local smoke tests where you don't have a real recorded file:

```go
// file: gen_replay.go — usage: go run gen_replay.go OUT N
package main
import ("encoding/binary"; "fmt"; "os")
func main() {
    f, _ := os.Create(os.Args[1])
    defer f.Close()
    method, payload := []byte("/proto.test.Replay/echo"), make([]byte, 256)
    var n int; fmt.Sscanf(os.Args[2], "%d", &n)
    for i := 0; i < n; i++ {
        binary.Write(f, binary.BigEndian, uint32(len(method)))
        f.Write(method)
        binary.Write(f, binary.BigEndian, uint32(len(payload)))
        f.Write(payload)
    }
}
```

`grpc-gap-target` and `grpc-dummy-target` accept any unknown method, so the method string is irrelevant for shape testing.

## Recording a real replay file

Capture a real workload by pointing your gRPC client traffic at a collector target that appends `[len][method][len][payload]` records to disk for the desired duration, then truncate at a record boundary to a target byte size.

## Gotchas

1. **`spike` shape stacks at 1µs spacing.** The kernel + Go runtime cannot honour this on real machines — expect the receive-side bin to show ~95% of `--burst-size`, not 100%. The receive-side `maxInWindow` for the smallest bin (1ms) is the truth.
2. **Absorbing mode floors at 0.** If `size/period > qps`, the base Poisson is silenced and the run is pure-burst. The replayer prints `effective_base_qps=0` so you notice.
3. **Drift is per-second, not cumulative.** A burst dumping 30 requests inside a 20ms window usually appears as `drift=+15..+30` in the second the burst lands, then `drift=-` of similar magnitude the following second under absorbing mode. This is by design; the long-run mean is preserved.
4. **`-target` accepts tuple forms.** `(host,port)`, `["host","port"]`, and bracketed IPv6 all normalise to `host:port`. Useful when pasting from log lines.
5. **Takeover mode forces TLS.** `-tls=false` is ignored under `-take-over-replay`; the deployment's identity cert is mandatory.
6. **The replayer retains all parsed requests in memory, capped at 8 GiB.** `loadRequests` accumulates method+payload bytes plus a 64-byte per-record overhead and aborts with `replay file exceeds the …-byte in-memory cap` once the total exceeds `maxTotalReplayBytes` (8 GiB). A ~7GiB file is fine on a 64GiB takeover pod, but a 10GiB file is rejected at load time. Records with zero-length payloads are skipped at load time and counted in the `Skipped N invalid requests` line.
7. **`grpc-gap-target` only prints summary on signal.** Send `SIGTERM` (or `SIGINT`) to flush the arrival summary to stdout. Killing it with `SIGKILL` loses the summary.
8. **Sender-side window analysis can disagree slightly with receive-side.** The sender records the dispatch time *before* the worker pool grabs the job; the network adds queueing. Differences <5% are normal; >20% indicates real wire-side queueing or scheduler lag — investigate `sched_block` and `sched_lag` columns.
9. **Race detector mode is supported.** `go test -race ./...` passes; counters use `sync/atomic` and the JSONL writer guards encoding with a mutex.
10. **`--metrics-jsonl` is one line per event, unbuffered per encode.** Safe to `tail -f` during long runs. Each `Encode` call flushes a single newline-terminated row.

## Testing the replayer itself

```bash
go test ./...           # parsers, takeover, scheduler, metrics, parity
go test -race ./...     # race detector clean
go vet ./...            # clean
golangci-lint run       # lint clean
```

Scheduler tests cover exact spike placement, additive vs absorbing rate semantics (including fractional base rates), replica stream splitting, deadline correctness, and counter snapshot/drain. Metrics tests cover Fano on periodic vs bursty synthetic streams, NDJSON validity, writer error propagation, and nil-receiver safety on the JSONL writer. The parity checker has tests for protobuf float extraction/comparison, the bounded diff reservoir, no-comparable-floats / unequal-count mismatch handling, and strict replay-file parsing (including huge-method-length rejection and truncation).

## When to use which target

| You want to | Use |
|---|---|
| Smoke-test that the replayer can reach a SUT | `grpc-dummy-target` |
| Verify the replayer's arrival shape on the wire | `grpc-gap-target` |
| Compare two SUT versions for response parity | `grpc-parity-check` |
| Stress a real SUT with controlled bursts | `go-replayer` against the SUT directly |
