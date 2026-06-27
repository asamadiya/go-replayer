# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Continuous integration workflow (build, vet, gofmt check, race tests, lint)
  with a coverage floor.
- `golangci-lint` configuration, `Makefile`, `CONTRIBUTING.md`, and package
  documentation.
- Unit tests for protobuf comparison, replay-file parsing, the bounded latency
  reservoir, scheduler absorbing precision, and the gap/dummy targets.

### Fixed
- `grpc-parity-check` no longer falsely certifies byte-different responses that
  contain no comparable floats or a differing number of floats.
- Absorbing burst mode now preserves `--qps` exactly using a float64 base rate,
  and rejects configurations whose burst average exceeds the target QPS.
- Replay-file parsers reject malformed length prefixes instead of attempting
  unbounded allocations; the parity parser errors on truncated/empty input.
- `grpc-parity-check` validates `--qps > 0` (previously a divide-by-zero panic)
  and checks CSV output-file creation.
- JSONL metrics writer surfaces the first write error instead of dropping it.

### Changed
- Replaced unbounded per-request latency and CSV buffers with streaming CSV
  writes and a bounded reservoir sample, and capped the window analyzer's
  retained timestamps, so long / high-QPS runs stay memory-bounded.
- Migrated deprecated `grpc.Dial`/`grpc.DialContext` to `grpc.NewClient`.

## [0.2.0]

### Added
- `--replicas` flag: split aggregate `--qps` across up to 10000 independent
  upstream-replica Poisson streams.
- Configurable micro-burst overlay (`--burst-size`, `--burst-window`,
  `--burst-period`, `--burst-shape`, `--burst-jitter`, `--burst-mode`) with
  additive and absorbing rate modes.
- Deadline-driven Poisson scheduler with per-replica streams.
- Sender-side end-of-run window analysis (Fano factor, threshold-crossing bins).
- NDJSON metrics output (`--metrics-jsonl`): per-second ticks, burst-onset
  events, and a final summary.
- `SKILL.md` operational guide.

## [0.1.0]

### Added
- Initial high-throughput Poisson gRPC replay sender.
- Takeover mode (`--take-over-replay`) for external replay deployments.
- Companion target servers: `grpc-dummy-target`, `grpc-gap-target`,
  `grpc-parity-check`.
