# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Continuous integration workflow (build, vet, gofmt check, race tests, lint).
- `golangci-lint` configuration, `Makefile`, `CONTRIBUTING.md`, and package
  documentation.
- Unit tests for protobuf float extraction and comparison in
  `cmd/grpc-parity-check`.

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
