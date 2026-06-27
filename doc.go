// Command go-replayer is a high-throughput gRPC replay tool with Poisson
// pacing, an optional micro-burst overlay, independent replica streams, and
// sender-side burst-shape analysis.
//
// It reads length-prefixed protobuf request payloads from a replay file and
// dispatches them as unary gRPC calls to a target, modeling realistic arrival
// processes for tail-latency-sensitive load testing. The companion target
// servers under cmd/ provide functional, burst-shape, and parity validation.
//
// See README.md and SKILL.md for usage and operational guidance.
package main
