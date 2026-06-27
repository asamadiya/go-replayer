package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"
)

// WindowAnalyzer records dispatch timestamps emitted by the sender (NOT
// arrivals at the target) and computes the same burstiness metrics
// grpc-gap-target reports for the receive side: Fano factor, max bin
// count, and threshold-crossing histograms over fixed-size windows.
//
// This lets a sender prove its own emission shape without relying on a
// receive-side analyzer.
// analyzerSampleCap bounds how many dispatch timestamps the analyzer retains
// so very long or high-QPS runs stay memory-bounded. Beyond the cap, samples
// are dropped and counted; shape statistics then describe the captured prefix.
const analyzerSampleCap = 5_000_000

type WindowAnalyzer struct {
	mu       sync.Mutex
	tsNanos  []int64
	windowNs int64
	dropped  int64
}

// NewWindowAnalyzer creates an analyzer with the given bin width.
func NewWindowAnalyzer(window time.Duration) *WindowAnalyzer {
	return &WindowAnalyzer{
		tsNanos:  make([]int64, 0, 1<<14),
		windowNs: int64(window),
	}
}

// Record adds an emitted dispatch timestamp, up to analyzerSampleCap.
func (a *WindowAnalyzer) Record(t time.Time) {
	a.mu.Lock()
	if len(a.tsNanos) < analyzerSampleCap {
		a.tsNanos = append(a.tsNanos, t.UnixNano())
	} else {
		a.dropped++
	}
	a.mu.Unlock()
}

// Dropped returns how many timestamps were discarded after the cap was hit.
func (a *WindowAnalyzer) Dropped() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dropped
}

// Snapshot returns the underlying timestamps in dispatch order.
func (a *WindowAnalyzer) Snapshot() []int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]int64, len(a.tsNanos))
	copy(out, a.tsNanos)
	return out
}

// WindowSummary captures per-window burstiness statistics.
type WindowSummary struct {
	Window           time.Duration  `json:"window"`
	NumBins          int            `json:"num_bins"`
	NumEvents        int            `json:"num_events"`
	DurationSec      float64        `json:"duration_sec"`
	ObservedQPS      float64        `json:"observed_qps"`
	MeanCount        float64        `json:"mean_count"`
	VarCount         float64        `json:"var_count"`
	Fano             float64        `json:"fano"` // Var/Mean — Poisson ideal ≈ 1.0
	Max              int            `json:"max"`  // largest count in any bin
	P50              int            `json:"p50"`  // bin-count quantiles
	P90              int            `json:"p90"`
	P99              int            `json:"p99"`
	ThresholdCrosses []ThresholdHit `json:"threshold_crosses"`
}

// ThresholdHit records how many bins met or exceeded a given count.
type ThresholdHit struct {
	Threshold int `json:"threshold"`
	Bins      int `json:"bins"`
}

// Summary computes the WindowSummary using the provided bin width and
// thresholds (e.g. 8, 10, 12, 30 to characterise tail micro-bursts).
func (a *WindowAnalyzer) Summary(window time.Duration, thresholds []int) WindowSummary {
	ts := a.Snapshot()
	if window <= 0 || len(ts) == 0 {
		return WindowSummary{Window: window}
	}
	// Sort defensively — emitted timestamps SHOULD be ordered already.
	sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })

	first, last := ts[0], ts[len(ts)-1]
	winNs := int64(window)
	numBins := int((last-first)/winNs) + 1
	if numBins < 1 {
		numBins = 1
	}
	counts := make([]int, numBins)
	for _, t := range ts {
		idx := int((t - first) / winNs)
		if idx >= numBins {
			idx = numBins - 1
		}
		counts[idx]++
	}
	// First-pass mean/variance.
	var sum, sumSq float64
	maxCnt := 0
	for _, c := range counts {
		f := float64(c)
		sum += f
		sumSq += f * f
		if c > maxCnt {
			maxCnt = c
		}
	}
	n := float64(numBins)
	mean := sum / n
	variance := sumSq/n - mean*mean
	if variance < 0 {
		variance = 0
	}
	fano := math.NaN()
	if mean > 0 {
		fano = variance / mean
	}

	// Quantiles over bin counts.
	sorted := append([]int(nil), counts...)
	sort.Ints(sorted)
	q := func(p float64) int {
		idx := int(p * float64(numBins-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= numBins {
			idx = numBins - 1
		}
		return sorted[idx]
	}

	hits := make([]ThresholdHit, 0, len(thresholds))
	for _, th := range thresholds {
		bins := 0
		for _, c := range counts {
			if c >= th {
				bins++
			}
		}
		hits = append(hits, ThresholdHit{Threshold: th, Bins: bins})
	}

	durSec := float64(last-first) / float64(time.Second)
	observedQPS := 0.0
	if durSec > 0 {
		observedQPS = float64(len(ts)) / durSec
	}

	return WindowSummary{
		Window:           window,
		NumBins:          numBins,
		NumEvents:        len(ts),
		DurationSec:      durSec,
		ObservedQPS:      observedQPS,
		MeanCount:        mean,
		VarCount:         variance,
		Fano:             fano,
		Max:              maxCnt,
		P50:              q(0.50),
		P90:              q(0.90),
		P99:              q(0.99),
		ThresholdCrosses: hits,
	}
}

// FormatWindowSummary returns a multi-line human-readable string. Used at
// end-of-run alongside the existing latency summary.
func FormatWindowSummary(label string, s WindowSummary) string {
	lines := []string{
		fmt.Sprintf("=== Sender-side window analysis (%s) ===", label),
		fmt.Sprintf(
			"window=%s bins=%d events=%d duration=%.2fs observed_qps=%.2f",
			s.Window, s.NumBins, s.NumEvents, s.DurationSec, s.ObservedQPS,
		),
		fmt.Sprintf(
			"mean=%.3f var=%.3f fano=%.3f (Poisson ideal=1.0)",
			s.MeanCount, s.VarCount, s.Fano,
		),
		fmt.Sprintf("bin_count_quantiles: p50=%d p90=%d p99=%d max=%d", s.P50, s.P90, s.P99, s.Max),
	}
	if len(s.ThresholdCrosses) > 0 {
		parts := make([]string, 0, len(s.ThresholdCrosses))
		for _, h := range s.ThresholdCrosses {
			parts = append(parts, fmt.Sprintf(">=%d:%d", h.Threshold, h.Bins))
		}
		lines = append(lines, "tail_bins "+joinWithSep(parts, " "))
	}
	out := ""
	for i, l := range lines {
		if i > 0 {
			out += "\n"
		}
		out += l
	}
	return out
}

func joinWithSep(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out += sep + p
	}
	return out
}

// JSONLWriter emits structured per-second snapshots, burst onset events,
// and a final summary as newline-delimited JSON for offline analysis.
//
// All writes are guarded by an internal mutex so emitters from multiple
// goroutines are serialised safely.
type JSONLWriter struct {
	mu       sync.Mutex
	w        io.Writer
	enc      *json.Encoder
	firstErr error
}

// NewJSONLWriter wraps w with line-flushing JSON encoding.
func NewJSONLWriter(w io.Writer) *JSONLWriter {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &JSONLWriter{w: w, enc: enc}
}

// OpenJSONLWriter creates the file at path and returns a writer plus a close func.
func OpenJSONLWriter(path string) (*JSONLWriter, func() error, error) {
	if path == "" {
		return nil, func() error { return nil }, nil
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("opening jsonl output %s: %w", path, err)
	}
	return NewJSONLWriter(f), f.Close, nil
}

// Emit writes one structured row. Missing writers are no-ops so the
// caller can stay branch-free. The first encode/write error is retained and
// surfaced via Err so the caller can report lost telemetry at shutdown.
func (j *JSONLWriter) Emit(row map[string]any) {
	if j == nil {
		return
	}
	j.mu.Lock()
	if err := j.enc.Encode(row); err != nil && j.firstErr == nil {
		j.firstErr = err
	}
	j.mu.Unlock()
}

// Err returns the first encode/write error encountered, or nil. Safe on a nil
// writer.
func (j *JSONLWriter) Err() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.firstErr
}

// EmitTick is the canonical per-second telemetry row.
type EmitTick struct {
	TS                time.Time
	Sec               int
	TargetQPS         int
	Replicas          int
	PerReplicaBaseQPS float64
	Sent              int64
	OK                int64
	Err               int64
	Drift             float64
	P50               float64
	P90               float64
	P99               float64
	Inflight          int64
	Queue             int
	SchedBlockedMs    float64
	SchedLagMs        float64
	BurstsFired       int64
	SpikesFired       int64
	BaseFired         int64
}

// Write encodes the tick into JSONL.
func (j *JSONLWriter) WriteTick(t EmitTick) {
	if j == nil {
		return
	}
	j.Emit(map[string]any{
		"event":                "tick",
		"ts":                   t.TS.UTC().Format(time.RFC3339Nano),
		"sec":                  t.Sec,
		"target_qps":           t.TargetQPS,
		"replicas":             t.Replicas,
		"per_replica_base_qps": t.PerReplicaBaseQPS,
		"sent":                 t.Sent,
		"ok":                   t.OK,
		"err":                  t.Err,
		"drift":                t.Drift,
		"p50_ms":               t.P50,
		"p90_ms":               t.P90,
		"p99_ms":               t.P99,
		"inflight":             t.Inflight,
		"queue":                t.Queue,
		"sched_blocked_ms":     t.SchedBlockedMs,
		"sched_lag_ms":         t.SchedLagMs,
		"bursts_fired":         t.BurstsFired,
		"spikes_fired":         t.SpikesFired,
		"base_fired":           t.BaseFired,
	})
}

// WriteBurstEvent emits an onset event when a burst is generated.
func (j *JSONLWriter) WriteBurstEvent(at time.Time, cfg BurstConfig) {
	if j == nil {
		return
	}
	j.Emit(map[string]any{
		"event":     "burst_onset",
		"ts":        at.UTC().Format(time.RFC3339Nano),
		"size":      cfg.Size,
		"window_ms": float64(cfg.Window) / float64(time.Millisecond),
		"period_ms": float64(cfg.Period) / float64(time.Millisecond),
		"shape":     string(cfg.Shape),
		"jitter":    string(cfg.Jitter),
		"mode":      string(cfg.Mode),
	})
}

// WriteSummary emits the end-of-run summary.
func (j *JSONLWriter) WriteSummary(row map[string]any) {
	if j == nil {
		return
	}
	row["event"] = "summary"
	j.Emit(row)
}
