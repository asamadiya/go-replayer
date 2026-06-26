package main

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestWindowAnalyzer_PoissonLikeFanoNearOne(t *testing.T) {
	// Synthesise a uniform 1ms-spaced stream → variance should be near 0
	// (regular grid). This sanity-checks that a perfectly periodic stream
	// has Fano factor near 0 (vs Poisson ideal ~ 1).
	a := NewWindowAnalyzer(20 * time.Millisecond)
	start := time.Unix(0, 0)
	for i := 0; i < 1000; i++ {
		a.Record(start.Add(time.Duration(i) * time.Millisecond))
	}
	s := a.Summary(20*time.Millisecond, []int{8, 10, 12, 30})
	if s.NumEvents != 1000 {
		t.Fatalf("expected 1000 events, got %d", s.NumEvents)
	}
	// Bins are 20ms wide → 50 bins, 20 events each → mean=20, var≈0.
	if math.Abs(s.MeanCount-20) > 0.5 {
		t.Errorf("mean=%v, want ~20", s.MeanCount)
	}
	if s.Fano > 0.05 {
		t.Errorf("fano=%v should be near 0 for periodic stream", s.Fano)
	}
	if s.Max != 20 && s.Max != 21 {
		t.Errorf("expected max bin count ~20, got %d", s.Max)
	}
}

func TestWindowAnalyzer_DetectsBurst(t *testing.T) {
	// Pack 30 events into a 10ms window, then 30 events spread over 990ms.
	// 1s of traffic, 20ms bins → 50 bins.
	// The first bin captures all 30 burst events → high Fano.
	a := NewWindowAnalyzer(20 * time.Millisecond)
	start := time.Unix(0, 0)
	// Burst: 30 packed events
	for i := 0; i < 30; i++ {
		a.Record(start.Add(time.Duration(i*100) * time.Microsecond))
	}
	// Background: 30 spread events
	for i := 0; i < 30; i++ {
		a.Record(start.Add(20*time.Millisecond + time.Duration(i)*30*time.Millisecond))
	}
	s := a.Summary(20*time.Millisecond, []int{10, 30})
	if s.Max < 30 {
		t.Errorf("expected max bin >= 30 (the burst), got %d", s.Max)
	}
	if s.Fano < 5 {
		t.Errorf("expected Fano >> 1 for burst, got %v", s.Fano)
	}
	// Find the >=30 threshold result
	var found bool
	for _, h := range s.ThresholdCrosses {
		if h.Threshold == 30 {
			found = true
			if h.Bins < 1 {
				t.Errorf("expected >=1 bin at threshold 30, got %d", h.Bins)
			}
		}
	}
	if !found {
		t.Error("threshold 30 not present in output")
	}
}

func TestWindowAnalyzer_EmptyStream(t *testing.T) {
	a := NewWindowAnalyzer(20 * time.Millisecond)
	s := a.Summary(20*time.Millisecond, []int{10})
	if s.NumEvents != 0 || s.NumBins != 0 {
		t.Errorf("empty stream should have zero counts, got %+v", s)
	}
}

func TestJSONLWriter_EmitsValidNDJSON(t *testing.T) {
	var buf bytes.Buffer
	j := NewJSONLWriter(&buf)
	j.WriteTick(EmitTick{
		TS:  time.Unix(1700000000, 0),
		Sec: 1, TargetQPS: 100, Replicas: 2, PerReplicaBaseQPS: 45.5, Sent: 99, OK: 99, Err: 0,
		P50: 1.2, P90: 2.3, P99: 5.4, BurstsFired: 1, SpikesFired: 30,
	})
	j.WriteBurstEvent(time.Unix(1700000000, 500000000), BurstConfig{
		Size: 30, Window: 20 * time.Millisecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive,
	})
	j.WriteSummary(map[string]any{"total_sent": 5000})

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d: %q", len(lines), buf.String())
	}
	for i, ln := range lines {
		var row map[string]any
		if err := json.Unmarshal([]byte(ln), &row); err != nil {
			t.Fatalf("line %d not valid JSON: %v\n%s", i, err, ln)
		}
		if _, ok := row["event"]; !ok {
			t.Errorf("line %d missing event field: %s", i, ln)
		}
	}
	// Sanity: tick.spikes_fired present.
	var first map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &first)
	if first["event"] != "tick" || first["spikes_fired"].(float64) != 30 {
		t.Errorf("first line malformed: %+v", first)
	}
	if v, _ := json.Marshal(first); !strings.Contains(string(v), "p99_ms") {
		t.Error("expected p99_ms in tick row")
	}
	if first["per_replica_base_qps"].(float64) != 45.5 {
		t.Errorf("expected per_replica_base_qps in tick row, got %+v", first)
	}
	if _, ok := first["per_replica_qps"]; ok {
		t.Error("did not expect ambiguous per_replica_qps in tick row")
	}
}

func TestFormatWindowSummary_HumanReadable(t *testing.T) {
	s := WindowSummary{
		Window: 20 * time.Millisecond, NumBins: 50, NumEvents: 1000,
		DurationSec: 1.0, ObservedQPS: 1000,
		MeanCount: 20, VarCount: 4, Fano: 0.2,
		Max: 25, P50: 20, P90: 23, P99: 25,
		ThresholdCrosses: []ThresholdHit{{Threshold: 30, Bins: 0}},
	}
	out := FormatWindowSummary("test", s)
	for _, want := range []string{"window=", "fano=", "p99=", ">=30:0"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in output:\n%s", want, out)
		}
	}
}

func TestJSONLWriter_NilSafe(t *testing.T) {
	var j *JSONLWriter // nil
	// Must not panic.
	j.WriteTick(EmitTick{})
	j.WriteBurstEvent(time.Unix(0, 0), BurstConfig{})
	j.WriteSummary(map[string]any{"foo": 1})
	j.Emit(map[string]any{"foo": 1})
}
