package main

import (
	"encoding/binary"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLatencyStatsExactUnderCap(t *testing.T) {
	l := newLatencyStats(rand.New(rand.NewSource(1)))
	for _, v := range []float64{5, 1, 3, 2, 4} {
		l.add(v)
	}
	lo, avg, p50, p90, p99, hi, n := l.summary()
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
	if lo != 1 || hi != 5 {
		t.Fatalf("min/max = %v/%v, want 1/5", lo, hi)
	}
	if avg != 3 {
		t.Fatalf("avg = %v, want 3", avg)
	}
	// Sorted reservoir is [1,2,3,4,5]: q(0.5)=idx2=3, q(0.9)=idx4=5, q(0.99)=idx4=5.
	if p50 != 3 || p90 != 5 || p99 != 5 {
		t.Fatalf("percentiles p50/p90/p99 = %v/%v/%v", p50, p90, p99)
	}
}

func TestLatencyStatsEmpty(t *testing.T) {
	l := newLatencyStats(rand.New(rand.NewSource(1)))
	if _, _, _, _, _, _, n := l.summary(); n != 0 {
		t.Fatalf("empty count = %d", n)
	}
}

func TestLatencyStatsReservoirBounded(t *testing.T) {
	l := newLatencyStats(rand.New(rand.NewSource(1)))
	const extra = 1000
	for i := 0; i < maxLatencySamples+extra; i++ {
		l.add(float64(i%97) + 1)
	}
	if len(l.reservoir) != maxLatencySamples {
		t.Fatalf("reservoir grew past cap: %d", len(l.reservoir))
	}
	if _, _, _, _, _, _, n := l.summary(); n != int64(maxLatencySamples+extra) {
		t.Fatalf("count must stay exact: %d", n)
	}
}

func TestSchedulerAbsorbingFractionalBaseIsExact(t *testing.T) {
	// burst-size 3 over a 2s period => 1.5 qps burst average.
	// Absorbing base must be exactly 10 - 1.5 = 8.5 (old int rounding gave 8).
	cfg := BurstConfig{
		Size: 3, Window: 20 * time.Millisecond, Period: 2 * time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAbsorbing,
	}
	start := time.Unix(0, 0)
	s := NewScheduler(10, cfg, start, start.Add(time.Minute), rand.New(rand.NewSource(1)))
	if got := s.EffectiveBaseRate(); math.Abs(got-8.5) > 1e-9 {
		t.Fatalf("effective base = %v, want 8.5", got)
	}
}

func TestLoadRequestsRejectsAbsurdMethodLen(t *testing.T) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 0xFFFFFFFF)
	p := filepath.Join(t.TempDir(), "bad.bin")
	if err := os.WriteFile(p, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loadRequests(p); err == nil {
		t.Fatal("absurd method-length prefix must error, not attempt a 4GB allocation")
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestJSONLWriterTracksWriteError(t *testing.T) {
	j := NewJSONLWriter(errWriter{})
	j.Emit(map[string]any{"a": 1})
	if j.Err() == nil {
		t.Fatal("expected Err() to surface the write failure")
	}
}
