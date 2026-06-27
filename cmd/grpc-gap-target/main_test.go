package main

import (
	"math"
	"testing"
	"time"
)

func TestQuantile(t *testing.T) {
	if !math.IsNaN(quantile(nil, 0.5)) {
		t.Fatal("empty input should be NaN")
	}
	s := []float64{1, 2, 3, 4, 5}
	if quantile(s, 0) != 1 {
		t.Errorf("p0 = %v, want 1", quantile(s, 0))
	}
	if quantile(s, 1) != 5 {
		t.Errorf("p1 = %v, want 5", quantile(s, 1))
	}
	if got := quantile(s, 0.5); math.Abs(got-3) > 1e-9 {
		t.Errorf("p50 = %v, want 3", got)
	}
	if got := quantile(s, 0.25); math.Abs(got-2) > 1e-9 {
		t.Errorf("p25 = %v, want 2", got)
	}
}

func TestExpectedExponentialQuantile(t *testing.T) {
	if !math.IsNaN(expectedExponentialQuantile(0, 0.5)) {
		t.Error("rate 0 should be NaN")
	}
	if !math.IsNaN(expectedExponentialQuantile(1, 0)) {
		t.Error("p 0 should be NaN")
	}
	// Median of Exp(rate=1) is ln(2).
	if got := expectedExponentialQuantile(1, 0.5); math.Abs(got-math.Ln2) > 1e-9 {
		t.Errorf("median = %v, want ln2", got)
	}
}

func TestPoissonTailProbAtLeast(t *testing.T) {
	if poissonTailProbAtLeast(5, 0) != 1 {
		t.Error("k=0 must be 1")
	}
	if poissonTailProbAtLeast(0, 3) != 0 {
		t.Error("mu=0, k>0 must be 0")
	}
	// P(X >= 1) for mu=1 is 1 - e^-1.
	if got := poissonTailProbAtLeast(1, 1); math.Abs(got-(1-math.Exp(-1))) > 1e-9 {
		t.Errorf("P(X>=1; mu=1) = %v", got)
	}
	if poissonTailProbAtLeast(3, 2) <= poissonTailProbAtLeast(3, 5) {
		t.Error("tail prob must decrease as k grows")
	}
}

func TestAnalyzeWindows(t *testing.T) {
	if nw, ob, mx := analyzeWindows(nil, time.Millisecond, 1); nw != 0 || ob != 0 || mx != 0 {
		t.Fatalf("empty input: got (%d,%d,%d)", nw, ob, mx)
	}
	base := time.Unix(0, 0).UnixNano()
	ms := int64(time.Millisecond)
	// Five arrivals packed in the first 1ms window, one in a later window.
	arr := []int64{base, base + 1, base + 2, base + 3, base + 4, base + ms + 10}
	nw, ob, mx := analyzeWindows(arr, time.Millisecond, 3)
	if mx != 5 {
		t.Errorf("maxInWindow = %d, want 5", mx)
	}
	if ob < 1 {
		t.Errorf("observedBurstWindows = %d, want >= 1", ob)
	}
	if nw < 2 {
		t.Errorf("numWindows = %d, want >= 2", nw)
	}
}

func TestStatsRecordSnapshot(t *testing.T) {
	var s stats
	s.recordArrival(time.Unix(0, 5))
	s.recordArrival(time.Unix(0, 9))
	snap := s.snapshot()
	if len(snap) != 2 || snap[0] != 5 || snap[1] != 9 {
		t.Fatalf("snapshot = %v", snap)
	}
	// snapshot must be a copy, not alias internal state.
	snap[0] = 999
	if s.snapshot()[0] != 5 {
		t.Error("snapshot should return a copy")
	}
}

func TestRawCodecGap(t *testing.T) {
	var c rawCodec
	b, err := c.Marshal([]byte("hi"))
	if err != nil || string(b) != "hi" {
		t.Fatalf("marshal: %v %q", err, b)
	}
	var out []byte
	if err := c.Unmarshal([]byte("yo"), &out); err != nil || string(out) != "yo" {
		t.Fatalf("unmarshal: %v %q", err, out)
	}
	if c.Name() != "proto" {
		t.Errorf("name = %q", c.Name())
	}
}
