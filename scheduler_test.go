package main

import (
	"math/rand"
	"sort"
	"testing"
	"time"
)

// drainScheduler runs Next until exhaustion and returns ordered arrival times
// labelled by kind. Time bookkeeping is virtual — tests do not sleep.
func drainScheduler(t *testing.T, s *Scheduler) ([]time.Time, []ArrivalKind) {
	t.Helper()
	var (
		times []time.Time
		kinds []ArrivalKind
	)
	for i := 0; ; i++ {
		if i > 1_000_000 {
			t.Fatalf("scheduler did not terminate within bound")
		}
		ts, kind, ok := s.Next()
		if !ok {
			break
		}
		times = append(times, ts)
		kinds = append(kinds, kind)
	}
	return times, kinds
}

func TestScheduler_PoissonOnly_ApproxRate(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	start := time.Unix(0, 0)
	deadline := start.Add(10 * time.Second)
	s := NewScheduler(100, BurstConfig{}, start, deadline, rng)
	times, kinds := drainScheduler(t, s)
	for _, k := range kinds {
		if k != ArrivalBase {
			t.Fatalf("expected only base arrivals, saw %v", k)
		}
	}
	// Mean QPS = 100 over 10s → expected 1000 events. Allow generous noise.
	if len(times) < 850 || len(times) > 1150 {
		t.Errorf("expected ~1000 events, got %d", len(times))
	}
}

func TestScheduler_ReplicasSplitRateAndPreserveAggregateQPS(t *testing.T) {
	rng := rand.New(rand.NewSource(17))
	start := time.Unix(0, 0)
	deadline := start.Add(60 * time.Second)
	s := NewSchedulerWithReplicas(300, 30, BurstConfig{}, start, deadline, rng)
	if s.Replicas() != 30 {
		t.Fatalf("expected 30 replicas, got %d", s.Replicas())
	}
	if s.EffectiveBaseRate() != 300 {
		t.Fatalf("expected aggregate base rate 300, got %d", s.EffectiveBaseRate())
	}
	if got := s.PerReplicaBaseRate(); got != 10 {
		t.Fatalf("expected per-replica base rate 10, got %v", got)
	}
	times, kinds := drainScheduler(t, s)
	for _, k := range kinds {
		if k != ArrivalBase {
			t.Fatalf("expected only base arrivals, saw %v", k)
		}
	}
	// Mean QPS = 300 over 60s → expected 18000 events. Allow Poisson noise.
	if len(times) < 17400 || len(times) > 18600 {
		t.Errorf("expected ~18000 events, got %d", len(times))
	}
}

func TestScheduler_ReplicasAllowFractionalPerReplicaRate(t *testing.T) {
	s := NewSchedulerWithReplicas(
		100,
		30,
		BurstConfig{},
		time.Unix(0, 0),
		time.Unix(0, 0).Add(time.Second),
		rand.New(rand.NewSource(19)),
	)
	if s.Replicas() != 30 {
		t.Fatalf("expected 30 replicas, got %d", s.Replicas())
	}
	if got := s.PerReplicaBaseRate(); got < 3.333 || got > 3.334 {
		t.Fatalf("expected fractional per-replica rate around 3.333, got %v", got)
	}
}

func TestScheduler_DisabledBurstIsNoOp(t *testing.T) {
	rng := rand.New(rand.NewSource(11))
	start := time.Unix(0, 0)
	deadline := start.Add(2 * time.Second)
	cfg := BurstConfig{Size: 0, Period: time.Second, Window: 20 * time.Millisecond,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive}
	s := NewScheduler(50, cfg, start, deadline, rng)
	if s.EffectiveBaseRate() != 50 {
		t.Errorf("disabled burst must not reduce base rate, got %d", s.EffectiveBaseRate())
	}
	_, kinds := drainScheduler(t, s)
	for _, k := range kinds {
		if k == ArrivalSpike {
			t.Fatalf("disabled burst yielded a spike")
		}
	}
}

func TestScheduler_BurstUniform_ExactSpikePlacement(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	start := time.Unix(0, 0)
	// 2 second deadline, period 500ms → onsets at 500ms, 1000ms, 1500ms.
	// Onset at 2000ms is at deadline, not strictly before → not fired.
	deadline := start.Add(2 * time.Second)
	cfg := BurstConfig{
		Size:   4,
		Window: 100 * time.Millisecond,
		Period: 500 * time.Millisecond,
		Shape:  BurstUniform,
		Jitter: BurstFixed,
		Mode:   BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rng) // pure burst, no Poisson noise
	times, kinds := drainScheduler(t, s)

	if len(times) != 12 {
		t.Fatalf("expected 12 spikes (3 bursts × 4), got %d", len(times))
	}
	for i, k := range kinds {
		if k != ArrivalSpike {
			t.Errorf("event %d: expected spike, got %v", i, k)
		}
	}
	// Burst 0 onset = 500ms; uniform 4 spikes in 100ms window: 500, 525, 550, 575ms.
	expected := []time.Duration{
		500 * time.Millisecond, 525 * time.Millisecond, 550 * time.Millisecond, 575 * time.Millisecond,
		1000 * time.Millisecond, 1025 * time.Millisecond, 1050 * time.Millisecond, 1075 * time.Millisecond,
		1500 * time.Millisecond, 1525 * time.Millisecond, 1550 * time.Millisecond, 1575 * time.Millisecond,
	}
	for i, want := range expected {
		got := times[i].Sub(start)
		if got != want {
			t.Errorf("spike %d: got %v, want %v", i, got, want)
		}
	}
}

func TestScheduler_BurstSpike_ShapeStacksAtOnset(t *testing.T) {
	start := time.Unix(0, 0)
	deadline := start.Add(2 * time.Second)
	cfg := BurstConfig{
		Size: 5, Window: 50 * time.Millisecond, Period: time.Second,
		Shape: BurstSpike, Jitter: BurstFixed, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rand.New(rand.NewSource(0)))
	times, _ := drainScheduler(t, s)
	if len(times) != 5 {
		t.Fatalf("expected 5 spikes (1 burst × 5), got %d", len(times))
	}
	// Spike shape: all at onset, 1µs apart (deterministic ordering).
	for i := 0; i < 5; i++ {
		want := time.Second + time.Duration(i)*time.Microsecond
		if got := times[i].Sub(start); got != want {
			t.Errorf("spike %d: got %v, want %v", i, got, want)
		}
	}
}

func TestScheduler_BurstRandom_StaysInsideWindow(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	start := time.Unix(0, 0)
	deadline := start.Add(2 * time.Second)
	cfg := BurstConfig{
		Size: 50, Window: 30 * time.Millisecond, Period: time.Second,
		Shape: BurstRandom, Jitter: BurstFixed, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rng)
	times, _ := drainScheduler(t, s)
	if len(times) != 50 {
		t.Fatalf("expected 50 spikes, got %d", len(times))
	}
	onset := time.Second
	for _, ts := range times {
		off := ts.Sub(start) - onset
		if off < 0 || off >= 30*time.Millisecond {
			t.Errorf("random spike offset %v outside [0,30ms)", off)
		}
	}
	// Must be sorted ascending.
	if !sort.SliceIsSorted(times, func(i, j int) bool { return times[i].Before(times[j]) }) {
		t.Error("random spikes are not in ascending order")
	}
}

func TestScheduler_AbsorbingMode_ReducesBaseRate(t *testing.T) {
	rng := rand.New(rand.NewSource(13))
	start := time.Unix(0, 0)
	deadline := start.Add(20 * time.Second)
	cfg := BurstConfig{
		// 30 reqs / 1s = 30 QPS contribution
		Size: 30, Window: 20 * time.Millisecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAbsorbing,
	}
	s := NewScheduler(100, cfg, start, deadline, rng)
	if s.EffectiveBaseRate() != 70 {
		t.Errorf("expected base rate 70 (100 - 30), got %d", s.EffectiveBaseRate())
	}
	times, kinds := drainScheduler(t, s)
	var spikes, base int
	for _, k := range kinds {
		if k == ArrivalSpike {
			spikes++
		} else {
			base++
		}
	}
	// 19 bursts × 30 = 570 spikes (last onset at 20s = deadline, not fired).
	if spikes != 19*30 {
		t.Errorf("expected %d spikes, got %d", 19*30, spikes)
	}
	// Base ~ 70 QPS × 20s = 1400; allow generous noise.
	if base < 1200 || base > 1600 {
		t.Errorf("expected ~1400 base events, got %d", base)
	}
	// Total ~ 100 QPS × 20s = 2000 (target preserved).
	if len(times) < 1800 || len(times) > 2200 {
		t.Errorf("expected ~2000 total events, got %d", len(times))
	}
}

func TestScheduler_AbsorbingMode_ZeroFloor(t *testing.T) {
	// burst contribution exceeds target → base rate floored at 0
	rng := rand.New(rand.NewSource(0))
	start := time.Unix(0, 0)
	deadline := start.Add(time.Second)
	cfg := BurstConfig{
		Size: 200, Window: 10 * time.Millisecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAbsorbing,
	}
	s := NewScheduler(50, cfg, start, deadline, rng)
	if s.EffectiveBaseRate() != 0 {
		t.Errorf("expected base rate floored at 0, got %d", s.EffectiveBaseRate())
	}
}

func TestScheduler_FixedJitter_PeriodicOnsets(t *testing.T) {
	start := time.Unix(0, 0)
	deadline := start.Add(5*time.Second + time.Millisecond)
	cfg := BurstConfig{
		Size: 1, Window: time.Microsecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rand.New(rand.NewSource(0)))
	times, _ := drainScheduler(t, s)
	if len(times) != 5 {
		t.Fatalf("expected 5 onsets at 1s,2s,3s,4s,5s, got %d", len(times))
	}
	for i, ts := range times {
		want := time.Duration(i+1) * time.Second
		if ts.Sub(start) != want {
			t.Errorf("onset %d: got %v, want %v", i, ts.Sub(start), want)
		}
	}
}

func TestScheduler_PoissonJitter_MeanInterval(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	start := time.Unix(0, 0)
	deadline := start.Add(500 * time.Second)
	cfg := BurstConfig{
		Size: 1, Window: time.Microsecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstPoisson, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rng)
	times, _ := drainScheduler(t, s)
	if len(times) < 350 || len(times) > 700 {
		t.Errorf("expected ~500 Poisson-jittered onsets, got %d", len(times))
	}
	// Mean inter-onset interval should be close to 1s.
	if len(times) >= 2 {
		var sum time.Duration
		for i := 1; i < len(times); i++ {
			sum += times[i].Sub(times[i-1])
		}
		mean := sum / time.Duration(len(times)-1)
		if mean < 800*time.Millisecond || mean > 1200*time.Millisecond {
			t.Errorf("expected mean inter-onset ~1s, got %v", mean)
		}
	}
}

func TestScheduler_SnapshotAndDrainCounters(t *testing.T) {
	start := time.Unix(0, 0)
	deadline := start.Add(3 * time.Second)
	cfg := BurstConfig{
		Size: 2, Window: 10 * time.Millisecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rand.New(rand.NewSource(0)))
	for {
		if _, _, ok := s.Next(); !ok {
			break
		}
	}
	totals := s.SnapshotCounters()
	// 2 onsets (at 1s and 2s), 2 spikes each = 4 spikes. Onset at 3s == deadline.
	if totals.BurstsFired != 2 {
		t.Errorf("expected 2 bursts, got %d", totals.BurstsFired)
	}
	if totals.SpikesFired != 4 {
		t.Errorf("expected 4 spikes, got %d", totals.SpikesFired)
	}
	// Drain should match snapshot first time.
	d := s.DrainCounters()
	if d.BurstsFired != 2 || d.SpikesFired != 4 {
		t.Errorf("drain mismatch: %+v", d)
	}
	// Drain twice = 0.
	d2 := s.DrainCounters()
	if d2.BurstsFired != 0 || d2.SpikesFired != 0 {
		t.Errorf("expected 0 after second drain, got %+v", d2)
	}
}

func TestScheduler_OnBurstSpawned_FiresWithCorrectTime(t *testing.T) {
	start := time.Unix(0, 0)
	deadline := start.Add(3 * time.Second)
	cfg := BurstConfig{
		Size: 3, Window: 10 * time.Millisecond, Period: time.Second,
		Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive,
	}
	s := NewScheduler(0, cfg, start, deadline, rand.New(rand.NewSource(0)))
	var seen []time.Time
	s.OnBurstSpawned = func(at time.Time, _ BurstConfig) { seen = append(seen, at) }
	for {
		if _, _, ok := s.Next(); !ok {
			break
		}
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 onset events, got %d", len(seen))
	}
	for i, ts := range seen {
		want := time.Duration(i+1) * time.Second
		if ts.Sub(start) != want {
			t.Errorf("onset %d: got %v, want %v", i, ts.Sub(start), want)
		}
	}
}

func TestParseBurstShape(t *testing.T) {
	cases := []struct {
		in  string
		out BurstShape
		err bool
	}{
		{"uniform", BurstUniform, false},
		{"  Uniform  ", BurstUniform, false},
		{"", BurstUniform, false},
		{"spike", BurstSpike, false},
		{"random", BurstRandom, false},
		{"weird", "", true},
	}
	for _, c := range cases {
		got, err := parseBurstShape(c.in)
		if (err != nil) != c.err {
			t.Errorf("parseBurstShape(%q): err=%v, want err=%v", c.in, err, c.err)
		}
		if !c.err && got != c.out {
			t.Errorf("parseBurstShape(%q): got %v, want %v", c.in, got, c.out)
		}
	}
}

func TestBurstConfig_Validate(t *testing.T) {
	cases := []struct {
		name string
		cfg  BurstConfig
		ok   bool
	}{
		{"disabled", BurstConfig{}, true},
		{"valid", BurstConfig{Size: 10, Window: time.Millisecond, Period: time.Second,
			Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive}, true},
		{"zero window", BurstConfig{Size: 10, Window: 0, Period: time.Second,
			Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive}, false},
		{"zero period", BurstConfig{Size: 10, Window: time.Second, Period: 0,
			Shape: BurstUniform, Jitter: BurstFixed, Mode: BurstAdditive}, false},
		{"bad shape", BurstConfig{Size: 10, Window: time.Millisecond, Period: time.Second,
			Shape: "??", Jitter: BurstFixed, Mode: BurstAdditive}, false},
		{"negative size", BurstConfig{Size: -1}, false},
	}
	for _, c := range cases {
		err := c.cfg.Validate()
		if (err == nil) != c.ok {
			t.Errorf("%s: err=%v, expected ok=%v", c.name, err, c.ok)
		}
	}
}
