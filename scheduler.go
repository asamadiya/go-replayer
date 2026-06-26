package main

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// ArrivalKind labels each emitted dispatch time so observability can
// distinguish base Poisson arrivals from burst spikes.
type ArrivalKind uint8

const (
	ArrivalBase ArrivalKind = iota
	ArrivalSpike
)

func (k ArrivalKind) String() string {
	if k == ArrivalSpike {
		return "spike"
	}
	return "base"
}

// BurstShape controls how a burst's N spikes are placed inside its window.
type BurstShape string

const (
	BurstUniform BurstShape = "uniform" // evenly spaced across the window
	BurstSpike   BurstShape = "spike"   // all N at the onset (1µs apart)
	BurstRandom  BurstShape = "random"  // uniform-random offsets in the window
)

// BurstJitter controls how successive burst onsets are spaced.
type BurstJitter string

const (
	BurstFixed   BurstJitter = "fixed"   // exact period between onsets
	BurstPoisson BurstJitter = "poisson" // exponentially distributed inter-onset gap (mean = period)
)

// BurstMode controls how the burst rate composes with the base Poisson rate.
type BurstMode string

const (
	BurstAdditive  BurstMode = "additive"  // bursts ride on top of base λ
	BurstAbsorbing BurstMode = "absorbing" // base λ is reduced so total mean QPS == --qps
)

// BurstConfig is the user-facing burst overlay specification.
type BurstConfig struct {
	Size   int           // requests per burst (0 disables bursts)
	Window time.Duration // time over which the Size requests fire
	Period time.Duration // mean interval between burst onsets
	Shape  BurstShape
	Jitter BurstJitter
	Mode   BurstMode
}

// Enabled reports whether burst injection is active.
func (c BurstConfig) Enabled() bool {
	return c.Size > 0 && c.Period > 0
}

// AvgQPS is the expected long-run rate contribution of bursts.
func (c BurstConfig) AvgQPS() float64 {
	if !c.Enabled() {
		return 0
	}
	return float64(c.Size) / c.Period.Seconds()
}

// Validate enforces user-friendly invariants. Returns nil when disabled.
func (c BurstConfig) Validate() error {
	if c.Size < 0 {
		return fmt.Errorf("--burst-size must be >= 0")
	}
	if c.Size == 0 {
		return nil
	}
	if c.Window <= 0 {
		return fmt.Errorf("--burst-window must be > 0 when --burst-size > 0")
	}
	if c.Period <= 0 {
		return fmt.Errorf("--burst-period must be > 0 when --burst-size > 0")
	}
	switch c.Shape {
	case BurstUniform, BurstSpike, BurstRandom:
	default:
		return fmt.Errorf("--burst-shape must be one of uniform|spike|random")
	}
	switch c.Jitter {
	case BurstFixed, BurstPoisson:
	default:
		return fmt.Errorf("--burst-jitter must be one of fixed|poisson")
	}
	switch c.Mode {
	case BurstAdditive, BurstAbsorbing:
	default:
		return fmt.Errorf("--burst-mode must be one of additive|absorbing")
	}
	return nil
}

func (c BurstConfig) String() string {
	if !c.Enabled() {
		return "disabled"
	}
	return fmt.Sprintf(
		"size=%d window=%s period=%s shape=%s jitter=%s mode=%s avg_qps=%.2f",
		c.Size, c.Window, c.Period, c.Shape, c.Jitter, c.Mode, c.AvgQPS(),
	)
}

// Scheduler emits the deterministic time series of dispatch arrivals,
// composed of an underlying Poisson stream and an optional burst overlay.
//
// Single-goroutine: callers must invoke Next from one goroutine. Counters
// on Scheduler are accessed via sync/atomic so an observer goroutine can
// swap-and-read them concurrently for live telemetry.
type Scheduler struct {
	baseRate int
	replicas int
	burst    BurstConfig
	deadline time.Time
	rng      *rand.Rand

	// Internal state.
	poissonStreams   poissonHeap
	nextBurstOnset   time.Time
	burstSched       bool
	pendingSpikes    []time.Time // sorted ascending
	pendingSpikesCap int

	// Counters (atomic — readable from any goroutine).
	burstsFired int64
	spikesFired int64
	baseFired   int64
	burstsLatch int64
	spikesLatch int64
	baseLatch   int64

	// OnBurstSpawned, if non-nil, is invoked synchronously when a burst is
	// materialised — typically called from the same goroutine that drives
	// Next. Useful for emitting structured burst-onset events.
	OnBurstSpawned func(at time.Time, cfg BurstConfig)
}

type poissonStream struct {
	rate float64
	next time.Time
}

type poissonHeap []poissonStream

func (h poissonHeap) Len() int { return len(h) }

func (h poissonHeap) Less(i, j int) bool { return h[i].next.Before(h[j].next) }

func (h poissonHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *poissonHeap) Push(x any) {
	*h = append(*h, x.(poissonStream))
}

func (h *poissonHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// NewScheduler builds a Scheduler. targetQPS is the user's --qps; the
// scheduler will internally reduce the Poisson base if cfg.Mode is
// BurstAbsorbing so that the long-run mean equals targetQPS.
func NewScheduler(targetQPS int, cfg BurstConfig, start, deadline time.Time, rng *rand.Rand) *Scheduler {
	return NewSchedulerWithReplicas(targetQPS, 1, cfg, start, deadline, rng)
}

// NewSchedulerWithReplicas builds a Scheduler with replicas independent
// Poisson streams. The effective base QPS is spread equally across streams.
func NewSchedulerWithReplicas(targetQPS int, replicas int, cfg BurstConfig, start, deadline time.Time, rng *rand.Rand) *Scheduler {
	if replicas < 1 {
		replicas = 1
	}
	base := targetQPS
	if cfg.Enabled() && cfg.Mode == BurstAbsorbing {
		burstQPS := cfg.AvgQPS()
		base = targetQPS - int(math.Round(burstQPS))
		if base < 0 {
			base = 0
		}
	}
	s := &Scheduler{
		baseRate: base,
		replicas: replicas,
		burst:    cfg,
		deadline: deadline,
		rng:      rng,
	}
	if base > 0 {
		perReplicaRate := float64(base) / float64(replicas)
		s.poissonStreams = make([]poissonStream, replicas)
		for i := range s.poissonStreams {
			s.poissonStreams[i] = poissonStream{
				rate: perReplicaRate,
				next: start.Add(poissonIntervalFloat(perReplicaRate, s.rng)),
			}
		}
		heap.Init(&s.poissonStreams)
	}
	if cfg.Enabled() {
		s.nextBurstOnset = start.Add(cfg.Period)
		s.burstSched = true
		// Pre-size pending spikes buffer to avoid allocations in steady state.
		// Worst-case overlap: ceil(window/period)+1 simultaneous bursts.
		bursts := int(cfg.Window/cfg.Period) + 2
		s.pendingSpikesCap = cfg.Size * bursts
		s.pendingSpikes = make([]time.Time, 0, s.pendingSpikesCap)
	}
	return s
}

// EffectiveBaseRate returns the Poisson λ actually used by the scheduler.
// In absorbing mode this is reduced from the user's --qps.
func (s *Scheduler) EffectiveBaseRate() int { return s.baseRate }

// Replicas returns the number of independent Poisson streams.
func (s *Scheduler) Replicas() int { return s.replicas }

// PerReplicaBaseRate returns the base Poisson λ assigned to each replica.
func (s *Scheduler) PerReplicaBaseRate() float64 {
	if s.replicas <= 0 {
		return 0
	}
	return float64(s.baseRate) / float64(s.replicas)
}

// Next returns the next scheduled arrival before the deadline.
// The bool is false when the schedule is exhausted; the time and kind
// are then zero values.
func (s *Scheduler) Next() (time.Time, ArrivalKind, bool) {
	for {
		// Pick the earliest of {next poisson, next burst onset, head of spikes}.
		nextKind, nextTime := s.peekEarliest()
		if nextKind == evNone {
			return time.Time{}, ArrivalBase, false
		}
		if !nextTime.Before(s.deadline) {
			return time.Time{}, ArrivalBase, false
		}

		switch nextKind {
		case evBurstOnset:
			s.spawnBurst(nextTime)
			s.advanceBurstOnset()
			continue
		case evSpike:
			s.pendingSpikes = s.pendingSpikes[1:]
			atomic.AddInt64(&s.spikesFired, 1)
			atomic.AddInt64(&s.spikesLatch, 1)
			return nextTime, ArrivalSpike, true
		case evPoisson:
			stream := heap.Pop(&s.poissonStreams).(poissonStream)
			stream.next = stream.next.Add(poissonIntervalFloat(stream.rate, s.rng))
			heap.Push(&s.poissonStreams, stream)
			atomic.AddInt64(&s.baseFired, 1)
			atomic.AddInt64(&s.baseLatch, 1)
			return nextTime, ArrivalBase, true
		}
	}
}

type schedulerEvent uint8

const (
	evNone schedulerEvent = iota
	evPoisson
	evSpike
	evBurstOnset
)

// peekEarliest returns the next pending event without advancing state.
func (s *Scheduler) peekEarliest() (schedulerEvent, time.Time) {
	var (
		bestKind schedulerEvent = evNone
		bestTime time.Time
	)
	if len(s.poissonStreams) > 0 {
		bestKind, bestTime = evPoisson, s.poissonStreams[0].next
	}
	if len(s.pendingSpikes) > 0 {
		t := s.pendingSpikes[0]
		if bestKind == evNone || t.Before(bestTime) {
			bestKind, bestTime = evSpike, t
		}
	}
	if s.burstSched {
		t := s.nextBurstOnset
		if bestKind == evNone || t.Before(bestTime) {
			bestKind, bestTime = evBurstOnset, t
		}
	}
	return bestKind, bestTime
}

// spawnBurst materialises s.burst.Size spike timestamps, ordered, into
// pendingSpikes. spawnBurst preserves the ascending invariant on
// pendingSpikes by sorting after the merge.
func (s *Scheduler) spawnBurst(at time.Time) {
	n := s.burst.Size
	w := s.burst.Window
	fresh := make([]time.Time, 0, n)
	switch s.burst.Shape {
	case BurstSpike:
		// All at onset, but spaced 1µs apart so equal-time-tie ordering is stable.
		for i := 0; i < n; i++ {
			fresh = append(fresh, at.Add(time.Duration(i)*time.Microsecond))
		}
	case BurstRandom:
		for i := 0; i < n; i++ {
			offset := time.Duration(s.rng.Float64() * float64(w))
			fresh = append(fresh, at.Add(offset))
		}
		sort.Slice(fresh, func(i, j int) bool { return fresh[i].Before(fresh[j]) })
	default: // BurstUniform
		for i := 0; i < n; i++ {
			offset := time.Duration(int64(w) * int64(i) / int64(n))
			fresh = append(fresh, at.Add(offset))
		}
	}
	if len(s.pendingSpikes) == 0 {
		s.pendingSpikes = append(s.pendingSpikes[:0], fresh...)
	} else {
		s.pendingSpikes = append(s.pendingSpikes, fresh...)
		sort.Slice(s.pendingSpikes, func(i, j int) bool { return s.pendingSpikes[i].Before(s.pendingSpikes[j]) })
	}
	atomic.AddInt64(&s.burstsFired, 1)
	atomic.AddInt64(&s.burstsLatch, 1)
	if s.OnBurstSpawned != nil {
		s.OnBurstSpawned(at, s.burst)
	}
}

// advanceBurstOnset chooses the next burst onset time. Fixed jitter
// preserves periodic spacing; Poisson jitter draws an exponential gap
// with mean = Period.
func (s *Scheduler) advanceBurstOnset() {
	var delta time.Duration
	if s.burst.Jitter == BurstPoisson {
		u := 1.0 - s.rng.Float64()
		if u <= 0 {
			u = math.SmallestNonzeroFloat64
		}
		delta = time.Duration(-math.Log(u) * float64(s.burst.Period))
	} else {
		delta = s.burst.Period
	}
	s.nextBurstOnset = s.nextBurstOnset.Add(delta)
}

// SnapshotCounters returns cumulative counters since scheduler start.
func (s *Scheduler) SnapshotCounters() SchedulerCounters {
	return SchedulerCounters{
		BurstsFired: atomic.LoadInt64(&s.burstsFired),
		SpikesFired: atomic.LoadInt64(&s.spikesFired),
		BaseFired:   atomic.LoadInt64(&s.baseFired),
	}
}

// DrainCounters atomically resets the per-interval latch counters and
// returns what was accumulated since the previous Drain call. Used by the
// reporter goroutine for per-second telemetry.
func (s *Scheduler) DrainCounters() SchedulerCounters {
	return SchedulerCounters{
		BurstsFired: atomic.SwapInt64(&s.burstsLatch, 0),
		SpikesFired: atomic.SwapInt64(&s.spikesLatch, 0),
		BaseFired:   atomic.SwapInt64(&s.baseLatch, 0),
	}
}

// SchedulerCounters captures fire counts (cumulative or per-interval).
type SchedulerCounters struct {
	BurstsFired int64
	SpikesFired int64
	BaseFired   int64
}

// Total returns the total number of arrivals (base + spike).
func (c SchedulerCounters) Total() int64 { return c.BaseFired + c.SpikesFired }

// parseBurstShape and parseBurstJitter / parseBurstMode normalise CLI input.
func parseBurstShape(raw string) (BurstShape, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "uniform", "":
		return BurstUniform, nil
	case "spike":
		return BurstSpike, nil
	case "random":
		return BurstRandom, nil
	default:
		return "", fmt.Errorf("unknown burst shape %q (want uniform|spike|random)", raw)
	}
}

func parseBurstJitter(raw string) (BurstJitter, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "fixed", "":
		return BurstFixed, nil
	case "poisson":
		return BurstPoisson, nil
	default:
		return "", fmt.Errorf("unknown burst jitter %q (want fixed|poisson)", raw)
	}
}

func parseBurstMode(raw string) (BurstMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "additive", "":
		return BurstAdditive, nil
	case "absorbing":
		return BurstAbsorbing, nil
	default:
		return "", fmt.Errorf("unknown burst mode %q (want additive|absorbing)", raw)
	}
}
