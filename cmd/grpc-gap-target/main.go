package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"flag"
	"fmt"
	"math"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/peer"
)

type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) { return v.([]byte), nil }
func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	*(v.(*[]byte)) = data
	return nil
}
func (rawCodec) Name() string { return "proto" }

func init() { encoding.RegisterCodec(rawCodec{}) }

type stats struct {
	mu          sync.Mutex
	arrivalsNs  []int64
	peerLogOnce sync.Once
}

func (s *stats) recordArrival(ts time.Time) {
	s.mu.Lock()
	s.arrivalsNs = append(s.arrivalsNs, ts.UnixNano())
	s.mu.Unlock()
}

func (s *stats) snapshot() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]int64, len(s.arrivalsNs))
	copy(cp, s.arrivalsNs)
	return cp
}

func quantile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}
	idx := p * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func expectedExponentialQuantile(rate float64, p float64) float64 {
	if rate <= 0 || p <= 0 || p >= 1 {
		return math.NaN()
	}
	return -math.Log(1-p) / rate
}

func poissonTailProbAtLeast(mu float64, k int) float64 {
	if k <= 0 {
		return 1.0
	}
	if mu <= 0 {
		if k <= 0 {
			return 1
		}
		return 0
	}
	// 1 - sum_{i=0}^{k-1} e^-mu * mu^i / i!
	term := math.Exp(-mu) // i=0
	sum := term
	for i := 1; i < k; i++ {
		term *= mu / float64(i)
		sum += term
	}
	tail := 1 - sum
	if tail < 0 {
		return 0
	}
	if tail > 1 {
		return 1
	}
	return tail
}

func analyzeWindows(arrivals []int64, window time.Duration, threshold int) (numWindows int64, observedBurstWindows int64, maxInWindow int) {
	if len(arrivals) == 0 || window <= 0 {
		return 0, 0, 0
	}
	first := arrivals[0]
	last := arrivals[len(arrivals)-1]
	windowNs := int64(window)
	numWindows = (last-first)/windowNs + 1
	counts := make([]int, numWindows)
	for _, ts := range arrivals {
		idx := (ts - first) / windowNs
		if idx < 0 {
			idx = 0
		}
		if idx >= int64(len(counts)) {
			idx = int64(len(counts) - 1)
		}
		counts[idx]++
	}
	for _, c := range counts {
		if c >= threshold {
			observedBurstWindows++
		}
		if c > maxInWindow {
			maxInWindow = c
		}
	}
	return numWindows, observedBurstWindows, maxInWindow
}

func printSummary(arrivals []int64) {
	n := len(arrivals)
	if n < 2 {
		fmt.Printf("summary: requests=%d (need >=2 for gap analysis)\n", n)
		return
	}
	durationSec := float64(arrivals[n-1]-arrivals[0]) / float64(time.Second)
	if durationSec <= 0 {
		fmt.Printf("summary: requests=%d duration=0 (cannot compute rate)\n", n)
		return
	}
	observedRate := float64(n) / durationSec

	gaps := make([]float64, 0, n-1)
	var sum float64
	for i := 1; i < n; i++ {
		g := float64(arrivals[i]-arrivals[i-1]) / float64(time.Second)
		gaps = append(gaps, g)
		sum += g
	}
	meanGap := sum / float64(len(gaps))
	var ss float64
	for _, g := range gaps {
		d := g - meanGap
		ss += d * d
	}
	stdGap := math.Sqrt(ss / float64(len(gaps)))
	cvGap := stdGap / meanGap

	sortedGaps := append([]float64(nil), gaps...)
	sort.Float64s(sortedGaps)

	empP50 := quantile(sortedGaps, 0.50)
	empP90 := quantile(sortedGaps, 0.90)
	empP99 := quantile(sortedGaps, 0.99)
	expP50 := expectedExponentialQuantile(observedRate, 0.50)
	expP90 := expectedExponentialQuantile(observedRate, 0.90)
	expP99 := expectedExponentialQuantile(observedRate, 0.99)

	fmt.Println("=== Arrival Gap Summary ===")
	fmt.Printf("requests=%d duration=%.3fs observed_rate=%.2f qps\n", n, durationSec, observedRate)
	fmt.Printf("gaps_mean=%.6fs gaps_std=%.6fs gaps_cv=%.3f (Poisson ideal CV≈1.0)\n", meanGap, stdGap, cvGap)
	fmt.Printf("gaps_p50 observed=%.6fs expected_exp=%.6fs ratio=%.3f\n", empP50, expP50, empP50/expP50)
	fmt.Printf("gaps_p90 observed=%.6fs expected_exp=%.6fs ratio=%.3f\n", empP90, expP90, empP90/expP90)
	fmt.Printf("gaps_p99 observed=%.6fs expected_exp=%.6fs ratio=%.3f\n", empP99, expP99, empP99/expP99)

	type winCfg struct {
		window    time.Duration
		threshold int
	}
	windows := []winCfg{
		{window: 1 * time.Millisecond, threshold: 2},
		{window: 5 * time.Millisecond, threshold: 3},
		{window: 10 * time.Millisecond, threshold: 5},
	}
	fmt.Println("=== Micro-burst vs Poisson Expectation ===")
	for _, cfg := range windows {
		nWin, obsBurst, maxCnt := analyzeWindows(arrivals, cfg.window, cfg.threshold)
		if nWin == 0 {
			continue
		}
		mu := observedRate * cfg.window.Seconds()
		expectedProb := poissonTailProbAtLeast(mu, cfg.threshold)
		observedProb := float64(obsBurst) / float64(nWin)
		expectedWins := expectedProb * float64(nWin)
		fmt.Printf(
			"window=%s threshold>=%d observed=%d/%d (%.6f) expected=%.2f/%d (%.6f) ratio=%.2fx maxInWindow=%d\n",
			cfg.window,
			cfg.threshold,
			obsBurst,
			nWin,
			observedProb,
			expectedWins,
			nWin,
			expectedProb,
			observedProb/expectedProb,
			maxCnt,
		)
	}
}

func main() {
	addr := flag.String("addr", ":28826", "listen address")
	cert := flag.String("cert", "", "server cert")
	key := flag.String("key", "", "server key")
	requireClientCert := flag.Bool("require-client-cert", false, "require clients to present a TLS cert")
	flag.Parse()

	if *cert == "" || *key == "" {
		panic("--cert and --key are required")
	}

	serverCert, err := tls.LoadX509KeyPair(*cert, *key)
	if err != nil {
		panic(err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{serverCert}}
	if *requireClientCert {
		tlsCfg.ClientAuth = tls.RequireAnyClientCert
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}

	st := &stats{arrivalsNs: make([]int64, 0, 1<<20)}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsCfg)),
		grpc.ForceServerCodec(rawCodec{}),
		grpc.UnknownServiceHandler(func(_ interface{}, stream grpc.ServerStream) error {
			st.peerLogOnce.Do(func() {
				if p, ok := peer.FromContext(stream.Context()); ok {
					if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok && len(tlsInfo.State.PeerCertificates) > 0 {
						c := tlsInfo.State.PeerCertificates[0]
						fp := sha256.Sum256(c.Raw)
						fmt.Printf(
							"peer_cert subject=%q issuer=%q serial=%s sha256=%s\n",
							c.Subject.String(),
							c.Issuer.String(),
							c.SerialNumber.Text(16),
							hex.EncodeToString(fp[:]),
						)
					}
				}
			})

			st.recordArrival(time.Now())
			var req []byte
			if err := stream.RecvMsg(&req); err != nil {
				return err
			}
			return stream.SendMsg([]byte("ok"))
		}),
	)

	fmt.Printf("gap dummy target listening on %s\n", *addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("signal received, shutting down...")
		srv.GracefulStop()
	}()

	if err := srv.Serve(lis); err != nil {
		fmt.Printf("server exited: %v\n", err)
	}

	printSummary(st.snapshot())
}
