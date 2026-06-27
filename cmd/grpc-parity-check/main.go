package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

// rawCodec sends/receives raw bytes without proto marshaling.
type rawCodec struct{}

func (rawCodec) Marshal(v interface{}) ([]byte, error) {
	payload, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec.Marshal: expected []byte, got %T", v)
	}
	return payload, nil
}
func (rawCodec) Unmarshal(data []byte, v interface{}) error {
	*(v.(*[]byte)) = data
	return nil
}
func (rawCodec) Name() string { return "proto" }

func init() { encoding.RegisterCodec(rawCodec{}) }

type request struct {
	method  string
	payload []byte
}

func loadRequests(path string) ([]request, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	const maxPayloadLen = 256 * 1024 * 1024
	var reqs []request
	offset := 0
	for offset < len(data) {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated method-length prefix at offset %d (record %d)", offset, len(reqs))
		}
		methodLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if methodLen == 0 || methodLen > 1024 {
			return nil, fmt.Errorf("invalid method length %d at record %d", methodLen, len(reqs))
		}
		if offset+methodLen > len(data) {
			return nil, fmt.Errorf("truncated method bytes at record %d", len(reqs))
		}
		method := string(data[offset : offset+methodLen])
		offset += methodLen
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated payload-length prefix at record %d", len(reqs))
		}
		payloadLen := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if payloadLen > maxPayloadLen {
			return nil, fmt.Errorf("invalid payload length %d at record %d (max %d)", payloadLen, len(reqs), maxPayloadLen)
		}
		if offset+payloadLen > len(data) {
			return nil, fmt.Errorf("truncated payload at record %d", len(reqs))
		}
		payload := make([]byte, payloadLen)
		copy(payload, data[offset:offset+payloadLen])
		offset += payloadLen
		reqs = append(reqs, request{method: method, payload: payload})
	}
	if len(reqs) == 0 {
		return nil, fmt.Errorf("no requests found in %s", path)
	}
	return reqs, nil
}

func dial(target string, tlsEnabled, insecureSkipVerify bool, certFile, keyFile string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	if tlsEnabled {
		tlsCfg := &tls.Config{InsecureSkipVerify: insecureSkipVerify}
		if certFile != "" && keyFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("load cert: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64*1024*1024)))
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(64*1024*1024)))
	return grpc.NewClient(target, opts...)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func main() {
	var (
		file         = flag.String("file", "", "Path to replay .bin file")
		targetA      = flag.String("target-a", "", "First gRPC target (baseline, no batching)")
		targetB      = flag.String("target-b", "", "Second gRPC target (batching)")
		qps          = flag.Int("qps", 100, "Requests per second")
		numRequests  = flag.Int("n", 0, "Number of requests (0 = all in file)")
		duration     = flag.Duration("duration", 0, "Run for this duration, looping over file (0 = single pass)")
		tolerance    = flag.Float64("tolerance", 1e-3, "Max absolute difference for float parity")
		tlsEnabled   = flag.Bool("tls", false, "Enable TLS")
		insecureFlag = flag.Bool("insecure", false, "Skip TLS verification")
		cert         = flag.String("cert", "", "Client cert path")
		key          = flag.String("key", "", "Client key path")
		timeout      = flag.Duration("timeout", 30*time.Second, "Per-request timeout")
		outputFile   = flag.String("output", "", "Write detailed results to CSV")
	)
	flag.Parse()

	if *file == "" || *targetA == "" || *targetB == "" {
		fmt.Fprintln(os.Stderr, "Usage: grpc-parity-check --file <replay.bin> --target-a <host:port> --target-b <host:port>")
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *qps <= 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --qps must be > 0")
		os.Exit(1)
	}

	fmt.Printf("Loading requests from %s...\n", *file)
	reqs, err := loadRequests(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR loading requests: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Loaded %d requests\n", len(reqs))

	if *numRequests > 0 && *numRequests < len(reqs) {
		reqs = reqs[:*numRequests]
	}

	fmt.Printf("Connecting to A: %s\n", *targetA)
	connA, err := dial(*targetA, *tlsEnabled, *insecureFlag, *cert, *key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR connecting to A: %v\n", err)
		os.Exit(1)
	}
	defer connA.Close()

	fmt.Printf("Connecting to B: %s\n", *targetB)
	connB, err := dial(*targetB, *tlsEnabled, *insecureFlag, *cert, *key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR connecting to B: %v\n", err)
		os.Exit(1)
	}
	defer connB.Close()

	if *duration > 0 {
		fmt.Printf("Running parity check: %d unique requests at %d QPS for %s (looping), tolerance=%.0e\n", len(reqs), *qps, *duration, *tolerance)
	} else {
		fmt.Printf("Running parity check: %d requests at %d QPS, tolerance=%.0e\n", len(reqs), *qps, *tolerance)
	}
	fmt.Printf("  A (baseline): %s\n  B (batching): %s\n", *targetA, *targetB)
	fmt.Println()

	interval := time.Second / time.Duration(*qps)
	var (
		totalSent     int64
		totalMatch    int64
		totalMismatch int64
		totalErrA     int64
		totalErrB     int64
		totalBothErr  int64
		totalByteDiff int64
		mu            sync.Mutex
		diffs         []float64
	)

	var csvFile *os.File
	if *outputFile != "" {
		csvFile, err = os.Create(*outputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR creating output file %s: %v\n", *outputFile, err)
			os.Exit(1)
		}
		defer csvFile.Close()
		fmt.Fprintln(csvFile, "index,match,max_abs_diff,total_floats,mismatch_floats,size_a,size_b,latency_a_ms,latency_b_ms,err_a,err_b")
	}

	sem := make(chan struct{}, 32) // concurrency limit

	startTime := time.Now()
	deadline := time.Time{}
	if *duration > 0 {
		deadline = startTime.Add(*duration)
	}
	reqIdx := 0
	for {
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
		if deadline.IsZero() && reqIdx >= len(reqs) {
			break
		}
		req := reqs[reqIdx%len(reqs)]
		i := reqIdx
		reqIdx++
		sem <- struct{}{}
		go func(idx int, r request) {
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), *timeout)
			defer cancel()

			var respA, respB []byte
			var errA, errB error

			// Send to both concurrently
			var wg sync.WaitGroup
			var latA, latB time.Duration

			wg.Add(2)
			go func() {
				defer wg.Done()
				t0 := time.Now()
				errA = connA.Invoke(ctx, r.method, r.payload, &respA)
				latA = time.Since(t0)
			}()
			go func() {
				defer wg.Done()
				t0 := time.Now()
				errB = connB.Invoke(ctx, r.method, r.payload, &respB)
				latB = time.Since(t0)
			}()
			wg.Wait()

			atomic.AddInt64(&totalSent, 1)

			switch {
			case errA != nil && errB != nil:
				atomic.AddInt64(&totalBothErr, 1)
			case errA != nil:
				atomic.AddInt64(&totalErrA, 1)
			case errB != nil:
				atomic.AddInt64(&totalErrB, 1)
			case bytes.Equal(respA, respB):
				atomic.AddInt64(&totalMatch, 1)
			default:
				atomic.AddInt64(&totalByteDiff, 1)
				match, maxDiff, totalFloats, mismatchCount := compareProtoFloats(respA, respB, *tolerance)
				if match {
					atomic.AddInt64(&totalMatch, 1)
				} else {
					atomic.AddInt64(&totalMismatch, 1)
				}

				mu.Lock()
				diffs = append(diffs, maxDiff)
				if csvFile != nil {
					fmt.Fprintf(csvFile, "%d,%v,%.8f,%d,%d,%d,%d,%.1f,%.1f,,\n",
						idx, match, maxDiff, totalFloats, mismatchCount,
						len(respA), len(respB),
						float64(latA.Microseconds())/1000.0,
						float64(latB.Microseconds())/1000.0)
				}
				mu.Unlock()
			}

			// Progress
			sent := atomic.LoadInt64(&totalSent)
			if sent%100 == 0 {
				m := atomic.LoadInt64(&totalMatch)
				mm := atomic.LoadInt64(&totalMismatch)
				fmt.Printf("\r[%d/%d] match=%d mismatch=%d errA=%d errB=%d",
					sent, len(reqs), m, mm,
					atomic.LoadInt64(&totalErrA), atomic.LoadInt64(&totalErrB))
			}
		}(i, req)

		// Rate limit
		time.Sleep(interval)
	}

	// Drain semaphore
	for i := 0; i < 32; i++ {
		sem <- struct{}{}
	}

	elapsed := time.Since(startTime)

	fmt.Printf("\n\n")
	fmt.Println(strings.Repeat("═", 70))
	fmt.Println("PARITY CHECK RESULTS")
	fmt.Println(strings.Repeat("═", 70))
	fmt.Printf("  Requests sent:      %d\n", totalSent)
	fmt.Printf("  Duration:           %s\n", elapsed.Round(time.Second))
	fmt.Printf("  Tolerance:          %.0e\n", *tolerance)
	fmt.Println()
	fmt.Printf("  Byte-identical:     %d\n", totalMatch-totalByteDiff+atomic.LoadInt64(&totalBothErr))
	fmt.Printf("  Float-match:        %d  (within tolerance)\n", totalByteDiff-totalMismatch)
	fmt.Printf("  MISMATCH:           %d\n", totalMismatch)
	fmt.Printf("  Errors (A only):    %d\n", totalErrA)
	fmt.Printf("  Errors (B only):    %d\n", totalErrB)
	fmt.Printf("  Errors (both):      %d\n", totalBothErr)
	fmt.Println()

	parity := float64(totalMatch) / float64(totalSent) * 100
	fmt.Printf("  PARITY RATE:        %.2f%%  (%d/%d)\n", parity, totalMatch, totalSent)

	if len(diffs) > 0 {
		sort.Float64s(diffs)
		n := len(diffs)
		fmt.Println()
		fmt.Println("  Float difference distribution (responses with byte differences):")
		fmt.Printf("    min:    %.8f\n", diffs[0])
		fmt.Printf("    p50:    %.8f\n", diffs[n/2])
		fmt.Printf("    p90:    %.8f\n", diffs[int(float64(n)*0.9)])
		fmt.Printf("    p99:    %.8f\n", diffs[int(float64(n)*0.99)])
		fmt.Printf("    max:    %.8f\n", diffs[n-1])
	}

	fmt.Println(strings.Repeat("═", 70))

	if totalMismatch > 0 {
		fmt.Printf("\n❌ PARITY FAILED: %d mismatches at tolerance %.0e\n", totalMismatch, *tolerance)
		os.Exit(1)
	} else {
		fmt.Printf("\n✅ PARITY PASSED: 100%% match at tolerance %.0e\n", *tolerance)
		os.Exit(0)
	}
}
