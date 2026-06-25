package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

const defaultMethod = "/example.replay.v1.ReplayService/Score"

var (
	takeoverInstallRootCandidates = []string{
		"/opt/replay/install",
		"/var/lib/replay/install",
	}
	takeoverReplayFileCandidates = []string{
		"replay_2gb.bin",
		"recorded_requests.bin",
		"var/replay_2gb.bin",
		"var/recorded_requests.bin",
	}
	takeoverCACandidates = []string{
		"var/identity.ca",
		"var/identity-ca.crt",
		"var/ca.crt",
	}
)

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

type dispatchJob struct {
	payload []byte
	method  string
}

type latencyRecord struct {
	ts      int64
	latency float64
	ok      bool
}

type takeoverArtifacts struct {
	installRoot string
	replayFile  string
	certFile    string
	keyFile     string
	caFile      string
}

func validateLoadedRequest(method string, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("zero-length payload for method %q", method)
	}
	return nil
}

func roundRobinRequest(reqs []request, idx int) request {
	return reqs[idx%len(reqs)]
}

func makeDispatchJob(req request, fallbackMethod string) dispatchJob {
	useMethod := fallbackMethod
	if req.method != "" {
		useMethod = req.method
	}

	payloadCopy := append([]byte(nil), req.payload...)
	return dispatchJob{
		method:  useMethod,
		payload: payloadCopy,
	}
}

func loadRequests(path string) ([]request, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var reqs []request
	var totalBytes int64
	var skippedInvalid int
	for {
		var methodLen uint32
		if err := binary.Read(f, binary.BigEndian, &methodLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading method length: %w", err)
		}
		method := make([]byte, methodLen)
		if _, err := io.ReadFull(f, method); err != nil {
			return nil, fmt.Errorf("reading method name (%d bytes): %w", methodLen, err)
		}
		var payloadLen uint32
		if err := binary.Read(f, binary.BigEndian, &payloadLen); err != nil {
			return nil, fmt.Errorf("reading payload length: %w", err)
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			return nil, fmt.Errorf("reading request body (%d bytes): %w", payloadLen, err)
		}
		recordBytes := int64(8 + methodLen + payloadLen)
		totalBytes += recordBytes

		methodName := string(method)
		if err := validateLoadedRequest(methodName, payload); err != nil {
			skippedInvalid++
			continue
		}

		reqs = append(reqs, request{method: methodName, payload: payload})
	}

	fmt.Printf("Loaded %d requests from %s (%.1f MB)\n", len(reqs), path, float64(totalBytes)/(1024*1024))
	if skippedInvalid > 0 {
		fmt.Printf("Skipped %d invalid requests while loading %s\n", skippedInvalid, path)
	}
	return reqs, nil
}

func osDirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func osFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func firstExisting(paths []string, exists func(string) bool) (string, bool) {
	for _, path := range paths {
		if exists(path) {
			return path, true
		}
	}
	return "", false
}

func resolveTakeoverInstallRoot(explicit string, dirExists func(string) bool) (string, error) {
	if explicit != "" {
		if !dirExists(explicit) {
			return "", fmt.Errorf("install root does not exist: %s", explicit)
		}
		return explicit, nil
	}

	if root, ok := firstExisting(takeoverInstallRootCandidates, dirExists); ok {
		return root, nil
	}

	return "", fmt.Errorf(
		"could not auto-discover install root; pass --install-root (checked %v)",
		takeoverInstallRootCandidates,
	)
}

func choosePath(override string, candidates []string, exists func(string) bool, label string) (string, error) {
	if override != "" {
		if !exists(override) {
			return "", fmt.Errorf("%s override does not exist: %s", label, override)
		}
		return override, nil
	}

	if selected, ok := firstExisting(candidates, exists); ok {
		return selected, nil
	}

	return "", fmt.Errorf("could not auto-discover %s; checked %v", label, candidates)
}

func resolveTakeoverArtifacts(
	installRoot string,
	replayOverride string,
	certOverride string,
	keyOverride string,
	caOverride string,
	fileExists func(string) bool,
) (takeoverArtifacts, error) {
	replayCandidates := make([]string, 0, len(takeoverReplayFileCandidates))
	for _, candidate := range takeoverReplayFileCandidates {
		replayCandidates = append(replayCandidates, filepath.Join(installRoot, candidate))
	}
	replayFile, err := choosePath(replayOverride, replayCandidates, fileExists, "replay file")
	if err != nil {
		return takeoverArtifacts{}, err
	}

	certFile, err := choosePath(
		certOverride,
		[]string{filepath.Join(installRoot, "var/identity.cert")},
		fileExists,
		"client cert",
	)
	if err != nil {
		return takeoverArtifacts{}, err
	}

	keyFile, err := choosePath(
		keyOverride,
		[]string{filepath.Join(installRoot, "var/identity.key")},
		fileExists,
		"client key",
	)
	if err != nil {
		return takeoverArtifacts{}, err
	}

	var caFile string
	if caOverride != "" {
		if !fileExists(caOverride) {
			return takeoverArtifacts{}, fmt.Errorf("CA override does not exist: %s", caOverride)
		}
		caFile = caOverride
	} else {
		candidates := make([]string, 0, len(takeoverCACandidates))
		for _, candidate := range takeoverCACandidates {
			candidates = append(candidates, filepath.Join(installRoot, candidate))
		}
		if selected, ok := firstExisting(candidates, fileExists); ok {
			caFile = selected
		}
	}

	return takeoverArtifacts{
		installRoot: installRoot,
		replayFile:  replayFile,
		certFile:    certFile,
		keyFile:     keyFile,
		caFile:      caFile,
	}, nil
}

func buildDialOptions(useTLS bool, insecureSkip bool, certFile string, keyFile string, caFile string) ([]grpc.DialOption, error) {
	var opts []grpc.DialOption
	if useTLS {
		tlsCfg := &tls.Config{InsecureSkipVerify: insecureSkip}

		if (certFile == "") != (keyFile == "") {
			return nil, fmt.Errorf("both --cert and --key must be provided together")
		}
		if certFile != "" {
			cert, err := tls.LoadX509KeyPair(certFile, keyFile)
			if err != nil {
				return nil, fmt.Errorf("loading cert/key: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		if caFile != "" {
			caPEM, err := os.ReadFile(caFile)
			if err != nil {
				return nil, fmt.Errorf("reading CA file %s: %w", caFile, err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caPEM) {
				return nil, fmt.Errorf("failed to parse CA PEM: %s", caFile)
			}
			tlsCfg.RootCAs = pool
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.ForceCodec(rawCodec{})))
	return opts, nil
}

func poissonInterval(rate int, rng *rand.Rand) time.Duration {
	if rate <= 0 {
		return 0
	}
	u := 1.0 - rng.Float64()
	if u <= 0 {
		u = math.SmallestNonzeroFloat64
	}
	seconds := -math.Log(u) / float64(rate)
	return time.Duration(seconds * float64(time.Second))
}

func stripBalancedWrappers(input string) string {
	out := strings.TrimSpace(input)
	for len(out) >= 2 {
		first := out[0]
		last := out[len(out)-1]
		if (first == '(' && last == ')') || (first == '[' && last == ']') || (first == '{' && last == '}') {
			out = strings.TrimSpace(out[1 : len(out)-1])
			continue
		}
		break
	}
	return out
}

func unquoteToken(input string) string {
	out := strings.TrimSpace(input)
	for len(out) >= 2 {
		first := out[0]
		last := out[len(out)-1]
		if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			out = strings.TrimSpace(out[1 : len(out)-1])
			continue
		}
		break
	}
	return out
}

func normalizeTargetArg(raw string) (string, error) {
	target := stripBalancedWrappers(raw)
	if target == "" {
		return "", fmt.Errorf("--target must not be empty")
	}

	if strings.Contains(target, ",") {
		parts := strings.Split(target, ",")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid --target %q: tuple form must be exactly (host,port)", raw)
		}

		host := strings.Trim(strings.Trim(unquoteToken(parts[0]), "[]"), " ")
		port := unquoteToken(parts[1])
		if host == "" || port == "" {
			return "", fmt.Errorf("invalid --target %q: tuple form requires non-empty host and port", raw)
		}

		portNum, err := strconv.Atoi(port)
		if err != nil || portNum < 1 || portNum > 65535 {
			return "", fmt.Errorf("invalid --target %q: port must be in [1, 65535]", raw)
		}
		return net.JoinHostPort(host, strconv.Itoa(portNum)), nil
	}

	target = unquoteToken(target)
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return "", fmt.Errorf(
			"invalid --target %q: expected host:port or tuple (host,port)",
			raw,
		)
	}
	if host == "" || port == "" {
		return "", fmt.Errorf("invalid --target %q: host and port are required", raw)
	}

	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return "", fmt.Errorf("invalid --target %q: port must be in [1, 65535]", raw)
	}
	return net.JoinHostPort(strings.Trim(host, "[]"), strconv.Itoa(portNum)), nil
}

func main() {
	filePath := flag.String("file", "", "Path to binary file of length-prefixed protobuf requests")
	target := flag.String("target", "", "gRPC target host:port or tuple (host,port) (required, including takeover mode)")
	qps := flag.Int("qps", 10, "Target queries per second (Poisson arrival rate)")
	duration := flag.Duration("duration", time.Minute, "How long to replay")
	method := flag.String("method", defaultMethod, "Full gRPC method path")
	useTLS := flag.Bool("tls", true, "Use TLS for gRPC connection")
	insecureSkip := flag.Bool("insecure", false, "Skip TLS certificate verification")
	certFile := flag.String("cert", "", "Path to client TLS certificate file")
	keyFile := flag.String("key", "", "Path to client TLS private key file")
	caFile := flag.String("ca", "", "Path to CA bundle PEM file")
	numConns := flag.Int("connections", 4, "Number of gRPC connections")
	numWorkers := flag.Int("workers", 0, "Number of sender workers (0=auto)")
	queueCapacity := flag.Int("queue-capacity", 0, "Send queue capacity (0=auto)")
	requestTimeout := flag.Duration("request-timeout", 30*time.Second, "Per-request timeout")
	outputPath := flag.String("output", "", "Path to write latency CSV")

	takeOver := flag.Bool("take-over-replay", false, "Take over an external replay deployment's artifacts and run high-throughput Poisson replay")
	takeoverInstallRoot := flag.String("install-root", "", "Install root for takeover mode")
	takeoverReplayFile := flag.String("replay-file", "", "Override replay file in takeover mode")
	takeoverCertFile := flag.String("takeover-cert", "", "Override client cert in takeover mode")
	takeoverKeyFile := flag.String("takeover-key", "", "Override client key in takeover mode")
	takeoverCAFile := flag.String("takeover-ca", "", "Override CA file in takeover mode")

	burstSize := flag.Int("burst-size", 0, "Requests per burst (0=disabled). Bursts overlay the base Poisson stream.")
	burstWindow := flag.Duration("burst-window", 20*time.Millisecond, "Time window over which --burst-size requests fire")
	burstPeriod := flag.Duration("burst-period", time.Second, "Mean interval between burst onsets")
	burstShapeRaw := flag.String("burst-shape", "uniform", "Burst shape: uniform|spike|random")
	burstJitterRaw := flag.String("burst-jitter", "fixed", "Burst onset spacing: fixed|poisson")
	burstModeRaw := flag.String("burst-mode", "additive", "Burst composition: additive (over base λ) or absorbing (base λ reduced so mean QPS == --qps)")
	metricsJSONL := flag.String("metrics-jsonl", "", "Path to write per-second telemetry, burst events, and final summary as newline-delimited JSON")
	burstWindowsAnalyzer := flag.Duration("analyzer-window", 20*time.Millisecond, "Bin width for sender-side window analysis printed at end of run")

	flag.Parse()

	if *takeOver {
		if *target == "" {
			fmt.Fprintln(os.Stderr, "ERROR: --target is required for --take-over-replay")
			flag.Usage()
			os.Exit(1)
		}

		installRoot, err := resolveTakeoverInstallRoot(*takeoverInstallRoot, osDirExists)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR resolving takeover install root: %v\n", err)
			os.Exit(1)
		}

		artifacts, err := resolveTakeoverArtifacts(
			installRoot,
			*takeoverReplayFile,
			*takeoverCertFile,
			*takeoverKeyFile,
			*takeoverCAFile,
			osFileExists,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR resolving takeover artifacts: %v\n", err)
			os.Exit(1)
		}

		if *filePath == "" {
			*filePath = artifacts.replayFile
		}
		if *certFile == "" {
			*certFile = artifacts.certFile
		}
		if *keyFile == "" {
			*keyFile = artifacts.keyFile
		}
		if *caFile == "" && artifacts.caFile != "" {
			*caFile = artifacts.caFile
		}
		*useTLS = true

		cpus := runtime.NumCPU()
		prev := runtime.GOMAXPROCS(cpus)
		if *numConns == 4 {
			*numConns = maxInt(32, cpus*4)
		}
		if *numWorkers == 0 {
			*numWorkers = maxInt(128, *numConns*4)
		}
		if *queueCapacity == 0 {
			*queueCapacity = maxInt(4096, *numWorkers*32)
		}

		fmt.Printf(
			"Takeover mode enabled\n  install=%s\n  replay_file=%s\n  cert=%s\n  key=%s\n  ca=%s\n  GOMAXPROCS=%d->%d\n",
			artifacts.installRoot,
			*filePath,
			*certFile,
			*keyFile,
			emptyIfUnset(*caFile),
			prev,
			runtime.GOMAXPROCS(0),
		)
	}

	if *filePath == "" || *target == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --file and --target are required")
		flag.Usage()
		os.Exit(1)
	}
	normalizedTarget, err := normalizeTargetArg(*target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	*target = normalizedTarget
	if *qps <= 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --qps must be > 0")
		os.Exit(1)
	}
	if *duration <= 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --duration must be > 0")
		os.Exit(1)
	}
	if *numConns <= 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --connections must be > 0")
		os.Exit(1)
	}
	if *requestTimeout <= 0 {
		fmt.Fprintln(os.Stderr, "ERROR: --request-timeout must be > 0")
		os.Exit(1)
	}
	if *numWorkers <= 0 {
		*numWorkers = maxInt(8, *numConns*2)
	}
	if *queueCapacity <= 0 {
		*queueCapacity = maxInt(512, *numWorkers*16)
	}
	if (*certFile == "") != (*keyFile == "") {
		fmt.Fprintln(os.Stderr, "ERROR: --cert and --key must be provided together")
		os.Exit(1)
	}

	burstShape, err := parseBurstShape(*burstShapeRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	burstJitter, err := parseBurstJitter(*burstJitterRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	burstMode, err := parseBurstMode(*burstModeRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	burstCfg := BurstConfig{
		Size:   *burstSize,
		Window: *burstWindow,
		Period: *burstPeriod,
		Shape:  burstShape,
		Jitter: burstJitter,
		Mode:   burstMode,
	}
	if err := burstCfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	reqs, err := loadRequests(*filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR loading requests: %v\n", err)
		os.Exit(1)
	}
	if len(reqs) == 0 {
		fmt.Fprintln(os.Stderr, "ERROR: no requests loaded")
		os.Exit(1)
	}

	opts, err := buildDialOptions(*useTLS, *insecureSkip, *certFile, *keyFile, *caFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR building dial options: %v\n", err)
		os.Exit(1)
	}

	conns := make([]*grpc.ClientConn, *numConns)
	for i := 0; i < *numConns; i++ {
		dialCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		conn, err := grpc.DialContext(dialCtx, *target, opts...)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR connecting (%d/%d): %v\n", i+1, *numConns, err)
			os.Exit(1)
		}
		conns[i] = conn
		defer conn.Close()
	}

	fmt.Printf(
		"Opened %d gRPC connections to %s (workers=%d queue=%d tls=%v insecure=%v cert=%v)\n",
		*numConns,
		*target,
		*numWorkers,
		*queueCapacity,
		*useTLS,
		*insecureSkip,
		*certFile != "",
	)
	fmt.Printf("Replaying at %d QPS for %s using method %s\n\n", *qps, *duration, *method)

	var (
		totalSent             int64
		totalOK               int64
		totalErr              int64
		secSent               int64
		secOK                 int64
		secErr                int64
		secLatencies          []float64
		allOKLatencies        []float64
		allRecords            []latencyRecord
		errorCounts           = make(map[string]int)
		inflight              int64
		schedulerBlockedNanos int64
		schedulerLagNanos     int64
		mu                    sync.Mutex
	)

	var csvFile *os.File
	if *outputPath != "" {
		csvFile, err = os.Create(*outputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR creating output file %s: %v\n", *outputPath, err)
			os.Exit(1)
		}
		defer csvFile.Close()
		fmt.Fprintln(csvFile, "timestamp_ns,latency_ms,status")
	}

	jsonl, closeJSONL, err := OpenJSONLWriter(*metricsJSONL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer func() {
		if err := closeJSONL(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN closing jsonl: %v\n", err)
		}
	}()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	scheduleStart := time.Now()
	replayDeadline := scheduleStart.Add(*duration)
	scheduler := NewScheduler(*qps, burstCfg, scheduleStart, replayDeadline, rng)
	scheduler.OnBurstSpawned = func(at time.Time, cfg BurstConfig) {
		jsonl.WriteBurstEvent(at, cfg)
	}
	analyzer := NewWindowAnalyzer(*burstWindowsAnalyzer)

	if burstCfg.Enabled() {
		fmt.Printf(
			"Bursts enabled: %s (effective_base_qps=%d)\n",
			burstCfg, scheduler.EffectiveBaseRate(),
		)
	}

	jobs := make(chan dispatchJob, *queueCapacity)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	startTime := time.Now()
	reporterStop := make(chan struct{})
	var reporterWG sync.WaitGroup
	reporterWG.Add(1)
	go func() {
		defer reporterWG.Done()
		sec := 0
		for {
			select {
			case <-ticker.C:
				sec++
				s := atomic.SwapInt64(&secSent, 0)
				o := atomic.SwapInt64(&secOK, 0)
				e := atomic.SwapInt64(&secErr, 0)
				mu.Lock()
				lats := make([]float64, len(secLatencies))
				copy(lats, secLatencies)
				secLatencies = secLatencies[:0]
				mu.Unlock()

				sort.Float64s(lats)
				p50, p90, p99 := 0.0, 0.0, 0.0
				if len(lats) > 0 {
					p50 = lats[len(lats)*50/100]
					p90 = lats[len(lats)*90/100]
					p99 = lats[len(lats)*99/100]
				}

				sendDrift := float64(s - int64(*qps))
				schedBlockedMs := float64(atomic.SwapInt64(&schedulerBlockedNanos, 0)) / float64(time.Millisecond)
				schedLagMs := float64(atomic.SwapInt64(&schedulerLagNanos, 0)) / float64(time.Millisecond)
				schedCounters := scheduler.DrainCounters()
				inflightSnapshot := atomic.LoadInt64(&inflight)
				queueLen := len(jobs)

				if burstCfg.Enabled() {
					fmt.Printf(
						"[%4ds] target=%-4d sent=%-5d ok=%-5d err=%-3d drift=%+6.1f p50=%.1fms p90=%.1fms p99=%.1fms inflight=%d queue=%d sched_block=%.1fms sched_lag=%.1fms bursts=%d spikes=%d base=%d\n",
						sec,
						*qps,
						s,
						o,
						e,
						sendDrift,
						p50,
						p90,
						p99,
						inflightSnapshot,
						queueLen,
						schedBlockedMs,
						schedLagMs,
						schedCounters.BurstsFired,
						schedCounters.SpikesFired,
						schedCounters.BaseFired,
					)
				} else {
					fmt.Printf(
						"[%4ds] target=%-4d sent=%-5d ok=%-5d err=%-3d drift=%+6.1f p50=%.1fms p90=%.1fms p99=%.1fms inflight=%d queue=%d sched_block=%.1fms sched_lag=%.1fms\n",
						sec,
						*qps,
						s,
						o,
						e,
						sendDrift,
						p50,
						p90,
						p99,
						inflightSnapshot,
						queueLen,
						schedBlockedMs,
						schedLagMs,
					)
				}
				jsonl.WriteTick(EmitTick{
					TS:             time.Now(),
					Sec:            sec,
					TargetQPS:      *qps,
					Sent:           s,
					OK:             o,
					Err:            e,
					Drift:          sendDrift,
					P50:            p50,
					P90:            p90,
					P99:            p99,
					Inflight:       inflightSnapshot,
					Queue:          queueLen,
					SchedBlockedMs: schedBlockedMs,
					SchedLagMs:     schedLagMs,
					BurstsFired:    schedCounters.BurstsFired,
					SpikesFired:    schedCounters.SpikesFired,
					BaseFired:      schedCounters.BaseFired,
				})
			case <-reporterStop:
				return
			}
		}
	}()

	var connRR uint64
	var workersWG sync.WaitGroup
	for i := 0; i < *numWorkers; i++ {
		workersWG.Add(1)
		go func() {
			defer workersWG.Done()
			for job := range jobs {
				conn := conns[int(atomic.AddUint64(&connRR, 1)-1)%len(conns)]

				atomic.AddInt64(&inflight, 1)
				start := time.Now()
				var resp []byte
				ctx, cancel := context.WithTimeout(context.Background(), *requestTimeout)
				err := conn.Invoke(ctx, job.method, job.payload, &resp, grpc.ForceCodec(rawCodec{}))
				cancel()
				lat := float64(time.Since(start).Microseconds()) / 1000.0
				atomic.AddInt64(&inflight, -1)
				errSummary := ""
				if err != nil {
					atomic.AddInt64(&secErr, 1)
					atomic.AddInt64(&totalErr, 1)
					errSummary = err.Error()
					if len(errSummary) > 120 {
						errSummary = errSummary[:120]
					}
				} else {
					atomic.AddInt64(&secOK, 1)
					atomic.AddInt64(&totalOK, 1)
				}

				mu.Lock()
				secLatencies = append(secLatencies, lat)
				if errSummary != "" {
					errorCounts[errSummary]++
				} else {
					allOKLatencies = append(allOKLatencies, lat)
				}
				if csvFile != nil {
					allRecords = append(allRecords, latencyRecord{ts: start.UnixNano(), latency: lat, ok: err == nil})
				}
				mu.Unlock()
			}
		}()
	}

	reqIdx := 0
	for {
		nextAt, _, ok := scheduler.Next()
		if !ok {
			break
		}
		now := time.Now()
		if now.Before(nextAt) {
			time.Sleep(nextAt.Sub(now))
		} else if now.After(nextAt) {
			atomic.AddInt64(&schedulerLagNanos, now.Sub(nextAt).Nanoseconds())
		}

		req := roundRobinRequest(reqs, reqIdx)
		reqIdx++
		job := makeDispatchJob(req, *method)

		analyzer.Record(time.Now())

		enqueueStart := time.Now()
		jobs <- job
		atomic.AddInt64(&schedulerBlockedNanos, time.Since(enqueueStart).Nanoseconds())

		atomic.AddInt64(&totalSent, 1)
		atomic.AddInt64(&secSent, 1)
	}

	close(jobs)
	workersWG.Wait()
	close(reporterStop)
	reporterWG.Wait()
	time.Sleep(100 * time.Millisecond)

	if csvFile != nil {
		mu.Lock()
		for _, record := range allRecords {
			status := "ok"
			if !record.ok {
				status = "err"
			}
			fmt.Fprintf(csvFile, "%d,%.3f,%s\n", record.ts, record.latency, status)
		}
		mu.Unlock()
	}

	mu.Lock()
	all := make([]float64, len(allOKLatencies))
	copy(all, allOKLatencies)
	errSnapshot := make(map[string]int, len(errorCounts))
	for msg, cnt := range errorCounts {
		errSnapshot[msg] = cnt
	}
	mu.Unlock()

	sort.Float64s(all)
	elapsed := time.Since(startTime)
	total := atomic.LoadInt64(&totalSent)
	ok := atomic.LoadInt64(&totalOK)
	errCount := atomic.LoadInt64(&totalErr)

	fmt.Println("────────────────────────────────────────────────────────────")
	fmt.Printf(
		"SUMMARY: total=%d ok=%d errors=%d error_rate=%.1f%% duration=%s target_qps=%d achieved_send_qps=%.1f achieved_ok_qps=%.1f\n",
		total,
		ok,
		errCount,
		float64(errCount)/float64(maxInt64(total, 1))*100,
		elapsed.Round(time.Second),
		*qps,
		float64(total)/elapsed.Seconds(),
		float64(ok)/elapsed.Seconds(),
	)
	if len(all) > 0 {
		fmt.Printf(
			"Latencies (ok only): min=%.1fms avg=%.1fms p50=%.1fms p90=%.1fms p99=%.1fms max=%.1fms\n",
			all[0],
			avg(all),
			all[len(all)*50/100],
			all[len(all)*90/100],
			all[len(all)*99/100],
			all[len(all)-1],
		)
	}
	if len(errSnapshot) > 0 {
		type errorCount struct {
			msg string
			cnt int
		}
		top := make([]errorCount, 0, len(errSnapshot))
		for msg, cnt := range errSnapshot {
			top = append(top, errorCount{msg: msg, cnt: cnt})
		}
		sort.Slice(top, func(i, j int) bool { return top[i].cnt > top[j].cnt })
		fmt.Println("Top errors:")
		for _, item := range top {
			fmt.Printf("  [%d] %s\n", item.cnt, item.msg)
		}
	}

	windowSummary := analyzer.Summary(*burstWindowsAnalyzer, []int{8, 10, 12, 30})
	if windowSummary.NumEvents > 0 {
		fmt.Println(FormatWindowSummary("emitted dispatches", windowSummary))
	}

	schedTotals := scheduler.SnapshotCounters()
	summaryRow := map[string]any{
		"target_qps":             *qps,
		"effective_base_qps":     scheduler.EffectiveBaseRate(),
		"duration":               elapsed.Round(time.Second).String(),
		"total_sent":             total,
		"total_ok":               ok,
		"total_err":              errCount,
		"achieved_send_qps":      float64(total) / elapsed.Seconds(),
		"achieved_ok_qps":        float64(ok) / elapsed.Seconds(),
		"bursts_fired":           schedTotals.BurstsFired,
		"spikes_fired":           schedTotals.SpikesFired,
		"base_fired":             schedTotals.BaseFired,
		"burst_config":           burstCfg.String(),
		"window_summary":         windowSummary,
	}
	if len(all) > 0 {
		summaryRow["latency_ms"] = map[string]float64{
			"min": all[0],
			"avg": avg(all),
			"p50": all[len(all)*50/100],
			"p90": all[len(all)*90/100],
			"p99": all[len(all)*99/100],
			"max": all[len(all)-1],
		}
	}
	jsonl.WriteSummary(summaryRow)
	fmt.Println("────────────────────────────────────────────────────────────")
}

func avg(series []float64) float64 {
	total := 0.0
	for _, value := range series {
		total += value
	}
	return total / float64(len(series))
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a int64, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func emptyIfUnset(value string) string {
	if value == "" {
		return "<unset>"
	}
	return value
}
