// Command loadgen drives synthetic telemetry at the ingest API.
//
// It exists for two reasons: to see what the pipeline actually does under load
// rather than asserting a number in a README, and to give the local dashboards
// something to display. It is a development tool, not a benchmark harness --
// it measures the client's view of latency, which includes everything between
// the two processes.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	cfg := parseFlags()

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	target      string
	apiKey      string
	workers     int
	batchSize   int
	duration    time.Duration
	rate        int
	metrics     int
	hosts       int
	idempotency bool
}

func parseFlags() config {
	var cfg config

	flag.StringVar(&cfg.target, "target", "http://localhost:8080", "ingest API base URL")
	flag.StringVar(&cfg.apiKey, "key", "fxg_local_local-dev-secret", "API key")
	flag.IntVar(&cfg.workers, "workers", 8, "concurrent senders")
	flag.IntVar(&cfg.batchSize, "batch", 100, "points per batch")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "how long to run")
	flag.IntVar(&cfg.rate, "rate", 0, "batches per second across all workers; 0 means as fast as possible")
	flag.IntVar(&cfg.metrics, "metrics", 4, "distinct metric names")
	flag.IntVar(&cfg.hosts, "hosts", 10, "distinct label values, which multiplies series cardinality")
	flag.BoolVar(&cfg.idempotency, "idempotency", false, "send an Idempotency-Key with every batch")

	flag.Parse()
	return cfg
}

// result is one request's outcome.
type result struct {
	status  int
	latency time.Duration
	err     error
}

func run(cfg config) error {
	ctx, stop := signal.NotifyContext(
		context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ctx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	fmt.Printf("driving %s for %s: %d workers, %d points per batch, %d series\n",
		cfg.target, cfg.duration, cfg.workers, cfg.batchSize, cfg.metrics*cfg.hosts)

	// A shared client so connections are reused. Without pooling this would
	// measure TCP and TLS setup rather than the API.
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.workers * 2,
			MaxIdleConnsPerHost: cfg.workers * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	var (
		results = make(chan result, cfg.workers*64)
		wg      sync.WaitGroup
		sent    atomic.Int64
	)

	// A shared ticker paces every worker when a rate is requested, so the
	// aggregate rate is the configured one rather than the per-worker rate
	// multiplied by the worker count.
	var pace <-chan time.Time
	if cfg.rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(cfg.rate))
		defer ticker.Stop()
		pace = ticker.C
	}

	for w := range cfg.workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			drive(ctx, client, cfg, worker, pace, results, &sent)
		}(w)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	return report(collect(results), cfg)
}

func drive(
	ctx context.Context, client *http.Client, cfg config, worker int,
	pace <-chan time.Time, results chan<- result, sent *atomic.Int64,
) {
	// Each worker gets its own source: a shared one would serialise every
	// worker on the same mutex and measure that instead of the API. A weak
	// generator is correct here -- this picks which fake metric name to send,
	// and nothing depends on it being unpredictable.
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker))) //nolint:gosec // synthetic load, not a secret

	for {
		if pace != nil {
			select {
			case <-ctx.Done():
				return
			case <-pace:
			}
		} else if ctx.Err() != nil {
			return
		}

		batch := buildBatch(cfg, rng)
		seq := sent.Add(1)

		started := time.Now()
		status, err := send(ctx, client, cfg, batch, worker, seq)
		latency := time.Since(started)

		if ctx.Err() != nil {
			// The run ended mid-request; that is not a failure worth counting.
			return
		}

		select {
		case results <- result{status: status, latency: latency, err: err}:
		case <-ctx.Done():
			return
		}
	}
}

type point struct {
	Metric string            `json:"metric"`
	Kind   string            `json:"kind"`
	Value  float64           `json:"value"`
	Labels map[string]string `json:"labels,omitempty"`
}

func buildBatch(cfg config, rng *rand.Rand) []byte {
	points := make([]point, cfg.batchSize)

	for i := range points {
		metric := fmt.Sprintf("loadgen.metric_%d", rng.Intn(cfg.metrics))
		host := fmt.Sprintf("host-%02d", rng.Intn(cfg.hosts))

		// A latency-shaped distribution: mostly fast with a heavy tail, so the
		// stored histograms have something realistic to bucket.
		value := 5 + rng.ExpFloat64()*20
		if rng.Float64() < 0.01 {
			value += 500
		}

		points[i] = point{
			Metric: metric,
			Kind:   "histogram",
			Value:  math.Round(value*100) / 100,
			Labels: map[string]string{"host": host, "source": "loadgen"},
		}
	}

	body, err := json.Marshal(map[string]any{"points": points})
	if err != nil {
		// The payload is generated from fixed-shape data; this cannot happen.
		panic(err)
	}
	return body
}

func send(
	ctx context.Context, client *http.Client, cfg config,
	body []byte, worker int, seq int64,
) (int, error) {
	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, cfg.target+"/v1/ingest", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.apiKey)

	if cfg.idempotency {
		req.Header.Set("Idempotency-Key", fmt.Sprintf("loadgen-%d-%d", worker, seq))
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	// Drained and closed so the connection returns to the pool. Skipping this
	// is the classic way a load generator ends up measuring connection setup.
	defer func() { _ = resp.Body.Close() }()

	if _, err := resp.Body.Read(make([]byte, 4096)); err != nil && resp.ContentLength > 0 {
		// A partially read body is fine; the status is what matters here.
		_ = err
	}

	return resp.StatusCode, nil
}

type summary struct {
	latencies []time.Duration
	byStatus  map[int]int
	errors    int
	started   time.Time
	ended     time.Time
}

func collect(results <-chan result) summary {
	s := summary{
		byStatus: make(map[int]int),
		started:  time.Now(),
	}

	for r := range results {
		if r.err != nil {
			s.errors++
			continue
		}
		s.byStatus[r.status]++
		s.latencies = append(s.latencies, r.latency)
	}

	s.ended = time.Now()
	return s
}

func report(s summary, cfg config) error {
	if len(s.latencies) == 0 && s.errors == 0 {
		return fmt.Errorf("no requests completed")
	}

	sort.Slice(s.latencies, func(i, j int) bool { return s.latencies[i] < s.latencies[j] })

	elapsed := s.ended.Sub(s.started).Seconds()
	total := len(s.latencies) + s.errors

	accepted := s.byStatus[http.StatusAccepted]

	fmt.Println()
	fmt.Println("results")
	fmt.Printf("  duration        %.1fs\n", elapsed)
	fmt.Printf("  batches         %d (%.0f/s)\n", total, float64(total)/elapsed)
	fmt.Printf("  points          %d (%.0f/s)\n",
		accepted*cfg.batchSize, float64(accepted*cfg.batchSize)/elapsed)
	fmt.Printf("  transport errs  %d\n", s.errors)

	fmt.Println("  status")
	codes := make([]int, 0, len(s.byStatus))
	for code := range s.byStatus {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		fmt.Printf("    %d           %d\n", code, s.byStatus[code])
	}

	if len(s.latencies) > 0 {
		fmt.Println("  latency (client-side, includes the broker acknowledgement)")
		for _, q := range []struct {
			label string
			p     float64
		}{{"p50", 0.50}, {"p90", 0.90}, {"p99", 0.99}, {"max", 1.0}} {
			fmt.Printf("    %-4s          %s\n", q.label, quantile(s.latencies, q.p).Round(time.Microsecond))
		}
	}

	// A non-zero exit on server errors, so this is usable in a script that
	// needs to know whether the run was clean.
	for code, count := range s.byStatus {
		if code >= 500 {
			return fmt.Errorf("%d responses with status %d", count, code)
		}
	}
	return nil
}

func quantile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(q*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
