package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	wrt "github.com/code-corhuila/multi-tour-worker-runtime"
	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
	"github.com/code-corhuila/multi-tour-worker-runtime/logging"
	"github.com/code-corhuila/multi-tour-worker-runtime/runner"
)

// fakeJob adapts a function to the Job port.
type fakeJob func(ctx context.Context, batch int) (int, int)

func (fakeJob) Name() string { return "fake" }

func (f fakeJob) Run(ctx context.Context, batch int) (int, int) { return f(ctx, batch) }

func TestA_run_asks_batch_size_and_stops_at_timeout(t *testing.T) {
	var asked int
	job := fakeJob(func(ctx context.Context, batch int) (int, int) {
		asked = batch
		items := make([]int, batch)
		return wrt.Each(ctx, logging.New(&bytes.Buffer{}), items, func(ctx context.Context, _ int) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
				return nil
			}
		})
	})
	r := runner.New(job, runner.Options{BatchSize: 100, RunTimeout: 100 * time.Millisecond, Logger: logging.New(&bytes.Buffer{})})

	start := time.Now()
	res := r.RunOnce(context.Background())

	if asked != 100 {
		t.Fatalf("job was asked for %d items, want 100", asked)
	}
	if res.Processed >= 100 {
		t.Fatalf("processed = %d, want fewer than 100 once the timeout passed", res.Processed)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("run lasted %s, the timeout did not stop it", time.Since(start))
	}
}

func TestA_failed_item_is_counted_and_batch_continues(t *testing.T) {
	var reached []int
	job := fakeJob(func(ctx context.Context, batch int) (int, int) {
		return wrt.Each(ctx, logging.New(&bytes.Buffer{}), []int{1, 2, 3}, func(_ context.Context, n int) error {
			reached = append(reached, n)
			if n == 2 {
				return errors.New("boom")
			}
			return nil
		})
	})
	r := runner.New(job, runner.Options{BatchSize: 100, RunTimeout: time.Second, Logger: logging.New(&bytes.Buffer{})})

	res := r.RunOnce(context.Background())

	if res.Processed != 2 || res.Failed != 1 {
		t.Fatalf("processed=%d failed=%d, want 2 and 1", res.Processed, res.Failed)
	}
	if len(reached) != 3 {
		t.Fatalf("reached %v, the third item must be reached", reached)
	}
}

func TestA_next_run_does_not_start_while_one_is_alive(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	job := fakeJob(func(ctx context.Context, batch int) (int, int) {
		mu.Lock()
		calls++
		mu.Unlock()
		started <- struct{}{}
		<-release
		return 0, 0
	})
	ticks := make(chan time.Time)
	r := runner.New(job, runner.Options{BatchSize: 1, RunTimeout: time.Minute, Ticks: ticks, Logger: logging.New(&bytes.Buffer{})})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Start(ctx); close(done) }()

	defer cancel()
	tick := func() {
		t.Helper()
		select {
		case ticks <- time.Now():
		case <-time.After(time.Second):
			t.Fatal("the runner does not read its ticks")
		}
	}

	tick()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("the first tick did not start a run")
	}
	tick() // arrives while the first run is alive
	tick() // accepted only after the previous tick was handled
	close(release)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("job ran %d times, want 1", calls)
	}
}

func TestC_log_lines_carry_the_run_correlation(t *testing.T) {
	var buf bytes.Buffer
	var seen string
	job := fakeJob(func(ctx context.Context, batch int) (int, int) {
		seen = correlation.From(ctx)
		return 0, 0
	})
	r := runner.New(job, runner.Options{BatchSize: 1, RunTimeout: time.Second, Logger: logging.New(&buf)})

	res := r.RunOnce(context.Background())

	if seen == "" || seen != res.CorrelationID {
		t.Fatalf("job saw %q, result has %q", seen, res.CorrelationID)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) == 0 || lines[0] == "" {
		t.Fatal("the run wrote no log line")
	}
	for _, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		if rec["correlationId"] != seen {
			t.Fatalf("line %q lacks correlationId %q", line, seen)
		}
	}
}
