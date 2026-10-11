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

// syncBuffer is a log sink safe for the runner goroutine and the test to share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func newRunner(t *testing.T, job wrt.Job, opts runner.Options) *runner.Runner {
	t.Helper()
	r, err := runner.New(job, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

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
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 100, RunTimeout: 100 * time.Millisecond, Logger: logging.New(&bytes.Buffer{})})

	start := time.Now()
	res := r.RunOnce(context.Background())

	if asked != 100 {
		t.Fatalf("job was asked for %d items, want 100", asked)
	}
	if res.Processed >= 100 {
		t.Fatalf("processed = %d, want fewer than 100 once the timeout passed", res.Processed)
	}
	if time.Since(start) > 500*time.Millisecond {
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
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 100, RunTimeout: time.Second, Logger: logging.New(&bytes.Buffer{})})

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
	var logs syncBuffer
	ticks := make(chan time.Time)
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 1, RunTimeout: time.Minute, Ticks: ticks, Logger: logging.New(&logs)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { r.Start(ctx); close(done) }()
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
	tick()
	tick()
	// Release only after the runner logged that it skipped both ticks, so no timing is assumed.
	deadline := time.Now().Add(time.Second)
	for strings.Count(logs.String(), "tick skipped") < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected two skipped ticks, log:\n%s", logs.String())
		}
		time.Sleep(time.Millisecond)
	}
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
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 1, RunTimeout: time.Second, Logger: logging.New(&buf)})

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

func TestNew_refuses_unusable_options(t *testing.T) {
	job := fakeJob(func(context.Context, int) (int, int) { return 0, 0 })
	ok := runner.Options{Interval: time.Second, BatchSize: 1, RunTimeout: time.Second}
	cases := map[string]func(o runner.Options) (wrt.Job, runner.Options){
		"interval zero without ticks": func(o runner.Options) (wrt.Job, runner.Options) { o.Interval = 0; return job, o },
		"interval negative":           func(o runner.Options) (wrt.Job, runner.Options) { o.Interval = -time.Second; return job, o },
		"run timeout zero":            func(o runner.Options) (wrt.Job, runner.Options) { o.RunTimeout = 0; return job, o },
		"batch size zero":             func(o runner.Options) (wrt.Job, runner.Options) { o.BatchSize = 0; return job, o },
		"batch size negative":         func(o runner.Options) (wrt.Job, runner.Options) { o.BatchSize = -1; return job, o },
		"nil job":                     func(o runner.Options) (wrt.Job, runner.Options) { return nil, o },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			j, o := mutate(ok)
			if _, err := runner.New(j, o); err == nil {
				t.Fatal("New accepted the options")
			}
		})
	}
	t.Run("interval zero with ticks is fine", func(t *testing.T) {
		o := ok
		o.Interval = 0
		o.Ticks = make(chan time.Time)
		if _, err := runner.New(job, o); err != nil {
			t.Fatalf("New: %v", err)
		}
	})
}

func TestA_panic_in_an_item_is_a_failed_item(t *testing.T) {
	var logs syncBuffer
	ctx := correlation.WithID(context.Background(), "run-1")
	var processed, failed int
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("the panic escaped Each: %v", p)
			}
		}()
		processed, failed = wrt.Each(ctx, logging.New(&logs), []int{1, 2, 3}, func(_ context.Context, n int) error {
			if n == 2 {
				panic("kaboom")
			}
			return nil
		})
	}()

	if processed != 2 || failed != 1 {
		t.Fatalf("processed=%d failed=%d, want 2 and 1", processed, failed)
	}
	if out := logs.String(); !strings.Contains(out, "kaboom") || !strings.Contains(out, "run-1") {
		t.Fatalf("the panic must be logged with the correlation id, log:\n%s", out)
	}
}

func TestA_panic_in_a_job_does_not_escape_the_run(t *testing.T) {
	var logs syncBuffer
	job := fakeJob(func(context.Context, int) (int, int) { panic("kaboom") })
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 1, RunTimeout: time.Second, Logger: logging.New(&logs)})

	var res runner.Result
	func() {
		defer func() {
			if p := recover(); p != nil {
				t.Errorf("the panic escaped RunOnce: %v", p)
			}
		}()
		res = r.RunOnce(context.Background())
	}()

	if res.CorrelationID == "" || !strings.Contains(logs.String(), "kaboom") || !strings.Contains(logs.String(), res.CorrelationID) {
		t.Fatalf("result %+v, log:\n%s", res, logs.String())
	}
}

func TestA_a_panicking_run_releases_the_schedule(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	job := fakeJob(func(context.Context, int) (int, int) {
		mu.Lock()
		calls++
		n := calls
		mu.Unlock()
		if n == 1 {
			panic("kaboom")
		}
		return 0, 0
	})
	ticks := make(chan time.Time)
	r := newRunner(t, job, runner.Options{Interval: time.Hour, BatchSize: 1, RunTimeout: time.Second, Ticks: ticks, Logger: logging.New(&bytes.Buffer{})})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Start(ctx); close(done) }()

	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		n := calls
		mu.Unlock()
		if n >= 2 || time.Now().After(deadline) {
			break
		}
		select {
		case ticks <- time.Now():
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if calls < 2 {
		t.Fatalf("job ran %d times, the run after a panic never started", calls)
	}
}
