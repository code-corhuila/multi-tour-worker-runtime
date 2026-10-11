package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	workerruntime "github.com/code-corhuila/multi-tour-worker-runtime"
	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
)

// Options configure a Runner. Ticks is a seam for tests; by default an interval ticker is used.
type Options struct {
	Interval   time.Duration
	BatchSize  int
	RunTimeout time.Duration
	Logger     *slog.Logger
	Ticks      <-chan time.Time
}

// Result is what one run did.
type Result struct {
	CorrelationID string
	Processed     int
	Failed        int
}

// Runner drives one Job.
type Runner struct {
	job     workerruntime.Job
	opts    Options
	running atomic.Bool
}

// New builds a Runner for job. It refuses options that would panic or silently misbehave later.
func New(job workerruntime.Job, opts Options) (*Runner, error) {
	switch {
	case job == nil:
		return nil, errors.New("runner: job is required")
	case opts.BatchSize < 1:
		return nil, fmt.Errorf("runner: BatchSize must be positive, got %d", opts.BatchSize)
	case opts.RunTimeout <= 0:
		return nil, fmt.Errorf("runner: RunTimeout must be positive, got %s", opts.RunTimeout)
	case opts.Ticks == nil && opts.Interval <= 0:
		return nil, fmt.Errorf("runner: Interval must be positive when no Ticks are given, got %s", opts.Interval)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Runner{job: job, opts: opts}, nil
}

// RunOnce executes one bounded run under a fresh correlation id. A panic in the job is logged
// and the run ends, so one bad run never kills the worker (Annex D, rule 2).
func (r *Runner) RunOnce(ctx context.Context) (res Result) {
	id := correlation.New()
	ctx = correlation.WithID(ctx, id)
	ctx, cancel := context.WithTimeout(ctx, r.opts.RunTimeout)
	defer cancel()
	res.CorrelationID = id
	defer func() {
		if p := recover(); p != nil {
			r.opts.Logger.ErrorContext(ctx, "run panicked", "job", r.job.Name(), "panic", fmt.Sprint(p))
		}
	}()

	r.opts.Logger.InfoContext(ctx, "run started", "job", r.job.Name(), "batch", r.opts.BatchSize)
	res.Processed, res.Failed = r.job.Run(ctx, r.opts.BatchSize)
	r.opts.Logger.InfoContext(ctx, "run finished", "job", r.job.Name(), "processed", res.Processed, "failed", res.Failed)
	return res
}

// Start runs the job on every tick, skipping a tick while the previous run is alive,
// and returns when ctx is done and the run in flight has ended.
func (r *Runner) Start(ctx context.Context) {
	ticks := r.opts.Ticks
	if ticks == nil {
		t := time.NewTicker(r.opts.Interval)
		defer t.Stop()
		ticks = t.C
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if !r.running.CompareAndSwap(false, true) {
				// Outside any run, so this line has no correlation id.
				r.opts.Logger.WarnContext(ctx, "tick skipped, previous run still alive", "job", r.job.Name())
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer r.running.Store(false)
				r.RunOnce(ctx)
			}()
		}
	}
}
