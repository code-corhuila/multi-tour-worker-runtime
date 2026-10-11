package runner

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	wrt "github.com/code-corhuila/multi-tour-worker-runtime"
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
	job     wrt.Job
	opts    Options
	running atomic.Bool
}

// New builds a Runner for job.
func New(job wrt.Job, opts Options) *Runner {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return &Runner{job: job, opts: opts}
}

// RunOnce executes one bounded run under a fresh correlation id.
func (r *Runner) RunOnce(ctx context.Context) Result {
	id := correlation.New()
	ctx = correlation.WithID(ctx, id)
	ctx, cancel := context.WithTimeout(ctx, r.opts.RunTimeout)
	defer cancel()

	r.opts.Logger.InfoContext(ctx, "run started", "job", r.job.Name(), "batch", r.opts.BatchSize)
	processed, failed := r.job.Run(ctx, r.opts.BatchSize)
	r.opts.Logger.InfoContext(ctx, "run finished", "job", r.job.Name(), "processed", processed, "failed", failed)
	return Result{CorrelationID: id, Processed: processed, Failed: failed}
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
