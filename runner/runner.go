package runner

import (
	"context"
	"log/slog"
	"time"

	wrt "github.com/code-corhuila/multi-tour-worker-runtime"
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
type Runner struct{}

// New builds a Runner for job.
func New(job wrt.Job, opts Options) *Runner { return &Runner{} }

// RunOnce executes one bounded run under a fresh correlation id.
func (r *Runner) RunOnce(ctx context.Context) Result { return Result{} }

// Start runs the job on every tick, skipping a tick while the previous run is alive,
// and returns when ctx is done and the run in flight has ended.
func (r *Runner) Start(ctx context.Context) {}
