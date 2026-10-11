package workerruntime

import (
	"context"
	"fmt"
	"log/slog"
)

// Job is the input port: a worker's use case implements it and the runner drives it.
type Job interface {
	Name() string
	// Run handles at most batch items and stops when ctx is done.
	Run(ctx context.Context, batch int) (processed, failed int)
}

// Each applies handle to every item. A failed item is counted and logged, never stops the batch;
// only a done context does, so RUN_TIMEOUT bounds the work.
func Each[T any](ctx context.Context, log *slog.Logger, items []T, handle func(context.Context, T) error) (processed, failed int) {
	for _, item := range items {
		if ctx.Err() != nil {
			return processed, failed
		}
		if err := safely(ctx, handle, item); err != nil {
			failed++
			log.WarnContext(ctx, "item failed, left for the next run", "error", err)
			continue
		}
		processed++
	}
	return processed, failed
}

// safely turns a panic in handle into an error, so one bad item cannot kill the worker.
func safely[T any](ctx context.Context, handle func(context.Context, T) error, item T) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic: %v", p)
		}
	}()
	return handle(ctx, item)
}
