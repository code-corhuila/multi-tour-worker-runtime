package runtime

import (
	"context"
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
		if err := handle(ctx, item); err != nil {
			failed++
			log.WarnContext(ctx, "item failed, left for the next run", "error", err)
			continue
		}
		processed++
	}
	return processed, failed
}
