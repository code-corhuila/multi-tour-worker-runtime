package logging

import (
	"context"
	"io"
	"log/slog"

	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
)

// New returns a JSON logger that adds correlationId to every line logged with a context
// that carries one.
func New(w io.Writer) *slog.Logger {
	return slog.New(handler{slog.NewJSONHandler(w, nil)})
}

type handler struct{ slog.Handler }

func (h handler) Handle(ctx context.Context, r slog.Record) error {
	if id := correlation.From(ctx); id != "" {
		r.AddAttrs(slog.String("correlationId", id))
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs and WithGroup keep the wrapper, otherwise derived loggers would lose the id.
func (h handler) WithAttrs(a []slog.Attr) slog.Handler { return handler{h.Handler.WithAttrs(a)} }

func (h handler) WithGroup(n string) slog.Handler { return handler{h.Handler.WithGroup(n)} }
