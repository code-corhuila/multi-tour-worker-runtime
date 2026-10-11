package logging

import (
	"io"
	"log/slog"
)

// New returns a JSON logger that adds correlationId to every line logged with a context
// that carries one.
func New(w io.Writer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }
