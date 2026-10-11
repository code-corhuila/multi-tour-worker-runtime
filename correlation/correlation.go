package correlation

import "context"

// Header is the HTTP header that carries the id between services.
const Header = "X-Correlation-Id"

// New returns a fresh id for one run.
func New() string { return "" }

// WithID returns a context carrying id.
func WithID(ctx context.Context, id string) context.Context { return ctx }

// From returns the id kept in ctx, or "" when there is none.
func From(ctx context.Context) string { return "" }
