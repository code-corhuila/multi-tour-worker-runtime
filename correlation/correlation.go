package correlation

import (
	"context"
	"crypto/rand"
	"fmt"
)

// Header is the HTTP header that carries the id between services.
const Header = "X-Correlation-Id"

type key struct{}

// New returns a fresh random id (UUID v4 layout) for one run.
func New() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // never fails on the supported platforms
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// WithID returns a context carrying id.
func WithID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, key{}, id)
}

// From returns the id kept in ctx, or "" when there is none.
func From(ctx context.Context) string {
	id, _ := ctx.Value(key{}).(string)
	return id
}
