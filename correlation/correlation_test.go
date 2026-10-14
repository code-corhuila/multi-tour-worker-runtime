package correlation_test

import (
	"context"
	"regexp"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNew_has_the_uuid_v4_layout_and_is_unique(t *testing.T) {
	a, b := correlation.New(), correlation.New()
	if !uuidV4.MatchString(a) || a == b {
		t.Fatalf("ids %q and %q", a, b)
	}
}

func TestContext_round_trip(t *testing.T) {
	if got := correlation.From(context.Background()); got != "" {
		t.Fatalf("empty context gave %q", got)
	}
	if got := correlation.From(correlation.WithID(context.Background(), "x")); got != "x" {
		t.Fatalf("got %q", got)
	}
}
