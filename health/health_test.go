package health_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/code-corhuila/multi-tour-worker-runtime/health"
)

func TestHandler_answers_200_ok(t *testing.T) {
	rec := httptest.NewRecorder()
	health.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("code %d body %q", rec.Code, rec.Body.String())
	}
}
