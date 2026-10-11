package httpclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/config"
	"github.com/code-corhuila/multi-tour-worker-runtime/correlation"
	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"
)

const token = "s3cret-service-token"

// fake records the waits and the jitter ceilings instead of sleeping.
type fake struct{ sleeps, ceilings []time.Duration }

func (f *fake) options() []httpclient.Option {
	return []httpclient.Option{
		httpclient.WithBaseWait(200 * time.Millisecond),
		httpclient.WithSleeper(func(_ context.Context, d time.Duration) error {
			f.sleeps = append(f.sleeps, d)
			return nil
		}),
		httpclient.WithJitter(func(max time.Duration) time.Duration {
			f.ceilings = append(f.ceilings, max)
			return max
		}),
	}
}

func newClient(url string, f *fake, extra ...httpclient.Option) *httpclient.Client {
	return httpclient.New(url, config.Secret(token), append(f.options(), extra...)...)
}

func envelope(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": code, "message": msg, "traceId": "trace-1"})
}

func TestC_call_carries_token_and_correlation(t *testing.T) {
	var got http.Header
	var body map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"abc"}`))
	}))
	defer srv.Close()
	ctx := correlation.WithID(context.Background(), "run-42")
	var out struct{ ID string }

	err := newClient(srv.URL, &fake{}).Do(ctx, httpclient.Request{
		Method: http.MethodPost, Path: "/api/v1/things", Body: map[string]string{"a": "b"}, IdempotencyKey: "key-12345678",
	}, &out)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Get("Authorization") != "Bearer "+token {
		t.Errorf("Authorization = %q", got.Get("Authorization"))
	}
	if got.Get(correlation.Header) != "run-42" {
		t.Errorf("%s = %q", correlation.Header, got.Get(correlation.Header))
	}
	if got.Get("Idempotency-Key") != "key-12345678" || got.Get("Content-Type") != "application/json" {
		t.Errorf("headers = %v", got)
	}
	if body["a"] != "b" || out.ID != "abc" {
		t.Errorf("body = %v, out = %+v", body, out)
	}
}

func TestC_retries_only_retryable_answers(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantCalls int32
	}{
		{"400 is final", 400, 1},
		{"401 is final", 401, 1},
		{"404 is final", 404, 1},
		{"429 is retried", 429, 3},
		{"503 is retried", 503, 3},
		{"500 is retried", 500, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				envelope(w, tc.status, "SOME_CODE", "boom")
			}))
			defer srv.Close()

			err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil)

			var apiErr *httpclient.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("want *APIError, got %T %v", err, err)
			}
			if apiErr.Status != tc.status || apiErr.Code != "SOME_CODE" || apiErr.Message != "boom" || apiErr.TraceID != "trace-1" {
				t.Errorf("apiErr = %+v", apiErr)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("calls = %d, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}

	t.Run("full jitter ceiling doubles per retry", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			envelope(w, 503, "SERVICE_UNAVAILABLE", "down")
		}))
		defer srv.Close()
		f := &fake{}
		_ = newClient(srv.URL, f).Get(context.Background(), "/x", nil)
		want := []time.Duration{400 * time.Millisecond, 800 * time.Millisecond}
		if len(f.ceilings) != 2 || f.ceilings[0] != want[0] || f.ceilings[1] != want[1] {
			t.Errorf("ceilings = %v, want %v", f.ceilings, want)
		}
	})

	t.Run("Retry-After is honored", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", "2")
				envelope(w, 429, "RATE_LIMITED", "slow down")
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()
		f := &fake{}
		if err := newClient(srv.URL, f).Get(context.Background(), "/x", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(f.sleeps) != 1 || f.sleeps[0] != 2*time.Second {
			t.Errorf("sleeps = %v, want [2s]", f.sleeps)
		}
	})

	t.Run("a later success ends the retries", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) < 3 {
				envelope(w, 502, "BAD_GATEWAY", "x")
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()
		if err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if calls.Load() != 3 {
			t.Errorf("calls = %d", calls.Load())
		}
	})

	t.Run("network errors are retried then typed", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		url := srv.URL
		srv.Close()
		f := &fake{}

		err := newClient(url, f).Get(context.Background(), "/x", nil)

		var tErr *httpclient.TransportError
		if !errors.As(err, &tErr) {
			t.Fatalf("want *TransportError, got %T %v", err, err)
		}
		if len(f.ceilings) != 2 {
			t.Errorf("retries = %d, want 2", len(f.ceilings))
		}
	})

	t.Run("a cancelled context is not retried", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		f := &fake{}
		err := newClient(srv.URL, f).Get(ctx, "/x", nil)
		if !errors.Is(err, context.Canceled) || len(f.sleeps) != 0 {
			t.Errorf("err = %v, sleeps = %v", err, f.sleeps)
		}
	})
}

func TestC_timeout_is_per_attempt_and_retried(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		<-r.Context().Done()
	}))
	defer srv.Close()

	err := newClient(srv.URL, &fake{}, httpclient.WithTimeout(20*time.Millisecond), httpclient.WithAttempts(2)).
		Get(context.Background(), "/slow", nil)

	var tErr *httpclient.TransportError
	if !errors.As(err, &tErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want a timeout TransportError, got %T %v", err, err)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestC_token_never_appears_in_an_error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		envelope(w, 500, "INTERNAL_ERROR", "failed")
	}))
	defer srv.Close()
	down := httptest.NewServer(http.NotFoundHandler())
	downURL := down.URL
	down.Close()

	for name, url := range map[string]string{"api error": srv.URL, "transport error": downURL} {
		err := newClient(url, &fake{}).Get(context.Background(), "/x", nil)
		if err == nil || strings.Contains(err.Error(), token) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestC_list_is_read_as_data_and_meta(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "page=2&limit=2" {
			t.Errorf("query = %q", r.URL.RawQuery)
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"a"},{"id":"b"}],"meta":{"page":2,"limit":2,"total":5,"totalPages":3}}`))
	}))
	defer srv.Close()
	type item struct{ ID string }

	page, err := httpclient.List[item](context.Background(), newClient(srv.URL, &fake{}), "/api/v1/items?page=2&limit=2")

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Data) != 2 || page.Data[1].ID != "b" {
		t.Errorf("data = %+v", page.Data)
	}
	if page.Meta != (httpclient.Meta{Page: 2, Limit: 2, Total: 5, TotalPages: 3}) {
		t.Errorf("meta = %+v", page.Meta)
	}
}
