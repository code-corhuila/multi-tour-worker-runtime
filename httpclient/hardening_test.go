package httpclient_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/httpclient"
)

func failing(status int, calls *atomic.Int32) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		envelope(w, status, "DOWN", "down")
	}))
}

func TestC_attempts_below_one_still_make_the_call(t *testing.T) {
	for _, n := range []int{0, -3} {
		var calls atomic.Int32
		srv := failing(503, &calls)
		err := newClient(srv.URL, &fake{}, httpclient.WithAttempts(n)).Get(context.Background(), "/x", nil)
		srv.Close()
		var apiErr *httpclient.APIError
		if !errors.As(err, &apiErr) || calls.Load() != 3 {
			t.Errorf("attempts=%d: err = %v, calls = %d, want the default 3 attempts", n, err, calls.Load())
		}
	}
}

func TestC_retry_after_is_capped_and_respects_the_deadline(t *testing.T) {
	t.Run("a huge value is capped at 30 s", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Retry-After", "9999999999999")
				envelope(w, 429, "RATE_LIMITED", "slow")
				return
			}
			_, _ = w.Write([]byte(`{}`))
		}))
		defer srv.Close()
		f := &fake{}
		if err := newClient(srv.URL, f).Get(context.Background(), "/x", nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(f.sleeps) != 1 || f.sleeps[0] != 30*time.Second {
			t.Errorf("sleeps = %v, want [30s]", f.sleeps)
		}
	})

	t.Run("a wait beyond the deadline fails fast", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.Header().Set("Retry-After", "5")
			envelope(w, 429, "RATE_LIMITED", "slow")
		}))
		defer srv.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		f := &fake{}

		err := newClient(srv.URL, f).Get(ctx, "/x", nil)

		var apiErr *httpclient.APIError
		if !errors.As(err, &apiErr) || apiErr.Status != 429 || len(f.sleeps) != 0 || calls.Load() != 1 {
			t.Errorf("err = %v, sleeps = %v, calls = %d", err, f.sleeps, calls.Load())
		}
	})
}

func TestC_invalid_options_fall_back_and_the_ceiling_is_clamped(t *testing.T) {
	var calls atomic.Int32
	srv := failing(503, &calls)
	defer srv.Close()

	t.Run("base wait zero uses the default", func(t *testing.T) {
		f := &fake{}
		_ = newClient(srv.URL, f, httpclient.WithBaseWait(0)).Get(context.Background(), "/x", nil)
		if len(f.ceilings) != 2 || f.ceilings[0] != 400*time.Millisecond {
			t.Errorf("ceilings = %v", f.ceilings)
		}
	})

	t.Run("a huge base wait is clamped to 30 s", func(t *testing.T) {
		f := &fake{}
		_ = newClient(srv.URL, f, httpclient.WithBaseWait(1<<60), httpclient.WithAttempts(5)).Get(context.Background(), "/x", nil)
		for _, c := range f.ceilings {
			if c <= 0 || c > 30*time.Second {
				t.Errorf("ceilings = %v", f.ceilings)
			}
		}
	})

	t.Run("timeout zero uses the default", func(t *testing.T) {
		ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
		defer ok.Close()
		if err := newClient(ok.URL, &fake{}, httpclient.WithTimeout(0)).Get(context.Background(), "/x", nil); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestC_redirects_are_not_followed_so_the_token_stays_put(t *testing.T) {
	var elsewhere atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		elsewhere.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
	}))
	defer srv.Close()

	err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil)

	var apiErr *httpclient.APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusFound || elsewhere.Load() != 0 {
		t.Errorf("err = %v, hits elsewhere = %d", err, elsewhere.Load())
	}
}

func TestC_creations_are_retried_only_with_an_idempotency_key(t *testing.T) {
	cases := []struct {
		name string
		req  httpclient.Request
		want int32
	}{
		{"post without key", httpclient.Request{Method: http.MethodPost, Path: "/x"}, 1},
		{"post with key", httpclient.Request{Method: http.MethodPost, Path: "/x", IdempotencyKey: "key-12345678"}, 3},
		{"get", httpclient.Request{Method: http.MethodGet, Path: "/x"}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			srv := failing(503, &calls)
			defer srv.Close()
			_ = newClient(srv.URL, &fake{}).Do(context.Background(), tc.req, nil)
			if calls.Load() != tc.want {
				t.Errorf("calls = %d, want %d", calls.Load(), tc.want)
			}
		})
	}
}

func TestC_foreign_text_and_the_token_never_reach_an_error(t *testing.T) {
	t.Run("a 4xx with an html body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(400)
			_, _ = w.Write([]byte(`<html>pq: duplicate key value violates constraint</html>`))
		}))
		defer srv.Close()
		err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil)
		var apiErr *httpclient.APIError
		if !errors.As(err, &apiErr) || strings.Contains(err.Error(), "duplicate key") || apiErr.Message != "Bad Request" {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("an envelope that echoes the token", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			envelope(w, 400, "VALIDATION_ERROR", "bad credential "+token)
		}))
		defer srv.Close()
		err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil)
		var apiErr *httpclient.APIError
		if !errors.As(err, &apiErr) || strings.Contains(err.Error(), token) || strings.Contains(apiErr.Message, token) {
			t.Errorf("err = %v", err)
		}
	})
}

func TestC_error_text_is_bounded(t *testing.T) {
	long := strings.Repeat("é", 2000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":"VALIDATION_ERROR","message":"` + long + `","details":"` + long + `","traceId":"t"}`))
	}))
	defer srv.Close()
	err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", nil)
	var apiErr *httpclient.APIError
	if !errors.As(err, &apiErr) || len([]rune(apiErr.Message)) > 500 || len([]rune(apiErr.Details)) > 500 {
		t.Errorf("message = %d runes, details = %d runes", len([]rune(apiErr.Message)), len([]rune(apiErr.Details)))
	}
}

func TestC_unusable_success_bodies_are_typed_and_not_retried(t *testing.T) {
	bodies := map[string]string{
		"oversized":    `{"a":"` + strings.Repeat("x", 1<<20+10) + `"}`,
		"invalid json": `{not json`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			var out map[string]any
			err := newClient(srv.URL, &fake{}).Get(context.Background(), "/x", &out)
			var dErr *httpclient.DecodeError
			if !errors.As(err, &dErr) || calls.Load() != 1 {
				t.Errorf("err = %T %v, calls = %d", err, err, calls.Load())
			}
		})
	}
}

func TestC_cancel_during_the_wait_stops_the_call(t *testing.T) {
	var calls atomic.Int32
	srv := failing(503, &calls)
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	c := httpclient.New(srv.URL, "tok", httpclient.WithSleeper(func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}))

	err := c.Get(ctx, "/x", nil)

	if !errors.Is(err, context.Canceled) || calls.Load() != 1 {
		t.Errorf("err = %v, calls = %d", err, calls.Load())
	}
}

func TestC_base_url_and_path_are_joined_with_one_slash(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	for _, tc := range [][2]string{{srv.URL + "/", "/x"}, {srv.URL, "x"}, {srv.URL + "/", "x"}} {
		path = ""
		if err := newClient(tc[0], &fake{}).Get(context.Background(), tc[1], nil); err != nil || path != "/x" {
			t.Errorf("base %q path %q: err = %v, got path %q", tc[0], tc[1], err, path)
		}
	}
}
