package config_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/code-corhuila/multi-tour-worker-runtime/config"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestC_startup_fails_without_service_token(t *testing.T) {
	if _, err := config.Load(env(map[string]string{})); err == nil {
		t.Fatal("Load accepted a missing SERVICE_TOKEN")
	}
}

func TestLoad_applies_declared_defaults(t *testing.T) {
	c, err := config.Load(env(map[string]string{"SERVICE_TOKEN": "t"}))
	if err != nil {
		t.Fatal(err)
	}
	if c.BatchSize != 100 || c.RunTimeout != 30*time.Second || c.HTTPTimeout != 5*time.Second || c.HTTPAttempts != 3 {
		t.Fatalf("defaults = %+v", c)
	}
	if c.ServiceToken.Reveal() != "t" {
		t.Fatal("Reveal does not return the token")
	}
}

func TestLoad_refuses_values_that_are_not_positive(t *testing.T) {
	for _, name := range []string{"BATCH_SIZE", "HTTP_ATTEMPTS"} {
		for _, v := range []string{"0", "-1", "abc"} {
			t.Run(name+"="+v, func(t *testing.T) {
				if _, err := config.Load(env(map[string]string{"SERVICE_TOKEN": "t", name: v})); err == nil {
					t.Fatal("Load accepted it")
				}
			})
		}
	}
	for _, name := range []string{"RUN_TIMEOUT", "HTTP_TIMEOUT"} {
		for _, v := range []string{"0s", "-5s", "abc", "30"} {
			t.Run(name+"="+v, func(t *testing.T) {
				if _, err := config.Load(env(map[string]string{"SERVICE_TOKEN": "t", name: v})); err == nil {
					t.Fatal("Load accepted it")
				}
			})
		}
	}
}

func TestSecret_never_prints_the_token(t *testing.T) {
	const token = "s3cret-token-value"
	c, err := config.Load(env(map[string]string{"SERVICE_TOKEN": token}))
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{
		fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c),
		fmt.Sprintf("%v", &c), fmt.Sprintf("%s", c.ServiceToken), fmt.Sprintf("%#v", c.ServiceToken),
	} {
		if strings.Contains(out, token) {
			t.Fatalf("printing leaked the token: %s", out)
		}
	}
}

func TestLoad_errors_never_contain_the_token(t *testing.T) {
	const token = "s3cret-token-value"
	_, err := config.Load(env(map[string]string{"SERVICE_TOKEN": token, "BATCH_SIZE": "abc"}))
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("err = %v", err)
	}
}
