package config_test

import (
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
}
