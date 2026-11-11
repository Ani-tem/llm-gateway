// Package config loads the gateway's JSON config file.
package config

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/Ani-tem/llm-gateway/internal/provider"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/route"
	"github.com/Ani-tem/llm-gateway/internal/server"
)

type Config struct {
	Listen    string `json:"listen"`
	RedisAddr string `json:"redis_addr"`
	// Address for the Prometheus /metrics endpoint. It is a separate port so
	// internal metrics are never exposed on the public API listener.
	MetricsListen string `json:"metrics_listen"`
	// Postgres connection string for usage metering; empty disables it. The
	// DATABASE_URL environment variable overrides it.
	PostgresURL string `json:"postgres_url"`
	// How long responses are cached; 0 disables the cache.
	CacheTTLSeconds int        `json:"cache_ttl_seconds"`
	Providers       []Provider `json:"providers"`
	Tenants         []Tenant   `json:"tenants"`
	Routes          []Route    `json:"routes"`
}

// Route is an alias clients can use as "model". Targets are "provider/model"
// strings, tried in order until one succeeds.
type Route struct {
	Name    string   `json:"name"`
	Targets []string `json:"targets"`
}

type Tenant struct {
	Name string `json:"name"`
	// SHA-256 hex of the API key; generate a pair with "gateway keygen".
	KeySHA256 string `json:"key_sha256"`
	RPM       int    `json:"rpm"`
	Burst     int    `json:"burst"` // defaults to rpm
}

type Provider struct {
	Name string `json:"name"`
	Type string `json:"type"` // "mock" or "openai"

	// openai only. The key is named by env var so it never lives in the file.
	BaseURL   string `json:"base_url"`
	APIKeyEnv string `json:"api_key_env"`

	// mock only.
	LatencyMS int     `json:"latency_ms"`
	FailRate  float64 `json:"fail_rate"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields() // a typo'd key should fail loudly, not be ignored
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if env := os.Getenv("DATABASE_URL"); env != "" {
		c.PostgresURL = env
	}
	if c.MetricsListen == "" {
		c.MetricsListen = ":9102"
	}
	if c.RedisAddr == "" {
		c.RedisAddr = "localhost:6379"
	}
	return &c, nil
}

// BuildProviders turns config entries into live providers. A provider whose
// API key env var is unset is skipped, so one config file works on machines
// with different keys.
func (c *Config) BuildProviders() (map[string]provider.Provider, error) {
	out := make(map[string]provider.Provider)
	for _, pc := range c.Providers {
		if pc.Name == "" || strings.Contains(pc.Name, "/") {
			return nil, fmt.Errorf("provider name %q must be non-empty and contain no slash", pc.Name)
		}
		if _, dup := out[pc.Name]; dup {
			return nil, fmt.Errorf("duplicate provider %q", pc.Name)
		}
		switch pc.Type {
		case "mock":
			out[pc.Name] = &provider.Mock{Latency: time.Duration(pc.LatencyMS) * time.Millisecond, FailRate: pc.FailRate}
		case "openai":
			if pc.BaseURL == "" {
				return nil, fmt.Errorf("provider %q: base_url is required", pc.Name)
			}
			key := ""
			if pc.APIKeyEnv != "" {
				if key = os.Getenv(pc.APIKeyEnv); key == "" {
					slog.Warn("skipping provider: API key env var not set", "provider", pc.Name, "env", pc.APIKeyEnv)
					continue
				}
			}
			out[pc.Name] = provider.NewOpenAI(pc.Name, pc.BaseURL, key)
		default:
			return nil, fmt.Errorf("provider %q: unknown type %q", pc.Name, pc.Type)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no providers configured")
	}
	return out, nil
}

// BuildTenants returns tenants keyed by API key hash, as the server wants.
func (c *Config) BuildTenants() (map[string]server.Tenant, error) {
	out := make(map[string]server.Tenant)
	for _, tc := range c.Tenants {
		if tc.Name == "" {
			return nil, fmt.Errorf("tenant name is required")
		}
		hash := strings.ToLower(tc.KeySHA256)
		if b, err := hex.DecodeString(hash); err != nil || len(b) != 32 {
			return nil, fmt.Errorf("tenant %q: key_sha256 must be 64 hex characters", tc.Name)
		}
		if _, dup := out[hash]; dup {
			return nil, fmt.Errorf("tenant %q: key already used by another tenant", tc.Name)
		}
		if tc.RPM <= 0 {
			return nil, fmt.Errorf("tenant %q: rpm must be positive", tc.Name)
		}
		burst := tc.Burst
		if burst <= 0 {
			burst = tc.RPM
		}
		out[hash] = server.Tenant{Name: tc.Name, Limit: ratelimit.Limit{PerMinute: tc.RPM, Burst: burst}}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no tenants configured: run \"gateway keygen\" and add one")
	}
	return out, nil
}

// BuildRoutes resolves route targets against the providers that are actually
// available. A target whose provider was skipped (no API key) is dropped, so
// a route degrades to whatever this machine can reach.
func (c *Config) BuildRoutes(providers map[string]provider.Provider) (map[string][]route.Target, error) {
	out := make(map[string][]route.Target)
	for _, rc := range c.Routes {
		if rc.Name == "" || strings.Contains(rc.Name, "/") {
			return nil, fmt.Errorf("route name %q must be non-empty and contain no slash", rc.Name)
		}
		if _, dup := out[rc.Name]; dup {
			return nil, fmt.Errorf("duplicate route %q", rc.Name)
		}
		var targets []route.Target
		for _, t := range rc.Targets {
			name, model, _ := strings.Cut(t, "/")
			if model == "" {
				return nil, fmt.Errorf("route %q: target %q must be \"provider/model\"", rc.Name, t)
			}
			if _, ok := providers[name]; !ok {
				slog.Warn("dropping route target: provider not available", "route", rc.Name, "target", t)
				continue
			}
			targets = append(targets, route.Target{Provider: name, Model: model})
		}
		if len(targets) == 0 {
			return nil, fmt.Errorf("route %q has no available targets", rc.Name)
		}
		out[rc.Name] = targets
	}
	return out, nil
}
