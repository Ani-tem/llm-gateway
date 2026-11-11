package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"github.com/Ani-tem/llm-gateway/internal/cache"
	"github.com/Ani-tem/llm-gateway/internal/config"
	"github.com/Ani-tem/llm-gateway/internal/ratelimit"
	"github.com/Ani-tem/llm-gateway/internal/route"
	"github.com/Ani-tem/llm-gateway/internal/server"
	"github.com/Ani-tem/llm-gateway/internal/usage"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "keygen" {
		keygen()
		return
	}
	configPath := flag.String("config", "config.json", "path to config file")
	listen := flag.String("listen", "", "listen address (overrides config)")
	flag.Parse()

	if err := run(*configPath, *listen); err != nil {
		slog.Error("gateway stopped", "err", err)
		os.Exit(1)
	}
}

func run(configPath, listen string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	if listen != "" {
		cfg.Listen = listen
	}
	providers, err := cfg.BuildProviders()
	if err != nil {
		return err
	}

	routes, err := cfg.BuildRoutes(providers)
	if err != nil {
		return err
	}
	tenants, err := cfg.BuildTenants()
	if err != nil {
		return err
	}
	// The client connects lazily, so a Redis that is down at startup does not
	// stop the gateway; the limiter fails open until it is back.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	var responses *cache.Cache
	if cfg.CacheTTLSeconds > 0 {
		responses = cache.New(rdb, time.Duration(cfg.CacheTTLSeconds)*time.Second)
	}

	serverCfg := server.Config{
		Router:  route.New(providers, routes),
		Tenants: tenants,
		Limiter: ratelimit.New(rdb),
		Cache:   responses,
	}
	if cfg.PostgresURL != "" {
		startCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err := usage.OpenPostgres(startCtx, cfg.PostgresURL)
		cancel()
		if err != nil {
			return err
		}
		defer store.Close()
		recorder := usage.NewRecorder(store)
		prometheus.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "gateway_usage_events_dropped_total",
			Help: "Usage events lost to a full queue or a failed database write.",
		}, func() float64 { return float64(recorder.Dropped()) }))
		// Deferred calls run last-in-first-out: by the time this runs the
		// HTTP server has stopped, so every event is already queued, and the
		// store is still open to receive the final flush.
		defer recorder.Close()
		serverCfg.Usage, serverCfg.UsageReader = recorder, store
	}

	srv := &http.Server{
		Addr:    cfg.Listen,
		Handler: server.New(serverCfg),
		// Bounds slow-header attacks. There is deliberately no WriteTimeout:
		// it would kill long streaming responses.
		ReadHeaderTimeout: 10 * time.Second,
	}

	// On Ctrl-C or SIGTERM, stop accepting connections and give in-flight
	// requests time to finish, which is what a rolling deploy needs.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", promhttp.Handler())
	metricsSrv := &http.Server{Addr: cfg.MetricsListen, Handler: metricsMux, ReadHeaderTimeout: 10 * time.Second}
	defer metricsSrv.Close()

	errc := make(chan error, 2)
	go func() { errc <- srv.ListenAndServe() }()
	go func() { errc <- metricsSrv.ListenAndServe() }()
	slog.Info("gateway listening", "addr", cfg.Listen, "providers", len(providers), "tenants", len(tenants), "redis", cfg.RedisAddr, "metrics", cfg.MetricsListen)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// keygen prints a new random API key and the hash to put in config.json.
// The key itself is shown once and stored nowhere.
func keygen() {
	buf := make([]byte, 24)
	rand.Read(buf) // never fails (crypto/rand panics instead since Go 1.24)
	key := "sk-gw-" + hex.EncodeToString(buf)
	fmt.Printf("api key (give to the client, shown once): %s\n", key)
	fmt.Printf("key_sha256 (put in config.json):          %s\n", server.HashKey(key))
}
