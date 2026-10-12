package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/broker"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/httpapi"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("manager stopped", "reason", err.Error())
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	shutdownTrace, err := telemetry.Init(context.Background(), "dq-manager")
	if err != nil {
		return errors.New("tracing configuration unavailable")
	}
	defer func() {
		ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = shutdownTrace(ctx)
	}()
	workers, closeConnections, err := manager.Connect(cfg)
	if err != nil {
		return err
	}
	defer closeConnections()
	api := control.New(cfg.ControlURL, cfg.ControlToken)
	m := manager.New(api, contract.Scope{InstallationID: cfg.InstallationID, Namespace: cfg.Namespace}, workers, cfg.BetaEnabled, cfg.RemoteOrigin, cfg.DispatchLimit, cfg.PollInterval)
	consumer, err := broker.New(cfg, api, &m.Metrics)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	public := http.NewServeMux()
	public.Handle("/v1/", (&httpapi.API{Manager: m, Identities: cfg.Identities}).Handler())
	public.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	public.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		probeCtx, stop := context.WithTimeout(control.ServiceContext(r.Context()), 2*time.Second)
		defer stop()
		var catalog jsonCatalog
		if err := api.Do(probeCtx, http.MethodGet, "/processors", control.Query(cfg.InstallationID, cfg.Namespace), nil, &catalog); err != nil {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	public.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintf(w, "dq_dispatch_total %d\ndq_dispatch_errors_total %d\ndq_dispatch_duration_seconds_sum %.9f\ndq_dispatch_duration_seconds_count %d\ndq_receipts_total %d\ndq_quarantines_total %d\ndq_lease_renewals_total %d\ndq_poll_errors_total %d\n", m.Metrics.Dispatches.Load(), m.Metrics.DispatchErrors.Load(), float64(m.Metrics.DispatchNanoseconds.Load())/float64(time.Second), m.Metrics.Dispatches.Load(), m.Metrics.Receipts.Load(), m.Metrics.Quarantines.Load(), m.Metrics.Renewals.Load(), m.Metrics.PollErrors.Load())
	})
	public.Handle("/", httpapi.Shell(cfg.UIPath, cfg.RemoteOrigin))
	server := newServer(cfg.HTTPAddr, public)
	remotes := newServer(cfg.RemoteHTTPAddr, httpapi.Remotes(cfg.RemotePath, m))
	errorsCh := make(chan error, 3)
	go m.Run(ctx)
	go func() { errorsCh <- consumer.Run(ctx) }()
	go func() { errorsCh <- server.ListenAndServe() }()
	go func() { errorsCh <- remotes.ListenAndServe() }()
	logger.Info("manager started", "beta_enabled", cfg.BetaEnabled, "approved_workers", len(workers))
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	_ = server.Shutdown(shutdownCtx)
	_ = remotes.Shutdown(shutdownCtx)
	return err
}

type jsonCatalog struct {
	Items []contract.Manifest `json:"items"`
}

func newServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
}
