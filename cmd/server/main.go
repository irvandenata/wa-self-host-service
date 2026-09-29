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

	"github.com/joho/godotenv"
	"wa-service/internal/api"
	"wa-service/internal/config"
	"wa-service/internal/queue"
	"wa-service/internal/whatsapp"
	"wa-service/internal/worker"
)

func main() {
	_ = godotenv.Load()
	c, err := config.Load()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	q, err := queue.New(c.RedisURL)
	if err != nil {
		slog.Error("redis configuration failed", "error", err)
		os.Exit(1)
	}
	if err = q.Init(ctx); err != nil {
		slog.Error("redis connection failed", "error", err)
		os.Exit(1)
	}
	slog.Info("redis connected")

	wa, err := whatsapp.New(c.SessionDB)
	if err != nil {
		slog.Error("WhatsApp initialization failed", "error", err)
		os.Exit(1)
	}
	wa.Start()
	w := worker.New(q, wa, c.MaxAttempts, c.WorkerConcurrency, c.QueueMax, c.QueueDuration)
	w.Run(ctx)

	server := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", c.Port), Handler: api.New(c, q, wa, func(ctx context.Context) error { return q.RDB.Ping(ctx).Err() }), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		slog.Info("service started", "address", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	w.Wait()
	_ = wa.Close()
	_ = q.RDB.Close()
	slog.Info("service stopped")
}
