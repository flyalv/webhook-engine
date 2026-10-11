package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"webhook-engine/internal/config"
	"webhook-engine/internal/delivery"
	"webhook-engine/internal/event"
	"webhook-engine/internal/httpserver"
	"webhook-engine/internal/logger"
	"webhook-engine/internal/postgres"
	"webhook-engine/internal/postgres/db"
	"webhook-engine/internal/worker"
)

func main() {
	cfg := config.New()
	log := logger.New(cfg.LogLevel)

	if cfg.DatabaseURL == "" {
		log.Error("DATABASE_URL is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	pool, err := postgres.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Error("database connection failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	gin.SetMode(cfg.GinMode)

	queries := db.New(pool)

	// Event API.
	eventService := event.NewService(pool, queries)
	eventHandler := event.NewHandler(eventService, log)
	router := httpserver.NewRouter(log, eventHandler)

	// Background delivery worker.
	deliveryService := delivery.NewService(pool, queries)
	deliveryWorker := worker.New(deliveryService, log)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: router,
	}

	workerDone := make(chan struct{})

	go func() {
		defer close(workerDone)

		log.Info("delivery worker started")

		if err := deliveryWorker.Run(ctx); err != nil {
			log.Error("delivery worker stopped with error", "error", err)
			stop()
		}

		log.Info("delivery worker stopped")
	}()

	go func() {
		log.Info("server started", "port", cfg.Port)

		if err := server.ListenAndServe(); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()

	log.Info("shutdown started")

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Error("server shutdown failed", "error", err)
	}

	// Wait until the worker finishes its current operation
	// or the shutdown deadline expires.
	select {
	case <-workerDone:
	case <-shutdownCtx.Done():
		log.Error("timed out waiting for delivery worker")
	}

	log.Info("server stopped")
}
