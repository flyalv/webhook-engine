package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"webhook-engine/internal/delivery"
	db "webhook-engine/internal/postgres/db"
)

func TestWorkerDeliversEventSuccessfully(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "127.0.0.1")

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	type receivedRequest struct {
		EventID     string
		ContentType string
		Payload     map[string]string
	}

	received := make(chan receivedRequest, 1)

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		received <- receivedRequest{
			EventID:     r.Header.Get("X-Event-ID"),
			ContentType: r.Header.Get("Content-Type"),
			Payload:     payload,
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "hello from webhook engine",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	createdEvent, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.created",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	createdDelivery, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: receiver.URL,
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	service := delivery.NewService(pool, queries)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	worker := New(service, logger)
	worker.pollInterval = 10 * time.Millisecond

	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)

	go func() {
		workerDone <- worker.Run(workerCtx)
	}()

	defer func() {
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Errorf("worker stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	}()

	select {
	case got := <-received:
		if got.EventID != createdEvent.ID.String() {
			t.Errorf("X-Event-ID = %q, want %q", got.EventID, createdEvent.ID.String())
		}

		if got.ContentType != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got.ContentType)
		}

		if got.Payload["message"] != "hello from webhook engine" {
			t.Errorf("payload message = %q, want %q",
				got.Payload["message"], "hello from webhook engine")
		}

	case <-ctx.Done():
		t.Fatal("worker did not deliver HTTP request before timeout")
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		var status string
		var attempts int32
		var attemptCount int64

		err := pool.QueryRow(ctx, `
			SELECT status, attempts_count
			FROM deliveries
			WHERE id = $1
		`, createdDelivery.ID).Scan(&status, &attempts)
		if err != nil {
			t.Fatalf("query delivery status: %v", err)
		}

		err = pool.QueryRow(ctx, `
			SELECT count(*)
			FROM delivery_attempts
			WHERE delivery_id = $1
		`, createdDelivery.ID).Scan(&attemptCount)
		if err != nil {
			t.Fatalf("query delivery attempts: %v", err)
		}

		if status == "succeeded" && attempts == 1 && attemptCount == 1 {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"delivery did not complete: status=%s attempts=%d recorded_attempts=%d",
				status, attempts, attemptCount,
			)
		case <-ticker.C:
		}
	}

	fmt.Println("worker successfully delivered event", createdEvent.ID)
}

func TestWorkerRetriesAfterHTTP500(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "127.0.0.1")

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	var requestCount atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		if requestCount.Add(1) == 1 {
			http.Error(w, "temporary failure", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "retry integration test",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	event, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.retry",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	created, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        event.ID,
		DestinationUrl: receiver.URL,
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	service := delivery.NewService(pool, queries)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	w := New(service, logger)
	w.pollInterval = 10 * time.Millisecond

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)

	go func() {
		workerDone <- w.Run(workerCtx)
	}()

	defer func() {
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Errorf("worker stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		var status string
		var attempts int32
		var attemptCount int64
		var failedCount int64
		var succeededCount int64

		err := pool.QueryRow(ctx, `
			SELECT status, attempts_count
			FROM deliveries
			WHERE id = $1
		`, created.ID).Scan(&status, &attempts)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		err = pool.QueryRow(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE http_status = 500),
			       count(*) FILTER (WHERE http_status = 204)
			FROM delivery_attempts
			WHERE delivery_id = $1
		`, created.ID).Scan(&attemptCount, &failedCount, &succeededCount)
		if err != nil {
			t.Fatalf("query delivery attempts: %v", err)
		}

		if status == "succeeded" &&
			attempts == 2 &&
			attemptCount == 2 &&
			failedCount == 1 &&
			succeededCount == 1 {
			break
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"retry flow did not complete: status=%s attempts=%d history=%d http500=%d http204=%d requests=%d",
				status, attempts, attemptCount, failedCount, succeededCount, requestCount.Load(),
			)
		case <-ticker.C:
		}
	}

	if got := requestCount.Load(); got != 2 {
		t.Errorf("HTTP request count = %d, want 2", got)
	}
}

func TestWorkerMarksDeliveryDeadAfterMaxAttempts(t *testing.T) {
	t.Setenv("WEBHOOK_ALLOWED_PRIVATE_HOSTS", "127.0.0.1")

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	var requestCount atomic.Int32

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		requestCount.Add(1)
		http.Error(w, "receiver unavailable", http.StatusInternalServerError)
	}))
	defer receiver.Close()

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "dead letter integration test",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	createdEvent, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.dead",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	createdDelivery, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: receiver.URL,
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	service := delivery.NewService(pool, queries)
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))

	w := New(service, logger)
	w.pollInterval = 10 * time.Millisecond

	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)

	go func() {
		workerDone <- w.Run(workerCtx)
	}()

	defer func() {
		stopWorker()
		select {
		case err := <-workerDone:
			if err != nil {
				t.Errorf("worker stopped with error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("worker did not stop after cancellation")
		}
	}()

	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		var status string
		var attempts int32
		var historyCount int64
		var failedCount int64

		err := pool.QueryRow(ctx, `
			SELECT status, attempts_count
			FROM deliveries
			WHERE id = $1
		`, createdDelivery.ID).Scan(&status, &attempts)
		if err != nil {
			t.Fatalf("query delivery: %v", err)
		}

		err = pool.QueryRow(ctx, `
			SELECT count(*),
			       count(*) FILTER (WHERE http_status = 500)
			FROM delivery_attempts
			WHERE delivery_id = $1
		`, createdDelivery.ID).Scan(&historyCount, &failedCount)
		if err != nil {
			t.Fatalf("query delivery attempts: %v", err)
		}

		if status == "dead" {
			if attempts != 5 {
				t.Errorf("attempts_count = %d, want 5", attempts)
			}
			if historyCount != 5 {
				t.Errorf("attempt history count = %d, want 5", historyCount)
			}
			if failedCount != 5 {
				t.Errorf("HTTP 500 count = %d, want 5", failedCount)
			}
			if got := requestCount.Load(); got != 5 {
				t.Errorf("HTTP request count = %d, want 5", got)
			}
			return
		}

		select {
		case <-ctx.Done():
			t.Fatalf(
				"delivery did not become dead: status=%s attempts=%d history=%d failed=%d requests=%d",
				status,
				attempts,
				historyCount,
				failedCount,
				requestCount.Load(),
			)
		case <-ticker.C:
		}
	}
}

func TestClaimExpiredDelivery(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "expired lease test",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	createdEvent, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.lease_expired",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	createdDelivery, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: "http://localhost:9999/webhook",
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	_, err = pool.Exec(ctx, `
		UPDATE deliveries
		SET status = 'processing',
		    locked_until = now() - interval '1 second'
		WHERE id = $1
	`, createdDelivery.ID)
	if err != nil {
		t.Fatalf("expire delivery lease: %v", err)
	}

	service := delivery.NewService(pool, queries)

	claimed, err := service.ClaimDelivery(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("claim expired delivery: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected expired delivery to be claimed, got nil")
	}

	wantID := createdDelivery.ID.String()
	if claimed.ID != wantID {
		t.Fatalf("claimed delivery ID = %s, want %s", claimed.ID, wantID)
	}

	if claimed.AttemptsCount != 0 {
		t.Errorf("attempts count = %d, want 0", claimed.AttemptsCount)
	}

	var status string
	var lockedUntil *time.Time
	err = pool.QueryRow(ctx, `
		SELECT status, locked_until
		FROM deliveries
		WHERE id = $1
	`, createdDelivery.ID).Scan(&status, &lockedUntil)
	if err != nil {
		t.Fatalf("query delivery state: %v", err)
	}

	if status != "processing" {
		t.Errorf("status = %q, want processing", status)
	}
	if lockedUntil == nil || !lockedUntil.After(time.Now()) {
		t.Errorf("expected a new lease in the future, got %v", lockedUntil)
	}
}

func TestConcurrentClaimDelivery(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create database pool: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	var databaseName string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName); err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	if _, err := pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`); err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "concurrent claim test",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	createdEvent, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.concurrent",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	createdDelivery, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: "http://localhost:9999/webhook",
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	service := delivery.NewService(pool, queries)

	type claimResult struct {
		task *delivery.ClaimedDelivery
		err  error
	}

	start := make(chan struct{})
	results := make(chan claimResult, 2)

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			<-start

			task, err := service.ClaimDelivery(ctx, 30*time.Second)
			results <- claimResult{task: task, err: err}
		}()
	}

	close(start)

	wg.Wait()
	close(results)

	var claimedIDs []string
	nilClaims := 0

	for result := range results {
		if result.err != nil {
			t.Fatalf("claim delivery: %v", result.err)
		}

		if result.task == nil {
			nilClaims++
			continue
		}

		claimedIDs = append(claimedIDs, result.task.ID)
	}

	if len(claimedIDs) != 1 {
		t.Fatalf("successful claims = %d, want 1; IDs = %v", len(claimedIDs), claimedIDs)
	}

	if claimedIDs[0] != createdDelivery.ID.String() {
		t.Errorf(
			"claimed ID = %s, want %s",
			claimedIDs[0],
			createdDelivery.ID.String(),
		)
	}

	if nilClaims != 1 {
		t.Errorf("empty claims = %d, want 1", nilClaims)
	}
}
