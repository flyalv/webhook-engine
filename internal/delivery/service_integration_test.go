package delivery

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	db "webhook-engine/internal/postgres/db"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClaimDelivery(t *testing.T) {
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
	err = pool.QueryRow(ctx, "SELECT current_database()").Scan(&databaseName)
	if err != nil {
		t.Fatalf("get database name: %v", err)
	}
	if databaseName != "webhook_engine_test" {
		t.Fatalf("refusing to clean database %q", databaseName)
	}

	_, err = pool.Exec(ctx, `
		TRUNCATE TABLE delivery_attempts, deliveries, events
		RESTART IDENTITY CASCADE
	`)
	if err != nil {
		t.Fatalf("clean test database: %v", err)
	}

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "integration test",
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

	const destinationURL = "http://localhost:9999/webhook"

	createdDelivery, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: destinationURL,
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	service := NewService(pool, queries)

	claimed, err := service.ClaimDelivery(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("claim delivery: %v", err)
	}
	if claimed == nil {
		t.Fatal("expected a delivery, got nil")
	}

	wantDeliveryID := uuidToString(createdDelivery.ID)
	if claimed.ID != wantDeliveryID {
		t.Fatalf("claimed delivery ID = %s, want %s", claimed.ID, wantDeliveryID)
	}

	if claimed.EventID != uuidToString(createdEvent.ID) {
		t.Errorf(
			"claimed event ID = %s, want %s",
			claimed.EventID,
			uuidToString(createdEvent.ID),
		)
	}

	if claimed.DestinationURL != destinationURL {
		t.Errorf(
			"destination URL = %s, want %s",
			claimed.DestinationURL,
			destinationURL,
		)
	}

	var gotPayload map[string]string
	if err := json.Unmarshal(claimed.Payload, &gotPayload); err != nil {
		t.Fatalf("unmarshal claimed payload: %v", err)
	}
	if gotPayload["message"] != "integration test" {
		t.Errorf(
			"payload message = %q, want %q",
			gotPayload["message"],
			"integration test",
		)
	}

	secondClaim, err := service.ClaimDelivery(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if secondClaim != nil && secondClaim.ID == claimed.ID {
		t.Errorf("claimed the same delivery twice: %s", claimed.ID)
	}
}

func TestFinishAttemptSuccess(t *testing.T) {
	pool, service, deliveryID := setupProcessingDelivery(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status := int32(200)
	now := time.Now()

	err := service.FinishAttempt(ctx, FinishAttemptInput{
		Result: AttemptResult{
			DeliveryID:    deliveryID,
			AttemptNumber: 1,
			StartedAt:     now.Add(-100 * time.Millisecond),
			FinishedAt:    now,
			HTTPStatus:    &status,
			Duration:      100 * time.Millisecond,
		},
		MaxAttempts: 5,
		RetryAt:     now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("FinishAttempt: %v", err)
	}

	assertDeliveryState(t, pool, deliveryID, "succeeded", 0, "")
	assertAttemptExists(t, pool, deliveryID, 1, 200)
}

func TestFinishAttemptRetriesAfterHTTP500(t *testing.T) {
	pool, service, deliveryID := setupProcessingDelivery(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status := int32(500)
	message := "unexpected HTTP status: 500"
	now := time.Now()
	retryAt := now.Add(time.Minute)

	err := service.FinishAttempt(ctx, FinishAttemptInput{
		Result: AttemptResult{
			DeliveryID:    deliveryID,
			AttemptNumber: 1,
			StartedAt:     now.Add(-100 * time.Millisecond),
			FinishedAt:    now,
			HTTPStatus:    &status,
			Error:         &message,
			Duration:      100 * time.Millisecond,
		},
		MaxAttempts: 5,
		RetryAt:     retryAt,
	})
	if err != nil {
		t.Fatalf("FinishAttempt: %v", err)
	}

	assertDeliveryState(t, pool, deliveryID, "pending", 1, message)
	assertAttemptExists(t, pool, deliveryID, 1, 500)
}

func TestFinishAttemptMarksDeliveryDeadAtMaxAttempts(t *testing.T) {
	pool, service, deliveryID := setupProcessingDelivery(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	status := int32(500)
	message := "unexpected HTTP status: 500"
	now := time.Now()

	err := service.FinishAttempt(ctx, FinishAttemptInput{
		Result: AttemptResult{
			DeliveryID:    deliveryID,
			AttemptNumber: 5,
			StartedAt:     now.Add(-100 * time.Millisecond),
			FinishedAt:    now,
			HTTPStatus:    &status,
			Error:         &message,
			Duration:      100 * time.Millisecond,
		},
		MaxAttempts: 5,
		RetryAt:     now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("FinishAttempt: %v", err)
	}

	assertDeliveryState(t, pool, deliveryID, "dead", 5, message)
	assertAttemptExists(t, pool, deliveryID, 5, 500)
}

func setupProcessingDelivery(t *testing.T) (*pgxpool.Pool, *Service, string) {
	t.Helper()

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
	t.Cleanup(pool.Close)

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping database: %v", err)
	}

	queries := db.New(pool)

	payload, err := json.Marshal(map[string]string{
		"message": "FinishAttempt integration test",
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	event, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: "test.finish_attempt",
		Payload:   payload,
	})
	if err != nil {
		t.Fatalf("create event: %v", err)
	}

	created, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        event.ID,
		DestinationUrl: "http://localhost:9999/webhook",
	})
	if err != nil {
		t.Fatalf("create delivery: %v", err)
	}

	tag, err := pool.Exec(ctx, `
        UPDATE deliveries
        SET status = 'processing',
            attempts_count = 0,
            locked_until = now() + interval '1 minute'
        WHERE id = $1
    `, created.ID)
	if err != nil {
		t.Fatalf("mark delivery as processing: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("updated rows = %d, want 1", tag.RowsAffected())
	}

	return pool, NewService(pool, queries), uuidToString(created.ID)
}

func assertDeliveryState(
	t *testing.T,
	pool *pgxpool.Pool,
	deliveryID string,
	wantStatus string,
	wantAttempts int32,
	wantError string,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var status string
	var attempts int32
	var lastError pgtype.Text

	err := pool.QueryRow(ctx, `
        SELECT status, attempts_count, last_error
        FROM deliveries
        WHERE id = $1
    `, deliveryID).Scan(&status, &attempts, &lastError)
	if err != nil {
		t.Fatalf("query delivery state: %v", err)
	}

	if status != wantStatus {
		t.Errorf("status = %q, want %q", status, wantStatus)
	}
	if attempts != wantAttempts {
		t.Errorf("attempts_count = %d, want %d", attempts, wantAttempts)
	}

	gotError := ""
	if lastError.Valid {
		gotError = lastError.String
	}
	if gotError != wantError {
		t.Errorf("last_error = %q, want %q", gotError, wantError)
	}
}

func assertAttemptExists(
	t *testing.T,
	pool *pgxpool.Pool,
	deliveryID string,
	attemptNumber int32,
	wantHTTPStatus int32,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var gotNumber int32
	var gotStatus pgtype.Int4

	err := pool.QueryRow(ctx, `
        SELECT attempt_number, http_status
        FROM delivery_attempts
        WHERE delivery_id = $1 AND attempt_number = $2
    `, deliveryID, attemptNumber).Scan(&gotNumber, &gotStatus)
	if err != nil {
		t.Fatalf("query delivery attempt: %v", err)
	}

	if gotNumber != attemptNumber {
		t.Errorf("attempt_number = %d, want %d", gotNumber, attemptNumber)
	}
	if !gotStatus.Valid || gotStatus.Int32 != wantHTTPStatus {
		t.Errorf("http_status = %+v, want %d", gotStatus, wantHTTPStatus)
	}
}
