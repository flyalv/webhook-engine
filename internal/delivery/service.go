package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"
	"webhook-engine/internal/postgres/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewService(pool *pgxpool.Pool, queries *db.Queries) *Service {
	return &Service{pool: pool, queries: queries}
}

type AttemptResult struct {
	DeliveryID    string
	AttemptNumber int32
	StartedAt     time.Time
	FinishedAt    time.Time
	HTTPStatus    *int32
	Error         *string
	Duration      time.Duration
}

type FinishAttemptInput struct {
	Result      AttemptResult
	MaxAttempts int32
	RetryAt     time.Time
}

type ClaimedDelivery struct {
	ID             string
	EventID        string
	DestinationURL string
	Payload        []byte
	AttemptsCount  int32
}

func (s *Service) FinishAttempt(
	ctx context.Context,
	input FinishAttemptInput,
) error {
	if input.Result.AttemptNumber <= 0 {
		return fmt.Errorf("attempt number must be positive")
	}

	if input.MaxAttempts <= 0 {
		return fmt.Errorf("max attempts must be positive")
	}

	deliveryID, err := parseDeliveryID(input.Result.DeliveryID)
	if err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transatcion: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	queries := s.queries.WithTx(tx)

	err = queries.CreateDeliveryAttempt(ctx, db.CreateDeliveryAttemptParams{
		DeliveryID:    deliveryID,
		AttemptNumber: input.Result.AttemptNumber,
		StartedAt:     toTimestamptz(input.Result.StartedAt),
		FinishedAt:    toTimestamptz(input.Result.FinishedAt),
		HttpStatus:    toInt4(input.Result.HTTPStatus),
		Error:         toText(input.Result.Error),
		DurationMs:    toInt8(input.Result.Duration.Milliseconds()),
	})
	if err != nil {
		return fmt.Errorf("create delivery attempt: %w", err)
	}

	succeeded := input.Result.Error == nil &&
		input.Result.HTTPStatus != nil &&
		*input.Result.HTTPStatus >= 200 &&
		*input.Result.HTTPStatus < 300

	if succeeded {
		rows, err := queries.CompleteDelivery(ctx, deliveryID)
		if err != nil {
			return fmt.Errorf("complete delivery: %w", err)
		}
		if rows != 1 {
			return fmt.Errorf("delivery is no longer processing")
		}
	} else {
		status := "pending"
		if input.Result.AttemptNumber >= input.MaxAttempts {
			status = "dead"
		}

		lastError := "HTTP request failed"
		if input.Result.Error != nil {
			lastError = *input.Result.Error
		} else if input.Result.HTTPStatus != nil {
			lastError = fmt.Sprintf(
				"unexpected HTTP status: %d",
				*input.Result.HTTPStatus,
			)
		}

		rows, err := queries.FailDelivery(ctx, db.FailDeliveryParams{
			ID:            deliveryID,
			Status:        status,
			AttemptsCount: input.Result.AttemptNumber,
			NextAttemptAt: toTimestamptz(input.RetryAt),
			LastError:     toText(&lastError),
		})
		if err != nil {
			return fmt.Errorf("fail delivery: %w", err)
		}
		if rows != 1 {
			return fmt.Errorf("delivery is no longer proccessing")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (s *Service) ClaimDelivery(ctx context.Context, leaseDuration time.Duration) (*ClaimedDelivery, error) {
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}

	leaseSeconds := int32(leaseDuration / time.Second)
	if leaseSeconds < 1 {
		leaseSeconds = 1
	}

	delivery, err := s.queries.ClaimDelivery(ctx, leaseSeconds)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("claim delivery: %w", err)
	}

	return &ClaimedDelivery{
		ID:             uuidToString(delivery.ID),
		EventID:        uuidToString(delivery.EventID),
		DestinationURL: delivery.DestinationUrl,
		Payload:        delivery.Payload,
		AttemptsCount:  delivery.AttemptsCount,
	}, nil
}

func (s *Service) StartDeliveryAttempt(ctx context.Context, deliveryID string) (int32, error) {
	id, err := parseDeliveryID(deliveryID)
	if err != nil {
		return 0, err
	}

	attemptNumber, err := s.queries.StartDeliveryAttempt(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("start delivery attempt: %w", err)
	}

	return attemptNumber, nil
}

func uuidToString(id pgtype.UUID) string {
	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		id.Bytes[0:4],
		id.Bytes[4:6],
		id.Bytes[6:8],
		id.Bytes[8:10],
		id.Bytes[10:16],
	)
}

func parseDeliveryID(id string) (pgtype.UUID, error) {
	var result pgtype.UUID
	if err := result.Scan(id); err != nil {
		return pgtype.UUID{}, fmt.Errorf("parse delivery ID: %w", err)
	}
	return result, nil
}

func toTimestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{
		Time:  t,
		Valid: true,
	}
}

func toInt4(value *int32) pgtype.Int4 {
	if value == nil {
		return pgtype.Int4{}
	}
	return pgtype.Int4{Int32: *value, Valid: true}
}

func toInt8(value int64) pgtype.Int8 {
	return pgtype.Int8{
		Int64: value,
		Valid: true,
	}
}

func toText(value *string) pgtype.Text {
	if value == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *value, Valid: true}
}
