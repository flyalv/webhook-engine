package event

import (
	"context"
	"fmt"
	"webhook-engine/internal/postgres/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewService(pool *pgxpool.Pool, queries *db.Queries) *Service {
	return &Service{pool: pool, queries: queries}
}

type CreateInput struct {
	EventType      string
	Payload        []byte
	DestinationURL string
}

type CreateResult struct {
	EventID    string
	DeliveryID string
}

func (s *Service) Create(ctx context.Context, input CreateInput) (*CreateResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transacrion: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	queries := s.queries.WithTx(tx)

	createdEvent, err := queries.CreateEvent(ctx, db.CreateEventParams{
		EventType: input.EventType,
		Payload:   input.Payload,
	})
	if err != nil {
		return nil, fmt.Errorf("create event: %w", err)
	}

	createdDeliver, err := queries.CreateDelivery(ctx, db.CreateDeliveryParams{
		EventID:        createdEvent.ID,
		DestinationUrl: input.DestinationURL,
	})

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return &CreateResult{
		EventID:    createdEvent.ID.String(),
		DeliveryID: createdDeliver.ID.String(),
	}, nil
}
