package worker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
	"webhook-engine/internal/delivery"
)

type Worker struct {
	service       *delivery.Service
	httpClient    *http.Client
	logger        *slog.Logger
	pollInterval  time.Duration
	maxAttempts   int32
	leaseDuration time.Duration
}

func New(
	service *delivery.Service,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		service:       service,
		httpClient:    newSafeHTTPClient(),
		logger:        logger,
		pollInterval:  time.Second,
		maxAttempts:   5,
		leaseDuration: 30 * time.Second,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		processed, err := w.processOne(ctx)
		if err != nil {
			w.logger.Error("worker failed to process delivery", "error", err)
		}

		if processed {
			continue
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (w *Worker) processOne(ctx context.Context) (bool, error) {
	task, err := w.service.ClaimDelivery(ctx, w.leaseDuration)
	if err != nil {
		return false, fmt.Errorf("claim delivery: %w", err)
	}
	if task == nil {
		return false, nil
	}

	attemptNumber, err := w.service.StartDeliveryAttempt(ctx, task.ID)
	if err != nil {
		return false, fmt.Errorf("start delivery attempt: %w", err)
	}

	startedAt := time.Now()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		task.DestinationURL,
		bytes.NewReader(task.Payload),
	)
	if err != nil {
		return false, fmt.Errorf("create webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Event-ID", task.EventID)

	resp, err := w.httpClient.Do(req)

	finishedAt := time.Now()

	var httpStatus *int32
	var attemptError *string

	if err != nil {
		message := err.Error()
		attemptError = &message
	} else {
		defer resp.Body.Close()

		status := int32(resp.StatusCode)
		httpStatus = &status

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			message := fmt.Sprintf("unexpected HTTP status: %d", resp.StatusCode)
			attemptError = &message
		}
	}

	result := delivery.AttemptResult{
		DeliveryID:    task.ID,
		AttemptNumber: attemptNumber,
		StartedAt:     startedAt,
		FinishedAt:    finishedAt,
		HTTPStatus:    httpStatus,
		Error:         attemptError,
		Duration:      finishedAt.Sub(startedAt),
	}

	retryAt := time.Now().Add(retryDelay(attemptNumber))

	err = w.service.FinishAttempt(ctx, delivery.FinishAttemptInput{
		Result:      result,
		MaxAttempts: w.maxAttempts,
		RetryAt:     retryAt,
	})
	if err != nil {
		return true, fmt.Errorf("finish delivery attempt: %w", err)
	}

	return true, nil
}
