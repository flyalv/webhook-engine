-- name: CreateDelivery :one
INSERT INTO deliveries (event_id, destination_url)
VALUES ($1, $2)
RETURNING
    id,
    event_id,
    destination_url,
    status,
    attempts_count,
    next_attempt_at,
    created_at;


-- name: ClaimDelivery :one
WITH candidate AS (
    SELECT id
    FROM deliveries
    WHERE (
        status = 'pending'
        AND next_attempt_at <= now()
    )
    OR (
        status = 'processing'
        AND locked_until <= now()
    )
    ORDER BY next_attempt_at, created_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
),
claimed AS (
    UPDATE deliveries AS d
    SET
        status = 'processing',
        locked_until = now() + (sqlc.arg(lease_seconds)::int * interval '1 second'),
        updated_at = now()
    FROM candidate
    WHERE d.id = candidate.id
    RETURNING
        d.id,
        d.event_id,
        d.destination_url,
        d.attempts_count,
        d.locked_until
)
SELECT
    c.id,
    c.event_id,
    c.destination_url,
    c.attempts_count,
    c.locked_until,
    e.payload
FROM claimed AS c
JOIN events AS e ON e.id = c.event_id;


-- name: CompleteDelivery :execrows
UPDATE deliveries
SET
    status = 'succeeded',
    locked_until = NULL,
    last_error = NULL,
    updated_at = now()
WHERE id = $1
    AND status = 'processing';


-- name: FailDelivery :execrows
UPDATE deliveries
SET
    status = $2,
    attempts_count = $3,
    next_attempt_at = $4,
    locked_until = NULL,
    last_error = $5,
    updated_at = now()
WHERE id = $1
    AND status = 'processing';

-- name: CreateDeliveryAttempt :exec
INSERT INTO delivery_attempts (
    delivery_id,
    attempt_number,
    started_at,
    finished_at,
    http_status,
    error,
    duration_ms
)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- name: StartDeliveryAttempt :one
UPDATE deliveries
SET
    attempts_count = attempts_count + 1,
    updated_at = now()
WHERE id = $1
  AND status = 'processing'
RETURNING attempts_count;
