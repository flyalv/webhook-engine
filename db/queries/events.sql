-- name: CreateEvent :one
INSERT INTO events (
    event_type,
    payload
)
VALUES (
    $1,
    $2
)
RETURNING id, event_type, payload, created_at;

-- name: GetEvent :one
SELECT id, event_type, payload, created_at
FROM events
WHERE id = $1;
