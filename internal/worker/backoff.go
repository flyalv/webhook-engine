package worker

import (
	"math/rand/v2"
	"time"
)

const (
	baseRetryDelay = time.Second
	maxRetryDelay  = time.Minute
)

func retryDelay(attempt int32) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := baseRetryDelay

	for i := int32(1); i < attempt; i++ {
		if delay >= maxRetryDelay/2 {
			delay = maxRetryDelay
			break
		}

		delay *= 2
	}

	if delay > maxRetryDelay {
		delay = maxRetryDelay
	}

	return time.Duration(rand.Int64N(int64(delay) + 1))
}
