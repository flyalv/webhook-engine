package worker

import (
	"testing"
	"time"
)

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		name    string
		attempt int32
		max     time.Duration
	}{
		{
			name:    "first attempt",
			attempt: 1,
			max:     time.Second,
		},
		{
			name:    "second attempt",
			attempt: 2,
			max:     2 * time.Second,
		},
		{
			name:    "third attempt",
			attempt: 3,
			max:     4 * time.Second,
		},
		{
			name:    "large attempt is capped",
			attempt: 20,
			max:     time.Minute,
		},
		{
			name:    "zero attempt uses first delay",
			attempt: 0,
			max:     time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryDelay(tt.attempt)

			if got < 0 || got > tt.max {
				t.Fatalf(
					"retryDelay(%d) = %v, want value between 0 and %v",
					tt.attempt,
					got,
					tt.max,
				)
			}
		})
	}
}
