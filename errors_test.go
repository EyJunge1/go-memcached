package memcached

import (
	"errors"
	"fmt"
	"testing"
)

func TestWrappedErrorChainPreservesSentinel(t *testing.T) {
	err := fmt.Errorf("request failed: %w", fmt.Errorf("send failed: %w", ErrNotFound))
	isTrue(t, errors.Is(err, ErrNotFound))
	contains(t, err.Error(), "request failed")
	contains(t, err.Error(), "send failed")
}

func TestErrorsJoinPreservesJoinedSentinels(t *testing.T) {
	joined := errors.Join(
		fmt.Errorf("node-a: %w", ErrNotFound),
		fmt.Errorf("node-b: %w", ErrServerError),
	)
	isError(t, joined)
	isTrue(t, errors.Is(joined, ErrNotFound))
	isTrue(t, errors.Is(joined, ErrServerError))
}
