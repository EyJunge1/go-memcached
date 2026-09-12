package memcached

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestWrappedErrorChainPreservesSentinel(t *testing.T) {
	err := fmt.Errorf("request: %w", fmt.Errorf("send: %w", ErrNotFound))
	isTrue(t, errors.Is(err, ErrNotFound))
	contains(t, errText(t, err), "request:")
	contains(t, errText(t, err), "send:")
}

func TestWrapRecvErrorKeepsProtocolSentinels(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		want          error
		transport     bool
		shouldDiscard bool
	}{
		{
			name: "not found",
			err:  ErrNotFound,
			want: ErrNotFound,
		},
		{
			name:          "io timeout",
			err:           ioTimeoutError{},
			want:          ErrTransport,
			transport:     true,
			shouldDiscard: true,
		},
		{
			name:          "malformed",
			err:           ErrMalformedResponse,
			want:          ErrMalformedResponse,
			shouldDiscard: true,
		},
		{
			name:          "server error",
			err:           ErrServerError,
			want:          ErrServerError,
			shouldDiscard: true,
		},
		{
			name: "command error",
			err:  ErrCommandError,
			want: ErrCommandError,
		},
		{
			name: "invalid argument",
			err:  ErrInvalidArgument,
			want: ErrInvalidArgument,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapRecvError(fmt.Errorf("recv: %w", tt.err))
			isErrorIs(t, got, tt.want)
			equal(t, tt.transport, isTransportError(got))
			equal(t, tt.shouldDiscard, shouldDiscardConn(got))
		})
	}
}

type ioTimeoutError struct{}

func (ioTimeoutError) Error() string { return "i/o timeout" }

func TestTransportSentinel(t *testing.T) {
	err := wrapTransport(fmt.Errorf("dial: %w", ioTimeoutError{}))
	isErrorIs(t, err, ErrTransport)
}

func TestWrapTransportSkipsProtocolSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "client error", err: ErrClientError, want: ErrClientError},
		{name: "invalid argument", err: ErrInvalidArgument, want: ErrInvalidArgument},
		{name: "canceled", err: context.Canceled, want: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded, want: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := wrapTransport(fmt.Errorf("alloc connection: %w", tt.err))
			isErrorIs(t, got, tt.want)
			isFalse(t, isTransportError(got))
		})
	}
}

func TestErrorsJoinPreservesJoinedSentinels(t *testing.T) {
	joined := errors.Join(
		fmt.Errorf("node-a: %w", ErrNotFound),
		fmt.Errorf("node-b: %w", ErrServerError),
	)
	isError(t, joined)
	isTrue(t, errors.Is(joined, ErrNotFound))
	isTrue(t, errors.Is(joined, ErrServerError))

	mix := errors.Join(ErrBusy, ErrServerError)
	isTrue(t, errors.Is(mix, ErrBusy))
	isTrue(t, errors.Is(mix, ErrServerError))
}
