package memcached

import (
	"context"
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

func TestWrapRecvErrorKeepsProtocolSentinels(t *testing.T) {
	err := wrapRecvError(fmt.Errorf("recv failed: %w", ErrNotFound))
	isErrorIs(t, err, ErrNotFound)
	isFalse(t, isTransportError(err))
	isFalse(t, shouldDiscardConn(err))

	ioErr := wrapRecvError(fmt.Errorf("recv failed: %w", ioTimeoutError{}))
	isTrue(t, isTransportError(ioErr))
	isTrue(t, shouldDiscardConn(ioErr))

	malformed := wrapRecvError(fmt.Errorf("bad frame: %w", ErrMalformedResponse))
	isTrue(t, shouldDiscardConn(malformed))
	isFalse(t, isTransportError(malformed))
	isErrorIs(t, malformed, ErrMalformedResponse)

	server := wrapRecvError(fmt.Errorf("recv failed: %w", ErrServerError))
	isErrorIs(t, server, ErrServerError)
	isFalse(t, isTransportError(server))
	isTrue(t, shouldDiscardConn(server))

	cmd := wrapRecvError(fmt.Errorf("recv failed: %w", ErrCommandError))
	isErrorIs(t, cmd, ErrCommandError)
	isFalse(t, isTransportError(cmd))
	isFalse(t, shouldDiscardConn(cmd))

	arg := wrapRecvError(fmt.Errorf("recv failed: %w", ErrInvalidArgument))
	isErrorIs(t, arg, ErrInvalidArgument)
	isFalse(t, isTransportError(arg))
}

type ioTimeoutError struct{}

func (ioTimeoutError) Error() string { return "i/o timeout" }

func TestTransportSentinel(t *testing.T) {
	err := wrapTransport(fmt.Errorf("dial failed: %w", ioTimeoutError{}))
	isErrorIs(t, err, ErrTransport)
}

func TestWrapTransportSkipsProtocolSentinels(t *testing.T) {
	err := wrapTransport(fmt.Errorf("alloc connection failed: %w", ErrClientError))
	isErrorIs(t, err, ErrClientError)
	isFalse(t, isTransportError(err))

	arg := wrapTransport(fmt.Errorf("alloc connection failed: %w", ErrInvalidArgument))
	isErrorIs(t, arg, ErrInvalidArgument)
	isFalse(t, isTransportError(arg))

	canceled := wrapTransport(fmt.Errorf("alloc connection failed: %w", context.Canceled))
	isErrorIs(t, canceled, context.Canceled)
	isFalse(t, isTransportError(canceled))

	deadline := wrapTransport(fmt.Errorf("alloc connection failed: %w", context.DeadlineExceeded))
	isErrorIs(t, deadline, context.DeadlineExceeded)
	isFalse(t, isTransportError(deadline))
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
