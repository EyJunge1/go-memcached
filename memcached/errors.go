package memcached

import (
	"errors"
)

var (
	// ErrNonexistentCommand response by server "ERROR"
	ErrNonexistentCommand = errors.New("nonexistent command")
	// ErrClientError response by server "CLIENT_ERROR <message>"
	ErrClientError = errors.New("client error")
	// ErrServerError response by server "SERVER_ERROR <message>"
	ErrServerError = errors.New("server error")
	// ErrNotFound response by server "NOT_FOUND"
	ErrNotFound = errors.New("not found")
	// ErrExists response by server "EXISTS"
	ErrExists = errors.New("exists")
	// ErrNotStored response by server "NOT_STORED"
	ErrNotStored = errors.New("not stored")
	// ErrInvalidArgument represents an invalid arguments error.
	ErrInvalidArgument = errors.New("invalid arguments")
	// ErrNotSupported represents a not supported error.
	ErrNotSupported = errors.New("not supported")

	// ErrMalformedResponse represents a malformed response error, it could be returned
	// when the response is not expected. Debug the server response to see whether it is
	// correct, if it is correct, please report this issue.
	ErrMalformedResponse = errors.New("malformed response")
	// ErrInvalidAddress represents an invalid address error.
	// It is returned when the given address is invalid.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrInvalidKey represents an invalid key error.
	ErrInvalidKey = errors.New("invalid key: empty, longer than 250 bytes, or contains whitespace/control")
	// ErrInvalidValue represents an invalid value error.
	ErrInvalidValue = errors.New("invalid value: longer than Config.MaxValueBytes")
	// ErrInvalidNetworkProtocol represents an invalid network protocol error.
	ErrInvalidNetworkProtocol = errors.New("invalid network protocol")
	// ErrFlushAllNotAllowed is returned when FlushAll is called without Config.AllowFlushAll.
	ErrFlushAllNotAllowed = errors.New("flush_all not allowed: set Config.AllowFlushAll")
	// ErrAdminNotEnabled is returned when Admin() is called without Config.AllowAdmin.
	ErrAdminNotEnabled = errors.New("admin commands not enabled: set Config.AllowAdmin")
	// ErrSetServersNotAllowed is returned when SetServers is called without Config.AllowSetServers.
	ErrSetServersNotAllowed = errors.New("SetServers not allowed: set Config.AllowSetServers")
	// ErrBusy is the server BUSY reply (slabs reassign / crawler).
	ErrBusy = errors.New("busy")
	// ErrBadClass is the server BADCLASS reply.
	ErrBadClass = errors.New("bad class")
	// ErrNoSpare is the server NOSPARE reply.
	ErrNoSpare = errors.New("no spare")
	// ErrNotFull is the Spec NOTFULL reply.
	ErrNotFull = errors.New("not full")
	// ErrUnsafe is the Spec UNSAFE reply.
	ErrUnsafe = errors.New("unsafe")
	// ErrSame is the server SAME reply (slabs reassign src == dest).
	ErrSame = errors.New("same")
	// ErrCommandError is the server "ERROR <message>" / "ERROR:…" reply.
	ErrCommandError = errors.New("command error")
	// ErrWatcherTooMany is the server WATCHER_TOO_MANY reply.
	ErrWatcherTooMany = errors.New("watcher too many")
	// ErrWatcherFailed is the server WATCHER_FAILED reply.
	ErrWatcherFailed = errors.New("watcher failed")
	// ErrMemlimitTooSmall is the server MEMLIMIT_TOO_SMALL reply.
	ErrMemlimitTooSmall = errors.New("memlimit too small")
	// ErrMemlimitAdjustFailed is the server MEMLIMIT_ADJUST_FAILED reply.
	ErrMemlimitAdjustFailed = errors.New("memlimit adjust failed")
	// ErrNotStarted is the server NOTSTARTED reply (crawler).
	ErrNotStarted = errors.New("not started")
	// ErrMixedTransport is returned when a client mixes UDP with TCP/Unix.
	ErrMixedTransport = errors.New("mixed UDP and stream transports")
	// ErrInsecureTLS is returned when InsecureSkipVerify is set without AllowInsecureTLS.
	ErrInsecureTLS = errors.New("tls: InsecureSkipVerify requires Transport.AllowInsecureTLS")
	// ErrDeadlineRequired is returned when a stream op is called without a deadline.
	ErrDeadlineRequired = errors.New("context deadline required")
	// ErrTransport marks dial, send, and recv failures.
	ErrTransport = errors.New("memcached transport")
	// ErrAdminTargetRequired is returned when a node-local admin op has no On() target.
	ErrAdminTargetRequired = errors.New("admin command requires On(server)")
)
