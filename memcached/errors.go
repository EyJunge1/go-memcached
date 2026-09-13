package memcached

import (
	"errors"
)

var (
	// ErrNonexistentCommand is the bare "ERROR" line (unknown command name).
	ErrNonexistentCommand = errors.New("nonexistent command")
	// ErrClientError is CLIENT_ERROR <message>.
	ErrClientError = errors.New("client error")
	// ErrServerError is SERVER_ERROR <message>.
	ErrServerError = errors.New("server error")
	// ErrNotFound is NOT_FOUND.
	ErrNotFound = errors.New("not found")
	// ErrExists is EXISTS (CAS mismatch on storage).
	ErrExists = errors.New("exists")
	// ErrNotStored is NOT_STORED (add/replace/append/prepend condition failed).
	ErrNotStored = errors.New("not stored")
	// ErrInvalidArgument is a client-side argument or Config fault.
	ErrInvalidArgument = errors.New("invalid arguments")
	// ErrNotSupported is a command or transport the client refuses (e.g. Stats on UDP).
	ErrNotSupported = errors.New("not supported")

	// ErrMalformedResponse is a line or frame that is not a known protocol reply.
	ErrMalformedResponse = errors.New("malformed response")
	// ErrInvalidAddress is a bad Server / address spec.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrInvalidKey is an empty key, a key longer than 250 bytes, or one with whitespace/control.
	ErrInvalidKey = errors.New("invalid key: empty, longer than 250 bytes, or contains whitespace/control")
	// ErrInvalidValue is a value longer than Config.MaxValueBytes.
	ErrInvalidValue = errors.New("invalid value: longer than Config.MaxValueBytes")
	// ErrInvalidNetworkProtocol is a network the client does not dial.
	ErrInvalidNetworkProtocol = errors.New("invalid network protocol")
	// ErrFlushAllNotAllowed is Flush without Config.AllowFlushAll.
	ErrFlushAllNotAllowed = errors.New("flush_all not allowed: set Config.AllowFlushAll")
	// ErrAdminNotEnabled is Admin() without Config.AllowAdmin.
	ErrAdminNotEnabled = errors.New("admin commands not enabled: set Config.AllowAdmin")
	// ErrSetServersNotAllowed is SetServers without Config.AllowSetServers.
	ErrSetServersNotAllowed = errors.New("SetServers not allowed: set Config.AllowSetServers")
	// ErrBusy is BUSY (slabs reassign / crawler).
	ErrBusy = errors.New("busy")
	// ErrBadClass is BADCLASS.
	ErrBadClass = errors.New("bad class")
	// ErrNoSpare is NOSPARE.
	ErrNoSpare = errors.New("no spare")
	// ErrNotFull is NOTFULL.
	ErrNotFull = errors.New("not full")
	// ErrUnsafe is UNSAFE.
	ErrUnsafe = errors.New("unsafe")
	// ErrSame is SAME (slabs reassign src == dest).
	ErrSame = errors.New("same")
	// ErrCommandError is "ERROR <message>" / "ERROR:…".
	ErrCommandError = errors.New("command error")
	// ErrWatcherTooMany is WATCHER_TOO_MANY.
	ErrWatcherTooMany = errors.New("watcher too many")
	// ErrWatcherFailed is WATCHER_FAILED.
	ErrWatcherFailed = errors.New("watcher failed")
	// ErrMemlimitTooSmall is MEMLIMIT_TOO_SMALL.
	ErrMemlimitTooSmall = errors.New("memlimit too small")
	// ErrMemlimitAdjustFailed is MEMLIMIT_ADJUST_FAILED.
	ErrMemlimitAdjustFailed = errors.New("memlimit adjust failed")
	// ErrNotStarted is NOTSTARTED (crawler).
	ErrNotStarted = errors.New("not started")
	// ErrMixedTransport is UDP mixed with TCP/Unix on one Client.
	ErrMixedTransport = errors.New("mixed UDP and stream transports")
	// ErrInsecureTLS is InsecureSkipVerify without AllowInsecureTLS.
	ErrInsecureTLS = errors.New("tls: InsecureSkipVerify requires Transport.AllowInsecureTLS")
	// ErrDeadlineRequired is a stream op without a context deadline.
	ErrDeadlineRequired = errors.New("context deadline required")
	// ErrTransport marks dial, send, and recv failures.
	ErrTransport = errors.New("memcached transport")
	// ErrAdminTargetRequired is a node-local admin op without On(server).
	ErrAdminTargetRequired = errors.New("admin command requires On(server)")
	// ErrClosed is returned after Client.Close.
	ErrClosed = errors.New("client is closed")
)
