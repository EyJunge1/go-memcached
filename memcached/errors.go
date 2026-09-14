package memcached

import (
	"errors"
)

var (
	// ErrNonexistentCommand matches a bare ERROR line (unknown command).
	ErrNonexistentCommand = errors.New("nonexistent command")
	// ErrClientError matches CLIENT_ERROR followed by a message.
	ErrClientError = errors.New("client error")
	// ErrServerError matches SERVER_ERROR followed by a message.
	ErrServerError = errors.New("server error")
	// ErrNotFound matches NOT_FOUND and meta EN/NF.
	ErrNotFound = errors.New("not found")
	// ErrExists matches EXISTS (CAS did not match on store).
	ErrExists = errors.New("exists")
	// ErrNotStored matches NOT_STORED when add/replace/append/prepend is refused.
	ErrNotStored = errors.New("not stored")
	// ErrInvalidArgument reports a bad caller argument or Config value.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrNotSupported reports a command or transport this client will not send.
	ErrNotSupported = errors.New("not supported")

	// ErrMalformedResponse reports a reply line or frame the parser cannot classify.
	ErrMalformedResponse = errors.New("malformed response")
	// ErrInvalidAddress reports an unusable Server or address spec.
	ErrInvalidAddress = errors.New("invalid address")
	// ErrInvalidKey reports an empty key, a key over 250 bytes, or one with whitespace or controls.
	ErrInvalidKey = errors.New("invalid key: empty, longer than 250 bytes, or contains whitespace/control")
	// ErrInvalidValue reports a value larger than Config.MaxValueBytes.
	ErrInvalidValue = errors.New("invalid value: longer than Config.MaxValueBytes")
	// ErrInvalidNetworkProtocol reports a network this client will not dial.
	ErrInvalidNetworkProtocol = errors.New("invalid network protocol")
	// ErrFlushAllNotAllowed is returned when Flush runs without Config.AllowFlushAll.
	ErrFlushAllNotAllowed = errors.New("flush_all not allowed: set Config.AllowFlushAll")
	// ErrAdminNotEnabled is returned when Admin is used without Config.AllowAdmin.
	ErrAdminNotEnabled = errors.New("admin commands not enabled: set Config.AllowAdmin")
	// ErrSetServersNotAllowed is returned when SetServers runs without Config.AllowSetServers.
	ErrSetServersNotAllowed = errors.New("SetServers not allowed: set Config.AllowSetServers")
	// ErrBusy matches BUSY from slabs reassign or the crawler.
	ErrBusy = errors.New("busy")
	// ErrBadClass matches BADCLASS.
	ErrBadClass = errors.New("bad class")
	// ErrNoSpare matches NOSPARE.
	ErrNoSpare = errors.New("no spare")
	// ErrNotFull matches NOTFULL.
	ErrNotFull = errors.New("not full")
	// ErrUnsafe matches UNSAFE.
	ErrUnsafe = errors.New("unsafe")
	// ErrSame matches SAME (slabs reassign with identical source and dest).
	ErrSame = errors.New("same")
	// ErrCommandError matches ERROR with a trailing message or ERROR:….
	ErrCommandError = errors.New("command error")
	// ErrWatcherTooMany matches WATCHER_TOO_MANY.
	ErrWatcherTooMany = errors.New("watcher too many")
	// ErrWatcherFailed matches WATCHER_FAILED.
	ErrWatcherFailed = errors.New("watcher failed")
	// ErrMemlimitTooSmall matches MEMLIMIT_TOO_SMALL.
	ErrMemlimitTooSmall = errors.New("memlimit too small")
	// ErrMemlimitAdjustFailed matches MEMLIMIT_ADJUST_FAILED.
	ErrMemlimitAdjustFailed = errors.New("memlimit adjust failed")
	// ErrNotStarted matches NOTSTARTED from the crawler.
	ErrNotStarted = errors.New("not started")
	// ErrMixedTransport is returned when one Client mixes UDP with TCP or Unix.
	ErrMixedTransport = errors.New("mixed UDP and stream transports")
	// ErrInsecureTLS is returned when InsecureSkipVerify is set without AllowInsecureTLS.
	ErrInsecureTLS = errors.New("tls: InsecureSkipVerify requires Transport.AllowInsecureTLS")
	// ErrDeadlineRequired is returned when a streaming call has no context deadline.
	ErrDeadlineRequired = errors.New("context deadline required")
	// ErrTransport wraps dial, send, and receive failures.
	ErrTransport = errors.New("transport")
	// ErrAdminTargetRequired is returned when a node-local admin call omits On(server).
	ErrAdminTargetRequired = errors.New("admin command requires On(server)")
	// ErrClosed is returned by methods after Client.Close.
	ErrClosed = errors.New("client is closed")
)
