package memcached

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// wireKind chooses which token set mapWireLine applies after the global errors.
type wireKind uint8

const (
	_wireApp wireKind = iota + 1
	_wireMeta
	_wireAdmin
	_wireStats
	_wireDump
)

// wireClass is the coarse class of one status line.
type wireClass uint8

const (
	_wireNominal wireClass = iota + 1
	_wireFault
	_wireStreamEnd
)

// wireHit is the mapping of one protocol line.
type wireHit struct {
	Class   wireClass
	Err     error
	Status  MetaStatus
	Discard bool
}

// mapWireLine classifies one text, meta, admin, stats, or dump status line.
func mapWireLine(line []byte, kind wireKind) wireHit {
	tok := string(trimCRLF(line))
	if kind == _wireDump && isDumpEnd(tok) {
		return wireHit{Class: _wireStreamEnd}
	}
	if hit, ok := mapGlobalErrorLine(line, tok); ok {
		return hit
	}
	switch kind {
	case _wireDump:
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return wireHit{Class: _wireNominal}
	case _wireAdmin:
		if tok == "OK" {
			return wireHit{Class: _wireNominal}
		}
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return malformedHit(tok)
	case _wireStats:
		if tok == "END" || tok == "RESET" || tok == "OK" ||
			strings.HasPrefix(tok, "STAT") ||
			tok == "PREFIX" || strings.HasPrefix(tok, "PREFIX ") {
			return wireHit{Class: _wireNominal}
		}
		return malformedHit(tok)
	case _wireMeta:
		return mapMetaCDLine(tok)
	default:
		return mapAppLine(tok)
	}
}

// isDumpEnd reports whether s is END or EN.
func isDumpEnd(s string) bool {
	return s == "END" || s == "EN"
}

// malformedHit returns a discardable ErrMalformedResponse hit.
func malformedHit(tok string) wireHit {
	return wireHit{
		Class:   _wireFault,
		Err:     fmt.Errorf("%s: %w", tok, ErrMalformedResponse),
		Discard: true,
	}
}

// mapGlobalErrorLine maps CLIENT_ERROR, SERVER_ERROR, and ERROR lines.
func mapGlobalErrorLine(line []byte, tok string) (wireHit, bool) {
	if err := prefixedProtocolError(line, _ClientErrorBytes, ErrClientError); err != nil {
		return wireHit{Class: _wireFault, Err: err}, true
	}
	if err := prefixedProtocolError(line, _ServerErrorBytes, ErrServerError); err != nil {
		return wireHit{Class: _wireFault, Err: err, Discard: true}, true
	}
	if tok == "ERROR" {
		return wireHit{Class: _wireFault, Err: ErrNonexistentCommand}, true
	}
	if err := prefixedProtocolError(line, _ErrorBytes, ErrCommandError); err != nil {
		return wireHit{Class: _wireFault, Err: err}, true
	}
	return wireHit{}, false
}

// mapAppLine maps a classic text-protocol status line.
func mapAppLine(tok string) wireHit {
	switch tok {
	case "NOT_FOUND":
		return wireHit{Class: _wireFault, Err: ErrNotFound}
	case "EXISTS":
		return wireHit{Class: _wireFault, Err: ErrExists}
	case "NOT_STORED":
		return wireHit{Class: _wireFault, Err: ErrNotStored}
	case "STORED", "DELETED", "TOUCHED", "OK", "END":
		return wireHit{Class: _wireNominal}
	}
	if strings.HasPrefix(tok, "VALUE") || strings.HasPrefix(tok, "VERSION") || strings.HasPrefix(tok, "STAT") {
		return wireHit{Class: _wireNominal}
	}
	if isArithmeticValue(tok) {
		return wireHit{Class: _wireNominal}
	}
	return malformedHit(tok)
}

// isArithmeticValue reports whether tok is an incr/decr decimal reply.
func isArithmeticValue(tok string) bool {
	if tok == "" {
		return false
	}
	for _, r := range tok {
		if r == ' ' {
			continue
		}
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

// mapMetaCDLine maps a two-letter meta status and optional flags.
func mapMetaCDLine(tok string) wireHit {
	cd, _, _ := strings.Cut(tok, " ")
	switch cd {
	case "HD":
		return wireHit{Class: _wireNominal, Status: MetaStatusHD}
	case "DE":
		// Some quiet md replies use DE (deleted) instead of HD; treat as success.
		return wireHit{Class: _wireNominal, Status: MetaStatusHD}
	case "VA":
		return wireHit{Class: _wireNominal, Status: MetaStatusVA}
	case "EN":
		return wireHit{Class: _wireFault, Err: ErrNotFound, Status: MetaStatusEN}
	case "NF":
		return wireHit{Class: _wireFault, Err: ErrNotFound, Status: MetaStatusNF}
	case "NS":
		return wireHit{Class: _wireFault, Err: ErrNotStored, Status: MetaStatusNS}
	case "EX":
		return wireHit{Class: _wireFault, Err: ErrExists, Status: MetaStatusEX}
	case "MN":
		return wireHit{Class: _wireNominal, Status: MetaStatusMN}
	case "ME":
		return wireHit{Class: _wireNominal, Status: MetaStatusME}
	default:
		return wireHit{
			Class:   _wireFault,
			Err:     fmt.Errorf("unexpected meta cd %q: %w", cd, ErrMalformedResponse),
			Discard: true,
		}
	}
}

// mapAdminKeywordHit maps a known admin fault keyword.
func mapAdminKeywordHit(tok string) (wireHit, bool) {
	type kw struct {
		prefix string
		err    error
	}
	for _, k := range []kw{
		{prefix: "BUSY", err: ErrBusy},
		{prefix: "BADCLASS", err: ErrBadClass},
		{prefix: "NOSPARE", err: ErrNoSpare},
		{prefix: "NOTFULL", err: ErrNotFull},
		{prefix: "UNSAFE", err: ErrUnsafe},
		{prefix: "SAME", err: ErrSame},
		{prefix: "WATCHER_TOO_MANY", err: ErrWatcherTooMany},
		{prefix: "WATCHER_FAILED", err: ErrWatcherFailed},
		{prefix: "MEMLIMIT_TOO_SMALL", err: ErrMemlimitTooSmall},
		{prefix: "MEMLIMIT_ADJUST_FAILED", err: ErrMemlimitAdjustFailed},
		{prefix: "NOTSTARTED", err: ErrNotStarted},
	} {
		if tok == k.prefix || strings.HasPrefix(tok, k.prefix+" ") {
			return wireHit{Class: _wireFault, Err: fmt.Errorf("%s: %w", tok, k.err)}, true
		}
	}
	return wireHit{}, false
}

// isChargeMetaFault reports whether err must abort a meta parse.
func isChargeMetaFault(err error) bool {
	return errors.Is(err, ErrClientError) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrNonexistentCommand) ||
		errors.Is(err, ErrCommandError) ||
		errors.Is(err, ErrMalformedResponse)
}
