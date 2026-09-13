package memcached

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// wireKind selects which token families mapWireLine applies after the global
// error strings.
type wireKind uint8

const (
	wireApp wireKind = iota
	wireMeta
	wireAdmin
	wireStats
	wireDump
)

// wireClass is the coarse classification of one status line.
type wireClass uint8

const (
	wireNominal wireClass = iota
	wireFault
	wireStreamEnd
)

// wireHit is the result of mapping one protocol line.
type wireHit struct {
	Class   wireClass
	Err     error
	Status  MetaStatus
	Discard bool
}

// mapWireLine classifies one ASCII/meta/admin/stats/dump status line.
func mapWireLine(line []byte, kind wireKind) wireHit {
	tok := string(trimCRLF(line))
	if kind == wireDump && isDumpEnd(tok) {
		return wireHit{Class: wireStreamEnd}
	}
	if hit, ok := mapGlobalErrorLine(line, tok); ok {
		return hit
	}
	switch kind {
	case wireDump:
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return wireHit{Class: wireNominal}
	case wireAdmin:
		if tok == "OK" {
			return wireHit{Class: wireNominal}
		}
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return malformedHit(tok)
	case wireStats:
		if tok == "END" || tok == "RESET" || tok == "OK" ||
			strings.HasPrefix(tok, "STAT") ||
			tok == "PREFIX" || strings.HasPrefix(tok, "PREFIX ") {
			return wireHit{Class: wireNominal}
		}
		return malformedHit(tok)
	case wireMeta:
		return mapMetaCDLine(tok)
	default:
		return mapAppLine(tok)
	}
}

func isDumpEnd(s string) bool {
	return s == "END" || s == "EN"
}

func malformedHit(tok string) wireHit {
	return wireHit{
		Class:   wireFault,
		Err:     fmt.Errorf("%s: %w", tok, ErrMalformedResponse),
		Discard: true,
	}
}

func mapGlobalErrorLine(line []byte, tok string) (wireHit, bool) {
	if err := prefixedProtocolError(line, "CLIENT_ERROR", ErrClientError); err != nil {
		return wireHit{Class: wireFault, Err: err}, true
	}
	if err := prefixedProtocolError(line, "SERVER_ERROR", ErrServerError); err != nil {
		return wireHit{Class: wireFault, Err: err, Discard: true}, true
	}
	if tok == "ERROR" {
		return wireHit{Class: wireFault, Err: ErrNonexistentCommand}, true
	}
	if err := prefixedProtocolError(line, "ERROR", ErrCommandError); err != nil {
		return wireHit{Class: wireFault, Err: err}, true
	}
	return wireHit{}, false
}

func mapAppLine(tok string) wireHit {
	switch tok {
	case "NOT_FOUND":
		return wireHit{Class: wireFault, Err: ErrNotFound}
	case "EXISTS":
		return wireHit{Class: wireFault, Err: ErrExists}
	case "NOT_STORED":
		return wireHit{Class: wireFault, Err: ErrNotStored}
	case "STORED", "DELETED", "TOUCHED", "OK", "END":
		return wireHit{Class: wireNominal}
	}
	if strings.HasPrefix(tok, "VALUE") || strings.HasPrefix(tok, "VERSION") || strings.HasPrefix(tok, "STAT") {
		return wireHit{Class: wireNominal}
	}
	if isArithmeticValue(tok) {
		return wireHit{Class: wireNominal}
	}
	return malformedHit(tok)
}

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

func mapMetaCDLine(tok string) wireHit {
	cd, _, _ := strings.Cut(tok, " ")
	switch cd {
	case "HD":
		return wireHit{Class: wireNominal, Status: MetaStatusHD}
	case "VA":
		return wireHit{Class: wireNominal, Status: MetaStatusVA}
	case "EN":
		return wireHit{Class: wireFault, Err: ErrNotFound, Status: MetaStatusEN}
	case "NF":
		return wireHit{Class: wireFault, Err: ErrNotFound, Status: MetaStatusNF}
	case "NS":
		return wireHit{Class: wireFault, Err: ErrNotStored, Status: MetaStatusNS}
	case "EX":
		return wireHit{Class: wireFault, Err: ErrExists, Status: MetaStatusEX}
	case "MN":
		return wireHit{Class: wireNominal, Status: MetaStatusMN}
	case "ME":
		return wireHit{Class: wireNominal, Status: MetaStatusME}
	default:
		return wireHit{
			Class:   wireFault,
			Err:     fmt.Errorf("unexpected meta cd %q: %w", cd, ErrMalformedResponse),
			Discard: true,
		}
	}
}

func mapAdminKeywordHit(tok string) (wireHit, bool) {
	type kw struct {
		prefix string
		err    error
	}
	for _, k := range []kw{
		{"BUSY", ErrBusy},
		{"BADCLASS", ErrBadClass},
		{"NOSPARE", ErrNoSpare},
		{"NOTFULL", ErrNotFull},
		{"UNSAFE", ErrUnsafe},
		{"SAME", ErrSame},
		{"WATCHER_TOO_MANY", ErrWatcherTooMany},
		{"WATCHER_FAILED", ErrWatcherFailed},
		{"MEMLIMIT_TOO_SMALL", ErrMemlimitTooSmall},
		{"MEMLIMIT_ADJUST_FAILED", ErrMemlimitAdjustFailed},
		{"NOTSTARTED", ErrNotStarted},
	} {
		if tok == k.prefix || strings.HasPrefix(tok, k.prefix+" ") {
			return wireHit{Class: wireFault, Err: fmt.Errorf("%s: %w", tok, k.err)}, true
		}
	}
	return wireHit{}, false
}

func isChargeMetaFault(err error) bool {
	return errors.Is(err, ErrClientError) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrNonexistentCommand) ||
		errors.Is(err, ErrCommandError) ||
		errors.Is(err, ErrMalformedResponse)
}
