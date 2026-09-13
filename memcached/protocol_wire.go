package memcached

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// WireKind selects which token families MapWireLine applies after the global
// error strings.
type WireKind uint8

const (
	WireApp WireKind = iota
	WireMeta
	WireAdmin
	WireStats
	WireDump
)

// WireClass is the coarse classification of one status line.
type WireClass uint8

const (
	WireNominal WireClass = iota
	WireFault
	WireStreamEnd
)

// WireHit is the result of mapping one protocol line.
type WireHit struct {
	Class   WireClass
	Err     error
	Status  MetaStatus
	Discard bool
}

// MapWireLine classifies one ASCII/meta/admin/stats/dump status line.
func MapWireLine(line []byte, kind WireKind) WireHit {
	tok := string(trimCRLF(line))
	if kind == WireDump && isDumpEnd(tok) {
		return WireHit{Class: WireStreamEnd}
	}
	if hit, ok := mapGlobalErrorLine(line, tok); ok {
		return hit
	}
	switch kind {
	case WireDump:
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return WireHit{Class: WireNominal}
	case WireAdmin:
		if tok == "OK" {
			return WireHit{Class: WireNominal}
		}
		if hit, ok := mapAdminKeywordHit(tok); ok {
			return hit
		}
		return malformedHit(tok)
	case WireStats:
		if tok == "END" || tok == "RESET" || tok == "OK" ||
			strings.HasPrefix(tok, "STAT") ||
			tok == "PREFIX" || strings.HasPrefix(tok, "PREFIX ") {
			return WireHit{Class: WireNominal}
		}
		return malformedHit(tok)
	case WireMeta:
		return mapMetaCDLine(tok)
	default:
		return mapAppLine(tok)
	}
}

func isDumpEnd(s string) bool {
	return s == "END" || s == "EN"
}

func malformedHit(tok string) WireHit {
	return WireHit{
		Class:   WireFault,
		Err:     fmt.Errorf("%s: %w", tok, ErrMalformedResponse),
		Discard: true,
	}
}

func mapGlobalErrorLine(line []byte, tok string) (WireHit, bool) {
	if err := prefixedProtocolError(line, "CLIENT_ERROR", ErrClientError); err != nil {
		return WireHit{Class: WireFault, Err: err}, true
	}
	if err := prefixedProtocolError(line, "SERVER_ERROR", ErrServerError); err != nil {
		return WireHit{Class: WireFault, Err: err, Discard: true}, true
	}
	if tok == "ERROR" {
		return WireHit{Class: WireFault, Err: ErrNonexistentCommand}, true
	}
	if err := prefixedProtocolError(line, "ERROR", ErrCommandError); err != nil {
		return WireHit{Class: WireFault, Err: err}, true
	}
	return WireHit{}, false
}

func mapAppLine(tok string) WireHit {
	switch tok {
	case "NOT_FOUND":
		return WireHit{Class: WireFault, Err: ErrNotFound}
	case "EXISTS":
		return WireHit{Class: WireFault, Err: ErrExists}
	case "NOT_STORED":
		return WireHit{Class: WireFault, Err: ErrNotStored}
	case "STORED", "DELETED", "TOUCHED", "OK", "END":
		return WireHit{Class: WireNominal}
	}
	if strings.HasPrefix(tok, "VALUE") || strings.HasPrefix(tok, "VERSION") || strings.HasPrefix(tok, "STAT") {
		return WireHit{Class: WireNominal}
	}
	if isArithmeticValue(tok) {
		return WireHit{Class: WireNominal}
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

func mapMetaCDLine(tok string) WireHit {
	cd, _, _ := strings.Cut(tok, " ")
	switch cd {
	case "HD":
		return WireHit{Class: WireNominal, Status: MetaStatusHD}
	case "VA":
		return WireHit{Class: WireNominal, Status: MetaStatusVA}
	case "EN":
		return WireHit{Class: WireFault, Err: ErrNotFound, Status: MetaStatusEN}
	case "NF":
		return WireHit{Class: WireFault, Err: ErrNotFound, Status: MetaStatusNF}
	case "NS":
		return WireHit{Class: WireFault, Err: ErrNotStored, Status: MetaStatusNS}
	case "EX":
		return WireHit{Class: WireFault, Err: ErrExists, Status: MetaStatusEX}
	case "MN":
		return WireHit{Class: WireNominal, Status: MetaStatusMN}
	case "ME":
		return WireHit{Class: WireNominal, Status: MetaStatusME}
	default:
		return WireHit{
			Class:   WireFault,
			Err:     fmt.Errorf("unexpected meta cd %q: %w", cd, ErrMalformedResponse),
			Discard: true,
		}
	}
}

func mapAdminKeywordHit(tok string) (WireHit, bool) {
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
			return WireHit{Class: WireFault, Err: fmt.Errorf("%s: %w", tok, k.err)}, true
		}
	}
	return WireHit{}, false
}

func isChargeMetaFault(err error) bool {
	return errors.Is(err, ErrClientError) ||
		errors.Is(err, ErrServerError) ||
		errors.Is(err, ErrNonexistentCommand) ||
		errors.Is(err, ErrCommandError) ||
		errors.Is(err, ErrMalformedResponse)
}
