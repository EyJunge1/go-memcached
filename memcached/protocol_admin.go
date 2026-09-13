package memcached

import (
	"bytes"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

var crawlerClassSelector = regexp.MustCompile(`^\d+(,\d+)*$`)

func validAdminToken(s string) error {
	if s == "" {
		return fmt.Errorf("empty token: %w", ErrInvalidArgument)
	}
	for i := 0; i < len(s); i++ {
		if s[i] <= ' ' || s[i] == 0x7f {
			return fmt.Errorf("invalid token: %w", ErrInvalidArgument)
		}
	}
	return nil
}

func validCrawlerSelector(s string) error {
	if err := validAdminToken(s); err != nil {
		return err
	}
	if s == "all" || s == "hash" {
		return nil
	}
	if crawlerClassSelector.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid crawler selector %q: %w", s, ErrInvalidArgument)
}

func buildAdminLine(cmd string, args ...string) (*request, *response) {
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	req := buildRequest([]byte(cmd), nil, b.AddCRLF().build())
	return req, buildLimitedLineResponse(1)
}

func buildVerbosityCommand(level int) (*request, *response) {
	return buildAdminLine("verbosity", strconv.Itoa(level))
}

func buildShutdownCommand(graceful bool) (*request, *response) {
	if graceful {
		return buildAdminLine("shutdown", "graceful")
	}
	return buildAdminLine("shutdown")
}

func buildCacheMemLimitCommand(megabytes uint32) (*request, *response) {
	return buildAdminLine("cache_memlimit", strconv.FormatUint(uint64(megabytes), 10))
}

func buildSlabsReassignCommand(source, dest int) (*request, *response) {
	return buildAdminLine("slabs", "reassign", strconv.Itoa(source), strconv.Itoa(dest))
}

func buildSlabsAutomoveCommand(mode int) (*request, *response) {
	return buildAdminLine("slabs", "automove", strconv.Itoa(mode))
}

func buildLRUTuneCommand(hotPct, warmPct int, hotFactor, warmFactor float64) (*request, *response) {
	return buildAdminLine("lru", "tune",
		strconv.Itoa(hotPct), strconv.Itoa(warmPct),
		strconv.FormatFloat(hotFactor, 'f', -1, 64),
		strconv.FormatFloat(warmFactor, 'f', -1, 64),
	)
}

func buildLRUModeCommand(mode string) (*request, *response) {
	return buildAdminLine("lru", "mode", mode)
}

func buildLRUTempTTLCommand(ttl int) (*request, *response) {
	return buildAdminLine("lru", "temp_ttl", strconv.Itoa(ttl))
}

func allowedWatchTopic(t WatchTopic) bool {
	switch t {
	case WatchFetchers, WatchMutations, WatchEvictions, WatchConnEvents,
		WatchProxyReqs, WatchProxyEvents, WatchProxyUser, WatchDeletions:
		return true
	default:
		return false
	}
}

func buildWatchCommand(topics []WatchTopic) (*request, *response) {
	args := make([]string, 0, len(topics))
	for _, t := range topics {
		args = append(args, string(t))
	}
	b := newProtocolBuilder().AddString("watch")
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	req := buildRequest([]byte("watch"), nil, b.AddCRLF().build())
	return req, buildLimitedLineResponse(1)
}

func buildLRUCrawlerCommand(args ...string) (*request, *response) {
	all := append([]string{"lru_crawler"}, args...)
	cmd := all[0]
	rest := all[1:]
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range rest {
		b.AddString(a)
	}
	req := buildRequest([]byte("lru_crawler"), nil, b.AddCRLF().build())
	return req, buildLimitedLineResponse(1)
}

func mapAdminKeyword(tok string) error {
	switch {
	case tok == "BUSY" || strings.HasPrefix(tok, "BUSY "):
		return ErrBusy
	case tok == "BADCLASS" || strings.HasPrefix(tok, "BADCLASS "):
		return ErrBadClass
	case tok == "NOSPARE" || strings.HasPrefix(tok, "NOSPARE "):
		return ErrNoSpare
	case tok == "NOTFULL" || strings.HasPrefix(tok, "NOTFULL "):
		return ErrNotFull
	case tok == "UNSAFE" || strings.HasPrefix(tok, "UNSAFE "):
		return ErrUnsafe
	case tok == "SAME" || strings.HasPrefix(tok, "SAME "):
		return ErrSame
	case tok == "ERROR":
		return ErrNonexistentCommand
	case strings.HasPrefix(tok, "ERROR "):
		return fmt.Errorf("%s: %w", tok, ErrInvalidArgument)
	default:
		return nil
	}
}

func adminStreamLineError(s string) error {
	if s == "" {
		return nil
	}
	if err := forecastCommonFaultLine([]byte(s + "\r\n")); err != nil {
		return err
	}
	return mapAdminKeyword(s)
}

func parseAdminReply(resp *response) error {
	if resp == nil || len(resp.rawLines) == 0 {
		return fmt.Errorf("empty admin reply: %w", ErrMalformedResponse)
	}
	return mapAdminStatus(resp.rawLines[0])
}

func mapAdminStatus(line []byte) error {
	if err := forecastCommonFaultLine(line); err != nil {
		return err
	}
	tok := string(trimCRLF(line))
	if tok == "OK" {
		return nil
	}
	if err := mapAdminKeyword(tok); err != nil {
		return err
	}
	return fmt.Errorf("%s: %w", tok, ErrMalformedResponse)
}

func isShutdownClosed(err error) bool {
	if err == nil {
		return false
	}
	if errorsIsEOF(err) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed network connection")
}

func errorsIsEOF(err error) bool {
	return err == io.EOF || bytes.Contains([]byte(err.Error()), []byte("EOF"))
}
