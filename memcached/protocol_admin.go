package memcached

import (
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"strconv"
	"syscall"
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

func buildAdminLine(cmd string, noReply bool, args ...string) (*request, *response) {
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	req := buildRequest([]byte(cmd), nil, b.AddCRLF().build())
	if noReply {
		return req, buildNoReplyResponse()
	}
	return req, buildLimitedLineResponse(1)
}

func buildVerbosityCommand(level int, noReply bool) (*request, *response) {
	return buildAdminLine("verbosity", noReply, strconv.Itoa(level))
}

func buildShutdownCommand(graceful bool) (*request, *response) {
	if graceful {
		return buildAdminLine("shutdown", false, "graceful")
	}
	return buildAdminLine("shutdown", false)
}

func buildCacheMemLimitCommand(megabytes uint32, noReply bool) (*request, *response) {
	return buildAdminLine("cache_memlimit", noReply, strconv.FormatUint(uint64(megabytes), 10))
}

func buildSlabsReassignCommand(source, dest int) (*request, *response) {
	return buildAdminLine("slabs", false, "reassign", strconv.Itoa(source), strconv.Itoa(dest))
}

func buildSlabsAutomoveCommand(mode int) (*request, *response) {
	return buildAdminLine("slabs", false, "automove", strconv.Itoa(mode))
}

func buildLRUTuneCommand(hotPct, warmPct int, hotFactor, warmFactor float64) (*request, *response) {
	return buildAdminLine("lru", false, "tune",
		strconv.Itoa(hotPct), strconv.Itoa(warmPct),
		strconv.FormatFloat(hotFactor, 'f', -1, 64),
		strconv.FormatFloat(warmFactor, 'f', -1, 64),
	)
}

func buildLRUModeCommand(mode string) (*request, *response) {
	return buildAdminLine("lru", false, "mode", mode)
}

func buildLRUTempTTLCommand(ttl int) (*request, *response) {
	return buildAdminLine("lru", false, "temp_ttl", strconv.Itoa(ttl))
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
	hit, ok := mapAdminKeywordHit(tok)
	if ok {
		return hit.Err
	}
	return nil
}

func adminStreamLineError(s string) error {
	if s == "" {
		return nil
	}
	hit := MapWireLine([]byte(s), WireDump)
	if hit.Class == WireFault {
		return hit.Err
	}
	return nil
}

func parseAdminReply(resp *response) error {
	if resp == nil || len(resp.rawLines) == 0 {
		return fmt.Errorf("empty admin reply: %w", ErrMalformedResponse)
	}
	return mapAdminStatus(resp.rawLines[0])
}

func mapAdminStatus(line []byte) error {
	hit := MapWireLine(line, WireAdmin)
	if hit.Class == WireFault {
		return hit.Err
	}
	if hit.Class == WireNominal {
		return nil
	}
	return fmt.Errorf("%s: %w", trimCRLF(line), ErrMalformedResponse)
}

func isShutdownClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return errors.Is(op.Err, syscall.ECONNRESET) || errors.Is(op.Err, syscall.EPIPE)
	}
	return false
}
