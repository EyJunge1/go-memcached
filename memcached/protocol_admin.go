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

var _crawlerClassSelector = regexp.MustCompile(`^\d+(,\d+)*$`)

// validAdminToken rejects empty tokens and tokens with whitespace or controls.
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

// validCrawlerSelector accepts all, hash, or a comma-separated slab class list.
func validCrawlerSelector(s string) error {
	if err := validAdminToken(s); err != nil {
		return err
	}
	if s == "all" || s == "hash" {
		return nil
	}
	if _crawlerClassSelector.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid crawler selector %q: %w", s, ErrInvalidArgument)
}

// validCrawlClasses accepts all or a comma-separated slab class list. hash is dump-only.
func validCrawlClasses(s string) error {
	if err := validAdminToken(s); err != nil {
		return err
	}
	if s == "all" {
		return nil
	}
	if _crawlerClassSelector.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid crawler selector %q: %w", s, ErrInvalidArgument)
}

// buildAdminLine encodes a one-line admin command, optional Extra tokens, and optional noreply.
func buildAdminLine(cmd string, extra []string, noReply bool, args ...string) (*request, *response, error) {
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	if err := b.appendTextTrail(extra, noReply); err != nil {
		return nil, nil, err
	}
	req := b.AddCRLF().request(verbBytes(cmd), nil)
	if noReply {
		return req, buildNoReplyResponse(), nil
	}
	return req, buildLimitedLineResponse(1), nil
}

// buildVerbosityCommand encodes verbosity <level> [extra…] [noreply].
func buildVerbosityCommand(level int, extra []string, noReply bool) (*request, *response, error) {
	return buildAdminLine("verbosity", extra, noReply, strconv.Itoa(level))
}

// buildShutdownCommand encodes shutdown [graceful].
func buildShutdownCommand(graceful bool) (*request, *response, error) {
	if graceful {
		return buildAdminLine("shutdown", nil, false /* noReply */, "graceful")
	}
	return buildAdminLine("shutdown", nil, false /* noReply */)
}

// buildCacheMemLimitCommand encodes cache_memlimit <megabytes> [extra…] [noreply].
func buildCacheMemLimitCommand(megabytes uint32, extra []string, noReply bool) (*request, *response, error) {
	return buildAdminLine("cache_memlimit", extra, noReply, strconv.FormatUint(uint64(megabytes), 10))
}

// buildRefreshCertsCommand encodes refresh_certs (TLS builds; see docs/tls).
func buildRefreshCertsCommand() (*request, *response, error) {
	return buildAdminLine("refresh_certs", nil, false /* noReply */)
}

// buildSlabsReassignCommand encodes slabs reassign <source> <dest>.
func buildSlabsReassignCommand(source, dest int) (*request, *response, error) {
	return buildAdminLine("slabs", nil, false /* noReply */, "reassign", strconv.Itoa(source), strconv.Itoa(dest))
}

// buildSlabsAutomoveCommand encodes slabs automove <mode>.
func buildSlabsAutomoveCommand(mode int) (*request, *response, error) {
	return buildAdminLine("slabs", nil, false /* noReply */, "automove", strconv.Itoa(mode))
}

// buildLRUTuneCommand encodes lru tune <hotPct> <warmPct> <hotFactor> <warmFactor>.
func buildLRUTuneCommand(hotPct, warmPct int, hotFactor, warmFactor float64) (*request, *response, error) {
	return buildAdminLine("lru", nil, false /* noReply */, "tune",
		strconv.Itoa(hotPct), strconv.Itoa(warmPct),
		strconv.FormatFloat(hotFactor, 'f', -1, 64),
		strconv.FormatFloat(warmFactor, 'f', -1, 64),
	)
}

// buildLRUModeCommand encodes lru mode <mode>.
func buildLRUModeCommand(mode string) (*request, *response, error) {
	return buildAdminLine("lru", nil, false /* noReply */, "mode", mode)
}

// buildLRUTempTTLCommand encodes lru temp_ttl <ttl>.
func buildLRUTempTTLCommand(ttl int) (*request, *response, error) {
	return buildAdminLine("lru", nil, false /* noReply */, "temp_ttl", strconv.Itoa(ttl))
}

// allowedWatchTopic reports whether t is a known watch topic.
func allowedWatchTopic(t WatchTopic) bool {
	switch t {
	case WatchFetchers, WatchMutations, WatchEvictions, WatchConnEvents,
		WatchProxyReqs, WatchProxyEvents, WatchProxyUser, WatchDeletions:
		return true
	default:
		return false
	}
}

// buildWatchCommand encodes watch <topic>*.
func buildWatchCommand(topics []WatchTopic) (*request, *response) {
	args := make([]string, 0, len(topics))
	for _, t := range topics {
		args = append(args, string(t))
	}
	b := newProtocolBuilder().AddBytes(_WatchBytes)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	req := b.AddCRLF().request(_WatchBytes, nil)
	return req, buildLimitedLineResponse(1)
}

// buildLRUCrawlerCommand encodes lru_crawler <args>*.
func buildLRUCrawlerCommand(args ...string) (*request, *response) {
	b := newProtocolBuilder().AddBytes(_LRUCrawlerBytes)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	req := b.AddCRLF().request(_LRUCrawlerBytes, nil)
	return req, buildLimitedLineResponse(1)
}

// adminStreamLineError maps a dump-stream fault line, or returns nil.
func adminStreamLineError(line []byte) error {
	if len(line) == 0 {
		return nil
	}
	hit := mapWireLine(line, _wireDump)
	if hit.Class == _wireFault {
		return hit.Err
	}
	return nil
}

func parseShutdownReply(resp *response) error {
	if resp == nil || len(resp.rawLines) == 0 {
		return io.EOF
	}
	return parseAdminReply(resp)
}

// parseAdminReply maps the first admin status line.
func parseAdminReply(resp *response) error {
	if resp == nil || len(resp.rawLines) == 0 {
		return fmt.Errorf("empty admin reply: %w", ErrMalformedResponse)
	}
	return mapAdminStatus(resp.rawLines[0])
}

// mapAdminStatus maps an admin OK or fault line, or ErrMalformedResponse.
func mapAdminStatus(line []byte) error {
	if len(line) == 0 {
		return fmt.Errorf("empty admin status: %w", ErrMalformedResponse)
	}
	hit := mapWireLine(line, _wireAdmin)
	if hit.Class == _wireFault {
		return hit.Err
	}
	if hit.Class == _wireNominal {
		return nil
	}
	return fmt.Errorf("%s: %w", trimCRLF(line), ErrMalformedResponse)
}

// isShutdownClosed reports whether err is a connection close after shutdown.
func isShutdownClosed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	if op, ok := errors.AsType[*net.OpError](err); ok {
		return errors.Is(op.Err, syscall.ECONNRESET) || errors.Is(op.Err, syscall.EPIPE)
	}
	return false
}
