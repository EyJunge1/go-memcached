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
	if crawlerClassSelector.MatchString(s) {
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
	if crawlerClassSelector.MatchString(s) {
		return nil
	}
	return fmt.Errorf("invalid crawler selector %q: %w", s, ErrInvalidArgument)
}

// buildAdminLine encodes a one-line admin command and optional noreply.
func buildAdminLine(cmd string, noReply bool, args ...string) (*request, *response) {
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	req := b.AddCRLF().request([]byte(cmd), nil)
	if noReply {
		return req, buildNoReplyResponse()
	}
	return req, buildLimitedLineResponse(1)
}

// buildVerbosityCommand encodes verbosity <level> [noreply].
func buildVerbosityCommand(level int, noReply bool) (*request, *response) {
	return buildAdminLine("verbosity", noReply, strconv.Itoa(level))
}

// buildShutdownCommand encodes shutdown [graceful].
func buildShutdownCommand(graceful bool) (*request, *response) {
	if graceful {
		return buildAdminLine("shutdown", false, "graceful")
	}
	return buildAdminLine("shutdown", false)
}

// buildCacheMemLimitCommand encodes cache_memlimit <megabytes> [noreply].
func buildCacheMemLimitCommand(megabytes uint32, noReply bool) (*request, *response) {
	return buildAdminLine("cache_memlimit", noReply, strconv.FormatUint(uint64(megabytes), 10))
}

// buildSlabsReassignCommand encodes slabs reassign <source> <dest>.
func buildSlabsReassignCommand(source, dest int) (*request, *response) {
	return buildAdminLine("slabs", false, "reassign", strconv.Itoa(source), strconv.Itoa(dest))
}

// buildSlabsAutomoveCommand encodes slabs automove <mode>.
func buildSlabsAutomoveCommand(mode int) (*request, *response) {
	return buildAdminLine("slabs", false, "automove", strconv.Itoa(mode))
}

// buildLRUTuneCommand encodes lru tune <hotPct> <warmPct> <hotFactor> <warmFactor>.
func buildLRUTuneCommand(hotPct, warmPct int, hotFactor, warmFactor float64) (*request, *response) {
	return buildAdminLine("lru", false, "tune",
		strconv.Itoa(hotPct), strconv.Itoa(warmPct),
		strconv.FormatFloat(hotFactor, 'f', -1, 64),
		strconv.FormatFloat(warmFactor, 'f', -1, 64),
	)
}

// buildLRUModeCommand encodes lru mode <mode>.
func buildLRUModeCommand(mode string) (*request, *response) {
	return buildAdminLine("lru", false, "mode", mode)
}

// buildLRUTempTTLCommand encodes lru temp_ttl <ttl>.
func buildLRUTempTTLCommand(ttl int) (*request, *response) {
	return buildAdminLine("lru", false, "temp_ttl", strconv.Itoa(ttl))
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
	b := newProtocolBuilder().AddString("watch")
	defer b.release()
	for _, a := range args {
		b.AddString(a)
	}
	req := b.AddCRLF().request([]byte("watch"), nil)
	return req, buildLimitedLineResponse(1)
}

// buildLRUCrawlerCommand encodes lru_crawler <args>*.
func buildLRUCrawlerCommand(args ...string) (*request, *response) {
	all := append([]string{"lru_crawler"}, args...)
	cmd := all[0]
	rest := all[1:]
	b := newProtocolBuilder().AddString(cmd)
	defer b.release()
	for _, a := range rest {
		b.AddString(a)
	}
	req := b.AddCRLF().request([]byte("lru_crawler"), nil)
	return req, buildLimitedLineResponse(1)
}

// adminStreamLineError maps a dump-stream fault line, or returns nil.
func adminStreamLineError(line []byte) error {
	if len(line) == 0 {
		return nil
	}
	hit := mapWireLine(line, wireDump)
	if hit.Class == wireFault {
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
	hit := mapWireLine(line, wireAdmin)
	if hit.Class == wireFault {
		return hit.Err
	}
	if hit.Class == wireNominal {
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
	var op *net.OpError
	if errors.As(err, &op) {
		return errors.Is(op.Err, syscall.ECONNRESET) || errors.Is(op.Err, syscall.EPIPE)
	}
	return false
}
