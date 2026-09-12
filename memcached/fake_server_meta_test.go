package memcached

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

func parseFakeMetaFlags(tokens []string) (quiet, value, returnCAS bool, cas uint64, casSet bool, mode byte, opaque string) {
	for _, tok := range tokens {
		switch {
		case tok == "q":
			quiet = true
		case tok == "v":
			value = true
		case tok == "c":
			returnCAS = true
		case len(tok) > 1 && tok[0] == 'C':
			cas, _ = strconv.ParseUint(tok[1:], 10, 64)
			casSet = true
		case len(tok) > 1 && tok[0] == 'O':
			opaque = tok[1:]
		case len(tok) == 2 && tok[0] == 'M':
			mode = tok[1]
		}
	}
	return
}

func (f *fakeMemcached) metaSet(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return true
	}
	key := fields[1]
	nbytes, err := strconv.Atoi(fields[2])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return true
	}
	quiet, _, _, cas, casSet, mode, opaque := parseFakeMetaFlags(fields[3:])
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	defer f.mu.Unlock()
	existing, ok := f.items[key]
	if casSet && ok && existing.cas != cas {
		writeFakeMetaStatus(w, "EX", quiet, opaque)
		return true
	}
	switch mode {
	case 'E':
		if ok {
			writeFakeMetaStatus(w, "NS", quiet, opaque)
			return true
		}
	case 'R':
		if !ok {
			writeFakeMetaStatus(w, "NS", quiet, opaque)
			return true
		}
	}
	f.cas++
	var flags uint32
	if ok {
		flags = existing.flags
	}
	f.items[key] = &fakeItem{value: value, flags: flags, cas: f.cas}
	writeFakeMetaStatus(w, "HD", quiet, opaque)
	return true
}

// writeFakeMetaStatus emits a meta status line. Quiet suppresses success unless
// opaque is set (pipeline matching requires O replies before mn).
func writeFakeMetaStatus(w *bufio.Writer, status string, quiet bool, opaque string) {
	if quiet && opaque == "" && (status == "HD" || status == "VA") {
		return
	}
	line := status + "\r\n"
	if opaque != "" {
		line = status + " O" + opaque + "\r\n"
	}
	_, _ = w.WriteString(line)
	_ = w.Flush()
}

func (f *fakeMemcached) metaGet(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	if fields[1] == "cerr" {
		_, _ = w.WriteString("CLIENT_ERROR bad meta\r\n")
		_ = w.Flush()
		return
	}
	quiet, wantValue, returnCAS, cas, casSet, _, opaque := parseFakeMetaFlags(fields[2:])
	returnKey, binaryKey, vivify, recache := false, false, false, false
	for _, tok := range fields[2:] {
		switch {
		case tok == "k":
			returnKey = true
		case tok == "b":
			binaryKey = true
		case len(tok) > 0 && tok[0] == 'N':
			vivify = true
		case len(tok) > 0 && tok[0] == 'R':
			recache = true
		}
	}
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	if !ok && vivify {
		f.cas++
		item = &fakeItem{cas: f.cas}
		f.items[fields[1]] = item
		item.recacheBusy = true
		f.mu.Unlock()
		suffix := recacheSuffix(opaque, returnKey, binaryKey, fields[1], true, false, false)
		if !quiet || opaque != "" {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	f.mu.Unlock()
	suffix := ""
	if opaque != "" {
		suffix = " O" + opaque
	}
	if returnKey {
		suffix += " k" + fields[1]
	}
	if binaryKey {
		suffix += " b"
	}
	if ok && returnCAS {
		suffix += " c" + strconv.FormatUint(item.cas, 10)
	}
	if !ok {
		if !quiet || opaque != "" {
			_, _ = w.WriteString("EN" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	won, stale, busy := false, item.stale, false
	if recache {
		f.mu.Lock()
		if item.recacheBusy {
			busy = true
		} else {
			won = true
			item.recacheBusy = true
		}
		f.mu.Unlock()
	}
	suffix += recacheExtra(won, stale, busy)
	if casSet && item.cas == cas {
		if !quiet {
			_, _ = w.WriteString("HD" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
	if wantValue {
		_, _ = fmt.Fprintf(w, "VA %d f%d%s\r\n", len(item.value), item.flags, suffix)
		_, _ = w.Write(item.value)
		_, _ = w.WriteString("\r\n")
		_ = w.Flush()
		return
	}
	if !quiet {
		_, _ = w.WriteString("HD" + suffix + "\r\n")
		_ = w.Flush()
	}
}

func recacheSuffix(opaque string, returnKey, binaryKey bool, key string, won, stale, busy bool) string {
	s := ""
	if opaque != "" {
		s = " O" + opaque
	}
	if returnKey {
		s += " k" + key
	}
	if binaryKey {
		s += " b"
	}
	return s + recacheExtra(won, stale, busy)
}

func recacheExtra(won, stale, busy bool) string {
	s := ""
	if won {
		s += " W"
	}
	if stale {
		s += " X"
	}
	if busy {
		s += " Z"
	}
	return s
}
