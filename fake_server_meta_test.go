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
	quiet, _, _, cas, casSet, mode, _ := parseFakeMetaFlags(fields[3:])
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	existing, ok := f.items[key]
	if casSet && ok && existing.cas != cas {
		f.mu.Unlock()
		_, _ = w.WriteString("EX\r\n")
		_ = w.Flush()
		return true
	}
	switch mode {
	case 'E':
		if ok {
			f.mu.Unlock()
			if !quiet {
				_, _ = w.WriteString("NS\r\n")
				_ = w.Flush()
			}
			return true
		}
	case 'R':
		if !ok {
			f.mu.Unlock()
			if !quiet {
				_, _ = w.WriteString("NS\r\n")
				_ = w.Flush()
			}
			return true
		}
	}
	f.cas++
	var flags uint32
	if ok {
		flags = existing.flags
	}
	f.items[key] = &fakeItem{value: value, flags: flags, cas: f.cas}
	f.mu.Unlock()
	if !quiet {
		_, _ = w.WriteString("HD\r\n")
		_ = w.Flush()
	}
	return true
}

func (f *fakeMemcached) metaGet(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	quiet, wantValue, returnCAS, cas, casSet, _, opaque := parseFakeMetaFlags(fields[2:])
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	f.mu.Unlock()
	suffix := ""
	if opaque != "" {
		suffix = " O" + opaque
	}
	if ok && returnCAS {
		suffix += " c" + strconv.FormatUint(item.cas, 10)
	}
	if !ok {
		if !quiet {
			_, _ = w.WriteString("EN" + suffix + "\r\n")
			_ = w.Flush()
		}
		return
	}
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
