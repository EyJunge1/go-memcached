package memcached

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

func (f *fakeMemcached) store(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	if len(fields) < 5 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return true
	}
	cmd := fields[0]
	key := fields[1]
	flags64, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR bad flags\r\n")
		_ = w.Flush()
		return true
	}
	nbytes, err := strconv.Atoi(fields[4])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return true
	}
	var casUnique uint64
	idx := 5
	if cmd == "cas" {
		if len(fields) < 6 {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return true
		}
		casUnique, err = strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return true
		}
		idx = 6
	}
	noReply := len(fields) > idx && fields[idx] == "noreply"
	body := make([]byte, nbytes+2)
	if _, err = io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:nbytes]...)

	f.mu.Lock()
	f.lastSet = strings.Join(fields, " ")
	if cmd == "set" && isAuthProbeKey(key) {
		f.authSets++
	}
	if f.tokenAuth && cmd == "set" && isAuthProbeKey(key) {
		wantUser, wantPass := f.authUser, f.authPass
		f.mu.Unlock()
		user, pass, ok := strings.Cut(string(value), " ")
		if !ok || user != wantUser || pass != wantPass {
			if !noReply {
				_, _ = w.WriteString("CLIENT_ERROR authentication failure\r\n")
				_ = w.Flush()
			}
			return true
		}
		if !noReply {
			_, _ = w.WriteString("STORED\r\n")
			_ = w.Flush()
		}
		return true
	}
	existing, ok := f.items[key]
	switch cmd {
	case "add":
		if ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
	case "replace":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
	case "append":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
		value = append(append([]byte(nil), existing.value...), value...)
		flags64 = uint64(existing.flags)
	case "prepend":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_STORED\r\n")
				_ = w.Flush()
			}
			return true
		}
		value = append(value, existing.value...)
		flags64 = uint64(existing.flags)
	case "cas":
		if !ok {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("NOT_FOUND\r\n")
				_ = w.Flush()
			}
			return true
		}
		if existing.cas != casUnique {
			f.mu.Unlock()
			if !noReply {
				_, _ = w.WriteString("EXISTS\r\n")
				_ = w.Flush()
			}
			return true
		}
	}
	f.cas++
	f.items[key] = &fakeItem{value: value, flags: uint32(flags64), cas: f.cas}
	f.mu.Unlock()

	if !noReply {
		_, _ = w.WriteString("STORED\r\n")
		_ = w.Flush()
	}
	return true
}

func (f *fakeMemcached) delete(fields []string, w *bufio.Writer) {
	if len(fields) < 2 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	noReply := len(fields) > 2 && fields[2] == "noreply"
	f.mu.Lock()
	_, ok := f.items[fields[1]]
	if ok {
		delete(f.items, fields[1])
	}
	f.mu.Unlock()
	if noReply {
		return
	}
	if ok {
		_, _ = w.WriteString("DELETED\r\n")
	} else {
		_, _ = w.WriteString("NOT_FOUND\r\n")
	}
	_ = w.Flush()
}

func (f *fakeMemcached) touch(fields []string, w *bufio.Writer) {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	f.mu.Lock()
	item, ok := f.items[fields[1]]
	if ok {
		item.exptime = fields[2]
	}
	f.mu.Unlock()
	noReply := len(fields) > 3 && fields[3] == "noreply"
	if noReply {
		return
	}
	if ok {
		_, _ = w.WriteString("TOUCHED\r\n")
	} else {
		_, _ = w.WriteString("NOT_FOUND\r\n")
	}
	_ = w.Flush()
}
