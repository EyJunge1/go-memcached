package memcached

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

type storeParsed struct {
	cmd       string
	key       string
	flags64   uint64
	nbytes    int
	casUnique uint64
	noReply   bool
	idx       int
}

func parseStoreFields(fields []string, w *bufio.Writer) (storeParsed, bool) {
	if len(fields) < 5 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return storeParsed{}, false
	}
	flags64, err := strconv.ParseUint(fields[2], 10, 32)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR bad flags\r\n")
		_ = w.Flush()
		return storeParsed{}, false
	}
	nbytes, err := strconv.Atoi(fields[4])
	if err != nil || nbytes < 0 {
		_, _ = w.WriteString("CLIENT_ERROR bad bytes\r\n")
		_ = w.Flush()
		return storeParsed{}, false
	}
	p := storeParsed{
		cmd: fields[0], key: fields[1], flags64: flags64, nbytes: nbytes,
	}
	idx := 5
	if p.cmd == "cas" {
		if len(fields) < 6 {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return storeParsed{}, false
		}
		p.casUnique, err = strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			_, _ = w.WriteString("CLIENT_ERROR bad cas\r\n")
			_ = w.Flush()
			return storeParsed{}, false
		}
		idx = 6
	}
	p.idx = idx
	p.noReply = len(fields) > idx && fields[idx] == "noreply"
	return p, true
}

func (f *fakeMemcached) storeWriteReply(w *bufio.Writer, msg string, noReply bool) {
	if noReply {
		return
	}
	_, _ = w.WriteString(msg)
	_ = w.Flush()
}

func (f *fakeMemcached) storeAuthProbe(
	fields []string, key string,
) (handled bool, wantUser, wantPass string) {
	if !f.tokenAuth || fields[0] != "set" || !isAuthProbeKey(key) {
		return false, "", ""
	}
	return true, f.authUser, f.authPass
}

func (f *fakeMemcached) storeMutateValue(
	cmd string, existing *fakeItem, value []byte, flags64 uint64, casUnique uint64,
	w *bufio.Writer, noReply bool,
) (newValue []byte, newFlags uint64, done bool, ok bool) {
	switch cmd {
	case "add":
		if existing != nil {
			f.storeWriteReply(w, "NOT_STORED\r\n", noReply)
			return nil, 0, true, true
		}
	case "replace", "append", "prepend":
		if existing == nil {
			f.storeWriteReply(w, "NOT_STORED\r\n", noReply)
			return nil, 0, true, true
		}
		switch cmd {
		case "append":
			value = append(append([]byte(nil), existing.value...), value...)
			flags64 = uint64(existing.flags)
		case "prepend":
			value = append(value, existing.value...)
			flags64 = uint64(existing.flags)
		}
	case "cas":
		if existing == nil {
			f.storeWriteReply(w, "NOT_FOUND\r\n", noReply)
			return nil, 0, true, true
		}
		if existing.cas != casUnique {
			f.storeWriteReply(w, "EXISTS\r\n", noReply)
			return nil, 0, true, true
		}
	}
	return value, flags64, false, true
}

func (f *fakeMemcached) store(fields []string, r *bufio.Reader, w *bufio.Writer) bool {
	p, ok := parseStoreFields(fields, w)
	if !ok {
		return true
	}
	body := make([]byte, p.nbytes+2)
	if _, err := io.ReadFull(r, body); err != nil {
		return false
	}
	value := append([]byte(nil), body[:p.nbytes]...)

	f.mu.Lock()
	f.lastSet = strings.Join(fields, " ")
	if p.cmd == "set" && isAuthProbeKey(p.key) {
		f.authSets++
	}
	if handled, wantUser, wantPass := f.storeAuthProbe(fields, p.key); handled {
		// Unlock before auth reply I/O (justified; not a multi-path cleanup issue).
		f.mu.Unlock()
		user, pass, match := strings.Cut(string(value), " ")
		if !match || user != wantUser || pass != wantPass {
			f.storeWriteReply(w, "CLIENT_ERROR authentication failure\r\n", p.noReply)
			return true
		}
		f.storeWriteReply(w, "STORED\r\n", p.noReply)
		return true
	}
	defer f.mu.Unlock()
	existing := f.items[p.key]
	newValue, newFlags, done, alive := f.storeMutateValue(
		p.cmd, existing, value, p.flags64, p.casUnique, w, p.noReply,
	)
	if !alive {
		return false
	}
	if done {
		return true
	}
	f.cas++
	f.items[p.key] = &fakeItem{value: newValue, flags: uint32(newFlags), cas: f.cas} // #nosec G115 -- flags parsed with bitSize 32
	f.storeWriteReply(w, "STORED\r\n", p.noReply)
	return true
}

func (f *fakeMemcached) handleDelete(fields []string, w *bufio.Writer) {
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
	reply := "NOT_FOUND\r\n"
	if ok {
		reply = "DELETED\r\n"
	}
	_, _ = w.WriteString(reply)
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
	reply := "NOT_FOUND\r\n"
	if ok {
		reply = "TOUCHED\r\n"
	}
	_, _ = w.WriteString(reply)
	_ = w.Flush()
}
