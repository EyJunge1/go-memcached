package memcached

import (
	"bufio"
	"net"
)

func (f *fakeMemcached) dispatchSessionEnd(cmd string, fields []string, w *bufio.Writer, cn net.Conn) bool {
	switch cmd {
	case "quit", "shutdown":
		return false
	case "watch":
		f.watch(fields, w, cn)
		return false
	default:
		return true
	}
}

func (f *fakeMemcached) dispatchMutation(cmd string, fields []string, r *bufio.Reader, w *bufio.Writer) (handled, keepConn bool) {
	switch cmd {
	case "set", "add", "replace", "append", "prepend", "cas":
		return true, f.store(fields, r, w)
	case "delete":
		f.handleDelete(fields, w)
		return true, true
	case "incr", "decr":
		f.arithmetic(fields, w)
		return true, true
	case "touch":
		f.touch(fields, w)
		return true, true
	default:
		return false, true
	}
}

func (f *fakeMemcached) dispatchMeta(cmd string, fields []string, r *bufio.Reader, w *bufio.Writer) (handled, keepConn bool) {
	switch cmd {
	case "mn":
		_, _ = w.WriteString("MN\r\n")
		_ = w.Flush()
		return true, true
	case "ms":
		return true, f.metaSet(fields, r, w)
	case "mg":
		f.metaGet(fields, w)
		return true, true
	case "md":
		f.metaDelete(fields, w)
		return true, true
	case "ma":
		f.metaArith(fields, w)
		return true, true
	case "me":
		f.metaDebug(fields, w)
		return true, true
	default:
		return false, true
	}
}

func (f *fakeMemcached) dispatchRead(cmd string, fields []string, w *bufio.Writer) bool {
	switch cmd {
	case "get", "gets":
		f.retrieve(fields[1:], cmd == "gets", w)
		return true
	case "gat", "gats":
		f.dispatchGAT(fields, w)
		return true
	default:
		return false
	}
}

func (f *fakeMemcached) dispatchAdmin(cmd string, fields []string, w *bufio.Writer) bool {
	switch cmd {
	case "version":
		_, _ = w.WriteString("VERSION fake-1.0\r\n")
		_ = w.Flush()
	case "flush_all":
		delay := ""
		noReply := len(fields) > 0 && fields[len(fields)-1] == "noreply"
		if len(fields) > 1 && fields[1] != "noreply" {
			delay = fields[1]
		}
		f.mu.Lock()
		f.lastFlushDelay = delay
		f.items = make(map[string]*fakeItem)
		f.mu.Unlock()
		if !noReply {
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
		}
	case "stats":
		f.stats(fields, w)
	case "verbosity", "cache_memlimit":
		if len(fields) == 0 || fields[len(fields)-1] != "noreply" {
			_, _ = w.WriteString("OK\r\n")
			_ = w.Flush()
		}
	case "slabs":
		f.slabs(fields, w)
	case "lru":
		_, _ = w.WriteString("OK\r\n")
		_ = w.Flush()
	case "lru_crawler":
		f.lruCrawler(fields, w)
	default:
		_, _ = w.WriteString("ERROR\r\n")
		_ = w.Flush()
		return false
	}
	return true
}

func (f *fakeMemcached) dispatchCommand(fields []string, r *bufio.Reader, w *bufio.Writer, cn net.Conn) bool {
	cmd := fields[0]
	if !f.dispatchSessionEnd(cmd, fields, w, cn) {
		return false
	}
	if handled, keep := f.dispatchMutation(cmd, fields, r, w); handled {
		return keep
	}
	if handled, keep := f.dispatchMeta(cmd, fields, r, w); handled {
		return keep
	}
	if f.dispatchRead(cmd, fields, w) {
		return true
	}
	if f.dispatchAdmin(cmd, fields, w) {
		return true
	}
	return true
}

func (f *fakeMemcached) dispatchGAT(fields []string, w *bufio.Writer) {
	if len(fields) < 3 {
		_, _ = w.WriteString("ERROR\r\n")
		_ = w.Flush()
		return
	}
	f.mu.Lock()
	for _, key := range fields[2:] {
		if item, ok := f.items[key]; ok {
			item.exptime = fields[1]
		}
	}
	f.mu.Unlock()
	f.retrieve(fields[2:], fields[0] == "gats", w)
}
