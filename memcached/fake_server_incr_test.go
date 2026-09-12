package memcached

import (
	"bufio"
	"strconv"
)

func (f *fakeMemcached) arithmetic(fields []string, w *bufio.Writer) {
	if len(fields) < 3 {
		_, _ = w.WriteString("CLIENT_ERROR bad command line\r\n")
		_ = w.Flush()
		return
	}
	delta, err := strconv.ParseUint(fields[2], 10, 64)
	if err != nil {
		_, _ = w.WriteString("CLIENT_ERROR bad delta\r\n")
		_ = w.Flush()
		return
	}
	noReply := len(fields) > 3 && fields[3] == "noreply"
	f.mu.Lock()
	defer f.mu.Unlock()
	item, ok := f.items[fields[1]]
	if !ok {
		if !noReply {
			_, _ = w.WriteString("NOT_FOUND\r\n")
			_ = w.Flush()
		}
		return
	}
	cur, err := strconv.ParseUint(string(item.value), 10, 64)
	if err != nil {
		if !noReply {
			_, _ = w.WriteString("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n")
			_ = w.Flush()
		}
		return
	}
	if fields[0] == "incr" {
		cur += delta
	} else if delta > cur {
		cur = 0
	} else {
		cur -= delta
	}
	item.value = []byte(strconv.FormatUint(cur, 10))
	f.cas++
	item.cas = f.cas
	if !noReply {
		_, _ = w.WriteString(strconv.FormatUint(cur, 10) + "\r\n")
		_ = w.Flush()
	}
}
