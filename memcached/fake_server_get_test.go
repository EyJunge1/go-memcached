package memcached

import (
	"bufio"
	"fmt"
	"time"
)

func (f *fakeMemcached) retrieve(keys []string, withCAS bool, w *bufio.Writer) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	for _, key := range keys {
		item, ok := f.getLiveLocked(key, now)
		if !ok {
			continue
		}
		if withCAS {
			_, _ = fmt.Fprintf(w, "VALUE %s %d %d %d\r\n", key, item.flags, len(item.value), item.cas)
		} else {
			_, _ = fmt.Fprintf(w, "VALUE %s %d %d\r\n", key, item.flags, len(item.value))
		}
		_, _ = w.Write(item.value)
		_, _ = w.WriteString("\r\n")
	}
	_, _ = w.WriteString("END\r\n")
	_ = w.Flush()
}
