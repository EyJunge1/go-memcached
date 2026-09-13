package memcached

import (
	"bufio"
	"net"
)

func (f *fakeMemcached) slabs(fields []string, w *bufio.Writer) {
	if len(fields) >= 2 && fields[1] == "reassign" {
		if len(fields) >= 4 && fields[3] == "999" {
			_, _ = w.WriteString("BUSY\r\n")
			_ = w.Flush()
			return
		}
		if len(fields) >= 4 && fields[2] == fields[3] {
			_, _ = w.WriteString("SAME\r\n")
			_ = w.Flush()
			return
		}
	}
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
}

func (f *fakeMemcached) lruCrawler(fields []string, w *bufio.Writer) {
	if len(fields) >= 2 && (fields[1] == "metadump" || fields[1] == "mgdump") {
		sel := "all"
		if len(fields) >= 3 {
			sel = fields[2]
		}
		f.mu.Lock()
		f.dumpSel = sel
		busy := f.dumpBusy
		f.mu.Unlock()
		if busy {
			_, _ = w.WriteString("BUSY crawler\r\n")
			_ = w.Flush()
			return
		}
		if fields[1] == "metadump" {
			_, _ = w.WriteString("key=foo exp=-1 la=0 cas=1 fetch=no cls=1 size=3\r\nEND\r\n")
		} else {
			_, _ = w.WriteString("mg foo\r\nEND\r\n")
		}
		_ = w.Flush()
		return
	}
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
}

func (f *fakeMemcached) watch(_ []string, w *bufio.Writer, cn net.Conn) {
	f.mu.Lock()
	f.watchOK = true
	f.mu.Unlock()
	_, _ = w.WriteString("OK\r\n")
	_ = w.Flush()
	_, _ = w.WriteString("ts=100 gid=1 key=foo\r\n")
	_ = w.Flush()
	buf := make([]byte, 1)
	_, _ = cn.Read(buf)
}

func (f *fakeMemcached) stats(fields []string, w *bufio.Writer) {
	arg := ""
	if len(fields) > 1 {
		arg = fields[1]
	}
	if arg == "reset" {
		_, _ = w.WriteString("RESET\r\n")
		_ = w.Flush()
		return
	}
	if arg == "" {
		_, _ = w.WriteString("STAT pid 1\r\nSTAT version fake-1.0\r\nEND\r\n")
	} else {
		_, _ = w.WriteString("STAT arg " + arg + "\r\nEND\r\n")
	}
	_ = w.Flush()
}
