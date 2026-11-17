package memcached

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeItem struct {
	value       []byte
	flags       uint32
	cas         uint64
	stale       bool
	recacheBusy bool
	// expiresAt is absolute expiry; zero means never expires (wire exptime 0).
	expiresAt time.Time
}

// fakeExpiryFromToken converts a memcached wire expiry token to an absolute time.
// 0 → never (zero Time); negative → already expired; 1–2592000 → relative seconds;
// larger → Unix timestamp. Matches real memcached protocol semantics.
func fakeExpiryFromToken(token int64, now time.Time) time.Time {
	switch {
	case token == 0:
		return time.Time{}
	case token < 0:
		return now.Add(-time.Second)
	case token <= _maxRelativeExpirySecs:
		return now.Add(time.Duration(token) * time.Second)
	default:
		return time.Unix(token, 0)
	}
}

// fakeRemainingTTLSecs returns remaining TTL seconds for meta t, or -1 if never.
// Expired items should be purged before calling this.
func fakeRemainingTTLSecs(item *fakeItem, now time.Time) int64 {
	if item == nil || item.expiresAt.IsZero() {
		return -1
	}
	sec := int64(item.expiresAt.Sub(now) / time.Second)
	if sec < 0 {
		return 0
	}
	return sec
}

// purgeExpiredLocked deletes the key when past expiresAt. Caller must hold f.mu.
func (f *fakeMemcached) purgeExpiredLocked(key string, item *fakeItem, now time.Time) bool {
	if item == nil || item.expiresAt.IsZero() || !now.After(item.expiresAt) {
		return false
	}
	delete(f.items, key)
	return true
}

// getLiveLocked returns a non-expired item, purging if needed. Caller must hold f.mu.
func (f *fakeMemcached) getLiveLocked(key string, now time.Time) (*fakeItem, bool) {
	item, ok := f.items[key]
	if !ok {
		return nil, false
	}
	if f.purgeExpiredLocked(key, item, now) {
		return nil, false
	}
	return item, true
}

type fakeMemcached struct {
	ln             net.Listener
	wg             sync.WaitGroup
	mu             sync.Mutex
	items          map[string]*fakeItem
	cas            uint64
	lastCmd        string
	lastSet        string
	authSets       int
	watchOK        bool
	dumpSel        string
	dumpBusy       bool
	tokenAuth      bool
	authUser       string
	authPass       string
	lastFlushDelay string
	dumpSendOK     bool
	dumpEmpty      bool
}

func startFakeMemcachedAuth(t *testing.T) *fakeMemcached {
	t.Helper()
	f := startFakeMemcached(t)
	f.tokenAuth = true
	f.authUser = "u"
	f.authPass = "p"
	return f
}

func startFakeMemcached(t testing.TB) *fakeMemcached {
	t.Helper()
	return serveFake(t, mustListen(t, "tcp", "127.0.0.1:0"))
}

func startFakeMemcachedUnix(t *testing.T) (*fakeMemcached, string) {
	t.Helper()
	// macOS sun_path is 104 bytes; t.TempDir() plus the test name is too long.
	path := os.TempDir() + "/g" + strconv.FormatInt(time.Now().UnixNano(), 16) + ".sock"
	t.Cleanup(func() { _ = os.Remove(path) })
	ln := mustListen(t, "unix", path)
	if err := os.Chmod(path, 0o770); err != nil { // #nosec G302 -- group-writable so mode checks have a bit to reject
		t.Fatal(err)
	}
	return serveFake(t, ln), path
}

func serveFake(t testing.TB, ln net.Listener) *fakeMemcached {
	t.Helper()
	if ln == nil {
		t.Fatal("nil listener")
	}
	f := &fakeMemcached{ln: ln, items: make(map[string]*fakeItem)}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		f.serve()
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-serveDone
		f.wg.Wait()
	})
	return f
}

func (f *fakeMemcached) Addr() string {
	return f.ln.Addr().String()
}

func (f *fakeMemcached) serve() {
	for {
		cn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.wg.Add(1)
		go f.handle(cn)
	}
}

func (f *fakeMemcached) handle(cn net.Conn) {
	defer f.wg.Done()
	defer func() { _ = cn.Close() }()
	r := bufio.NewReader(cn)
	w := bufio.NewWriter(cn)

	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(string(bytes.TrimRight(line, "\r\n")))
		if len(fields) == 0 {
			continue
		}
		f.mu.Lock()
		if fields[0] != "quit" {
			f.lastCmd = strings.Join(fields, " ")
		}
		f.mu.Unlock()
		if !f.dispatchCommand(fields, r, w, cn) {
			return
		}
	}
}

type prefixPicker struct {
	addrs []*addr
}

var _ picker = prefixPicker{}

func (p prefixPicker) Pick(_ []*addr, key []byte) (*addr, error) {
	if len(p.addrs) == 0 {
		return nil, ErrInvalidAddress
	}
	if len(p.addrs) > 1 && bytes.HasPrefix(key, []byte("b-")) {
		return p.addrs[1], nil
	}
	return p.addrs[0], nil
}

type firstAddrPicker struct{}

var _ picker = firstAddrPicker{}

func (firstAddrPicker) Pick(addrs []*addr, _ []byte) (*addr, error) {
	if len(addrs) == 0 {
		return nil, ErrInvalidAddress
	}
	return addrs[0], nil
}

type firstAddrPickBuilder struct{}

var _ builder = firstAddrPickBuilder{}

func (firstAddrPickBuilder) Build([]*addr) picker {
	return firstAddrPicker{}
}

type lastAddrPicker struct{}

var _ picker = lastAddrPicker{}

func (lastAddrPicker) Pick(addrs []*addr, _ []byte) (*addr, error) {
	if len(addrs) == 0 {
		return nil, ErrInvalidAddress
	}
	return addrs[len(addrs)-1], nil
}

type lastAddrPickBuilder struct{}

var _ builder = lastAddrPickBuilder{}

func (lastAddrPickBuilder) Build([]*addr) picker {
	return lastAddrPicker{}
}

type prefixPickBuilder struct{}

var _ builder = prefixPickBuilder{}

func (prefixPickBuilder) Build(addrs []*addr) picker {
	return prefixPicker{addrs: addrs}
}
