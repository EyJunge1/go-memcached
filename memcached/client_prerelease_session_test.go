package memcached

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
)

func TestSetServersRemoveBeforeFirstDial(t *testing.T) {
	a := startFakeMemcached(t)
	b := startFakeMemcached(t)
	client := testClient(t, a.Addr(), func(cfg *Config) {
		cfg.AllowSetServers = true
	})

	addrs, _ := client.snapshotCluster()
	old := addrs[0]
	noError(t, client.SetServers([]Server{{Address: b.Addr()}}))

	_, err := client.getConn(context.Background(), old)
	isErrorIs(t, err, ErrInvalidAddress)
	client.mu.RLock()
	_, exists := client.connPools[old.id()]
	client.mu.RUnlock()
	isFalse(t, exists)
}

func TestSessionCloseAfterClientCloseReleasesCheckout(t *testing.T) {
	srv := startFakeMemcached(t)
	client := testClient(t, srv.Addr())
	session, err := client.Session(context.Background(), []byte("k"))
	noError(t, err)
	session = requirePtr(t, session)
	pool := session.cn.getConnPool()
	notNil(t, pool)
	equal(t, 1, int(pool.open.Load()))

	noError(t, client.Close())
	err = session.Store(context.Background(), Item{Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrClosed)
	noError(t, session.Close())
	equal(t, 0, int(pool.open.Load()))
	equal(t, 0, len(pool.idle))
}

func TestCheckoutPoolMapsErrPoolClosedToErrClosed(t *testing.T) {
	pool := newConnPool(1, 1, 0, 0, createConn)
	noError(t, pool.closePool())
	client := &Client{}
	client.closed.Store(true)
	_, err := client.checkoutPool(context.Background(), pool)
	isErrorIs(t, err, ErrClosed)
}

func TestCheckoutPoolKeepsErrPoolClosedWhenClientOpen(t *testing.T) {
	pool := newConnPool(1, 1, 0, 0, createConn)
	noError(t, pool.closePool())
	client := &Client{}
	_, err := client.checkoutPool(context.Background(), pool)
	isErrorIs(t, err, errPoolClosed)
}

func TestUDPConnReleaseRotatesOnEpoch(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { _ = serverSide.Close() })
	pool := newConnPool(1, 1, 0, 0, nil)
	oldEpoch := _udpRequestEpoch.Load()
	cn := &conn{
		raw:      clientSide,
		network:  "udp",
		udpEpoch: oldEpoch,
		pool:     pool,
	}
	pool.open.Store(1)
	_udpRequestEpoch.Store(oldEpoch + 1)
	t.Cleanup(func() { _udpRequestEpoch.Store(oldEpoch) })

	noError(t, cn.release())
	isTrue(t, cn.closed.Load())
	equal(t, 0, int(pool.open.Load()))
	equal(t, 0, pool.stats().IdleConns)
	noError(t, pool.closePool())
}

func TestUDPWrapRequestRotatesBeforeRecv(t *testing.T) {
	oldEpoch := _udpRequestEpoch.Load()
	t.Cleanup(func() {
		_udpRequestEpoch.Store(oldEpoch)
		_udpRequestID.Store(1)
	})

	var dials atomic.Int32
	freshSide, _ := net.Pipe()
	t.Cleanup(func() { _ = freshSide.Close() })

	pool := newConnPool(1, 1, 0, 0, func(context.Context) (wireConn, error) {
		dials.Add(1)
		return &conn{
			raw:      freshSide,
			network:  "udp",
			udpEpoch: _udpRequestEpoch.Load(),
			udpIn:    make([]byte, 64),
			udpOut:   make([]byte, 0, 64),
		}, nil
	})

	staleSide, _ := net.Pipe()
	t.Cleanup(func() { _ = staleSide.Close() })
	stale := &conn{
		raw:      staleSide,
		network:  "udp",
		udpEpoch: oldEpoch,
		pool:     pool,
		udpIn:    make([]byte, 64),
		udpOut:   make([]byte, 0, 64),
	}
	pool.open.Store(1)

	_udpRequestEpoch.Store(oldEpoch + 1)
	client := &Client{}
	node := newAddr("udp", "127.0.0.1:11211")
	node.pool.Store(pool)

	got, err := client.rotateUDPConnIfStale(context.Background(), node, stale)
	noError(t, err)
	isTrue(t, stale.closed.Load())
	equal(t, int32(1), dials.Load())
	fresh, ok := got.(*conn)
	isTrue(t, ok)
	equal(t, oldEpoch+1, fresh.udpEpoch)
	isFalse(t, udpConnEpochStale(fresh))
	noError(t, pool.discard(got))
	noError(t, pool.closePool())
}
