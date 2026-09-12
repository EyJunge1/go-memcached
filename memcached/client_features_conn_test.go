package memcached

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func TestConnCloseSkipsQuitOnUDP(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "udp", wr: bufio.NewWriter(client)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cn.Close()
	}()
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if n != 0 {
		t.Fatalf("udp close wrote %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected EOF or timeout on udp close")
	}
	<-done
}

func TestConnCloseTCPWritesQuit(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "tcp", wr: bufio.NewWriter(client)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cn.Close()
	}()
	_ = server.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	noError(t, err)
	equal(t, []byte("quit\r\n"), buf[:n])
	<-done
}

func TestConnCloseSilentSkipsQuit(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "tcp", wr: bufio.NewWriter(client)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cn.closeSilent()
	}()
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if n != 0 {
		t.Fatalf("silent close wrote %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected EOF or timeout on silent close")
	}
	<-done
}

func TestPoolCleanerCloseIsSilent(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{
		raw:        client,
		network:    "tcp",
		wr:         bufio.NewWriter(client),
		returnedAt: time.Now().Add(-time.Hour),
	}
	pool := newConnPool(1, 1, 0, time.Second, createConn)
	pool.open.Add(1)
	cn.setConnPool(pool)
	pool.idle = append(pool.idle, cn)
	pool.mu.Lock()
	_, closing := pool.connectionCleanerRunLocked(time.Second)
	pool.mu.Unlock()
	equal(t, 1, len(closing))
	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.closeReaped(closing)
	}()
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if n != 0 {
		t.Fatalf("cleaner close wrote %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected EOF or timeout on cleaner close")
	}
	<-done
}

func TestPoolOverflowCloseIsSilent(t *testing.T) {
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	cn := &conn{raw: client, network: "tcp", wr: bufio.NewWriter(client)}
	pool := newConnPool(0, 1, 0, 0, createConn)
	pool.open.Add(1)
	cn.setConnPool(pool)
	done := make(chan struct{})
	go func() {
		defer close(done)
		noError(t, pool.put(cn))
	}()
	_ = server.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 16)
	n, err := server.Read(buf)
	if n != 0 {
		t.Fatalf("overflow close wrote %q", buf[:n])
	}
	if err == nil {
		t.Fatal("expected EOF or timeout on overflow close")
	}
	<-done
}
