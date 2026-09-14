package memcached

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func Test_tlsConfigForDial_setsServerName(t *testing.T) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	got := tlsConfigForDial(cfg, "tcp", "memcached.example.com:11211")
	equal(t, "memcached.example.com", got.ServerName)
	equal(t, "", cfg.ServerName)

	preset := &tls.Config{ServerName: "pinned.example"}
	got = tlsConfigForDial(preset, "tcp", "other.example:11211")
	equal(t, "pinned.example", got.ServerName)

	unix := tlsConfigForDial(cfg, "unix", "/tmp/memcached.sock")
	equal(t, "", unix.ServerName)
}

func Test_tlsConfigForDial_defaultsMinVersion(t *testing.T) {
	got := tlsConfigForDial(&tls.Config{}, "tcp", "127.0.0.1:11211")
	equal(t, uint16(tls.VersionTLS12), got.MinVersion)
}

func TestNewRejectsMixedUDPAndTCP(t *testing.T) {
	_, err := New(Config{Servers: []Server{
		{Address: "127.0.0.1:11211"},
		{Network: "udp", Address: "127.0.0.1:11212"},
	}})
	isErrorIs(t, err, ErrMixedTransport)
}

func TestNewRejectsTLSOnUDP(t *testing.T) {
	_, err := New(Config{
		Servers:   []Server{{Network: "udp", Address: "127.0.0.1:11211"}},
		Transport: Transport{TLS: &tls.Config{MinVersion: tls.VersionTLS12}},
	})
	isErrorIs(t, err, ErrNotSupported)
}

func TestNewRejectsInsecureTLSWithoutOptIn(t *testing.T) {
	_, err := New(Config{
		Servers: []Server{{Address: "127.0.0.1:11211"}},
		Transport: Transport{TLS: &tls.Config{
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: true,
		}},
	})
	isErrorIs(t, err, ErrInsecureTLS)
}

func TestNewAllowsInsecureTLSWithOptIn(t *testing.T) {
	_, err := New(Config{
		Servers: []Server{{Address: "127.0.0.1:11211"}},
		Transport: Transport{
			TLS: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
			},
			AllowInsecureTLS: true,
		},
	})
	noError(t, err)
}

func TestNewClonesTLSConfig(t *testing.T) {
	shared := &tls.Config{MinVersion: tls.VersionTLS12}
	client, err := New(Config{
		Servers:   []Server{{Address: "127.0.0.1:11211"}},
		Transport: Transport{TLS: shared},
	})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	shared.InsecureSkipVerify = true
	got := tlsConfigForDial(client.rt.tlsConfig, "tcp", "127.0.0.1:11211")
	isFalse(t, got.InsecureSkipVerify)
}

func TestWrapTLSDialRejectsInsecureWithoutOptIn(t *testing.T) {
	dial := wrapTLSDial(func(context.Context, string, string) (net.Conn, error) {
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return c1, nil
	}, &tls.Config{InsecureSkipVerify: true}, false)
	_, err := dial(context.Background(), "tcp", "127.0.0.1:11211")
	isErrorIs(t, err, ErrInsecureTLS)
}

func TestTLSWrapsCustomDial(t *testing.T) {
	addr, roots := startTLSVersionServer(t, "tls-ok")
	rawDials := 0
	client, err := New(Config{
		Servers: []Server{{Address: addr}},
		Transport: Transport{
			TLS: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    roots,
				ServerName: "localhost",
			},
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				rawDials++
				return (&net.Dialer{}).DialContext(ctx, network, address)
			},
			DialTimeout: 2 * time.Second,
			ReadTimeout: 200 * time.Millisecond,
		},
	})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ver, err := doVersion(client, ctx)
	noError(t, err)
	equal(t, []byte("tls-ok"), ver)
	greater(t, rawDials, 0)
}

func TestTLSHandshakeFailure(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	var closed atomic.Bool
	go func() {
		cn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		buf := make([]byte, 256)
		_, _ = cn.Read(buf)
		_ = cn.Close()
	}()

	client, err := New(Config{
		Servers: []Server{{Address: ln.Addr().String()}},
		Transport: Transport{
			TLS: &tls.Config{
				MinVersion:         tls.VersionTLS12,
				InsecureSkipVerify: true,
				ServerName:         "localhost",
			},
			AllowInsecureTLS: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				raw, err := (&net.Dialer{}).DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return &closeNotifyConn{Conn: raw, closed: &closed}, nil
			},
			DialTimeout: 500 * time.Millisecond,
		},
	})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	err = client.Version(context.Background(), new([]byte))
	isError(t, err)
	isTrue(t, closed.Load())
}

func TestTLSDefaultRawDial(t *testing.T) {
	addr, roots := startTLSVersionServer(t, "tls-default")
	client, err := New(Config{
		Servers: []Server{{Address: addr}},
		Transport: Transport{
			TLS: &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    roots,
				ServerName: "localhost",
			},
			DialTimeout: 2 * time.Second,
			ReadTimeout: 200 * time.Millisecond,
		},
	})
	noError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ver, err := doVersion(client, ctx)
	noError(t, err)
	equal(t, []byte("tls-default"), ver)
}

func startTLSVersionServer(t *testing.T, version string) (addr string, roots *x509.CertPool) {
	t.Helper()
	cert, key := mustTestTLSCert(t)
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{cert.Raw}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	})
	noError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadBytes('\n')
			if err != nil {
				return
			}
			cmd := strings.Fields(string(line))
			if len(cmd) == 0 {
				continue
			}
			switch cmd[0] {
			case "version":
				_, _ = conn.Write([]byte("VERSION " + version + "\r\n"))
			case "quit":
				return
			default:
				_, _ = conn.Write([]byte("ERROR\r\n"))
			}
		}
	}()
	roots = x509.NewCertPool()
	roots.AddCert(cert)
	return ln.Addr().String(), roots
}

type closeNotifyConn struct {
	net.Conn
	closed *atomic.Bool
}

func (c *closeNotifyConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func mustTestTLSCert(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	noError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	noError(t, err)
	cert, err := x509.ParseCertificate(der)
	noError(t, err)
	_ = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key
}
