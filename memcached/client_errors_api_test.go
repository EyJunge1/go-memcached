package memcached

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func startCmdServer(t *testing.T, reply func(fields []string) string) (addr string, dials *atomic.Int32) {
	t.Helper()
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	var n atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(string(line))
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "set", "add", "replace", "append", "prepend", "cas":
						if len(fields) >= 5 {
							if nbytes, err := strconv.Atoi(fields[4]); err == nil && nbytes >= 0 {
								_, _ = io.ReadFull(r, make([]byte, nbytes+2))
							}
						}
					case "quit", "shutdown":
						return
					}
					if out := reply(fields); out != "" {
						_, _ = cn.Write([]byte(out))
					}
				}
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String(), &n
}

// startCloseAfterCommand is like startCmdServer but closes the socket after
// reading cmd (and its data block). version gets a reply; other verbs get ERROR.
func startCloseAfterCommand(t *testing.T, cmd string) (addr string, dials *atomic.Int32) {
	t.Helper()
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	var n atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			n.Add(1)
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				errReply := []byte("ERROR\r\n")
				for {
					line, err := r.ReadBytes('\n')
					if err != nil {
						return
					}
					fields := strings.Fields(string(line))
					if len(fields) == 0 {
						continue
					}
					switch fields[0] {
					case "set", "add", "replace", "append", "prepend", "cas":
						if len(fields) >= 5 {
							if nbytes, err := strconv.Atoi(fields[4]); err == nil && nbytes >= 0 {
								_, _ = io.ReadFull(r, make([]byte, nbytes+2))
							}
						}
					case "quit", "shutdown":
						return
					}
					if fields[0] == cmd {
						return
					}
					switch fields[0] {
					case "version":
						_, _ = cn.Write([]byte("VERSION after-close\r\n"))
					default:
						_, _ = cn.Write(errReply)
					}
				}
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})
	return ln.Addr().String(), &n
}

func TestCommandErrorHeldOnGet(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "ERROR bad args\r\n"
		case "version":
			return "VERSION held\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr, func(cfg *Config) {
		cfg.Transport.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}
	})
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrCommandError)
	isFalse(t, errors.Is(err, ErrTransport))
	contains(t, errText(t, err), "bad args")
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("held"), ver)
	equal(t, int32(1), dials.Load())
}

func TestPipelineClientErrorHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "set":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-pipe\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	err := client.Pipeline().Store(Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")}).Run(context.Background())
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad flags")
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-pipe"), ver)
	equal(t, int32(1), dials.Load())
}

func TestGetAcceptsLFOnlyEND(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "END\n"
		case "version":
			return "VERSION lf-end\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	noError(t, err)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("lf-end"), ver)
	equal(t, int32(1), dials.Load())
}

func TestIncrWithQuietClientError(t *testing.T) {
	addr, _ := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "incr":
			return "CLIENT_ERROR bad delta\r\n"
		case "version":
			return "VERSION after-incr\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	var n uint64
	err := client.Incr(context.Background(), []byte("n"), 1, &n, ArithmeticOpts{})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad delta")
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-incr"), ver)
}

func TestParseArithmeticAfterRecvForecastsFaults(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		want error
	}{
		{
			name: "client error",
			line: []byte("CLIENT_ERROR cannot increment or decrement non-numeric value\r\n"),
			want: ErrClientError,
		},
		{
			name: "not found",
			line: []byte("NOT_FOUND\r\n"),
			want: ErrNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mapArithmeticReply(&response{rawLines: [][]byte{tt.line}})
			isErrorIs(t, err, tt.want)
			isFalse(t, errors.Is(err, ErrMalformedResponse))
		})
	}
}

func TestGetNonexistentCommandHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "ERROR\r\n"
		case "version":
			return "VERSION after-error\r\n"
		default:
			return "CLIENT_ERROR unexpected\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrNonexistentCommand)
	isFalse(t, shouldDiscardConn(err))
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-error"), ver)
	equal(t, int32(1), dials.Load())
}

func TestGetClientErrorHeld(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "CLIENT_ERROR bad flags\r\n"
		case "version":
			return "VERSION after-get\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrClientError)
	contains(t, errText(t, err), "bad flags")
	isFalse(t, shouldDiscardConn(err))
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-get"), ver)
	equal(t, int32(1), dials.Load())
}

func TestGetAndStoreServerErrorDiscards(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "SERVER_ERROR oom\r\n"
		case "set":
			return "SERVER_ERROR oom\r\n"
		case "version":
			return "VERSION after-oom\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	_, err := doGet(context.Background(), client, [][]byte{[]byte("k")}, GetOpts{})
	isErrorIs(t, err, ErrServerError)
	isTrue(t, shouldDiscardConn(err))
	err = client.Store(context.Background(), Item{Mode: StoreSet, Key: []byte("k"), Value: []byte("v")})
	isErrorIs(t, err, ErrServerError)
	isTrue(t, shouldDiscardConn(err))
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-oom"), ver)
	if dials.Load() < 3 {
		t.Fatalf("SERVER_ERROR must discard; dials=%d", dials.Load())
	}
}

func TestGetValueBodyLooksLikeCLIENT_ERROR(t *testing.T) {
	addr, dials := startCmdServer(t, func(fields []string) string {
		switch fields[0] {
		case "get", "gets":
			return "VALUE k 0 12\r\nCLIENT_ERROR\r\nEND\r\n"
		case "version":
			return "VERSION after-body\r\n"
		default:
			return "ERROR\r\n"
		}
	})
	client := testClient(t, addr)
	item := mustGet(context.Background(), t, client, []byte("k"))
	equal(t, []byte("CLIENT_ERROR"), item.Value)
	ver, err := doVersion(context.Background(), client)
	noError(t, err)
	equal(t, []byte("after-body"), ver)
	equal(t, int32(1), dials.Load())
}
