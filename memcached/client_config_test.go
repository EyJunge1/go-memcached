package memcached

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetConfigCluster(t *testing.T) {
	ln := mustListen(t, "tcp", "127.0.0.1:0")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func(cn net.Conn) {
				defer wg.Done()
				defer func() { _ = cn.Close() }()
				r := bufio.NewReader(cn)
				line, _ := r.ReadString('\n')
				if !strings.HasPrefix(line, "config get cluster") {
					_, _ = io.WriteString(cn, "ERROR\r\n")
					return
				}
				_, _ = io.WriteString(cn, "CONFIG cluster 0 8\r\n1\r\nh|127.0.0.1|11211\r\n\r\nEND\r\n")
			}(cn)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		wg.Wait()
	})

	c := testClient(t, ln.Addr().String(), func(cfg *Config) {
		cfg.Transport.DialTimeout = time.Second
		cfg.Transport.ReadTimeout = time.Second
		cfg.Transport.WriteTimeout = time.Second
	})
	var dst ConfigGetResult
	noError(t, c.GetConfig(context.Background(), "cluster", &dst))
	contains(t, string(dst.Body), "CONFIG")
	equal(t, "cluster", dst.Type)
}
