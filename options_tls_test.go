package memcached

import (
	"crypto/tls"
	"testing"
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
