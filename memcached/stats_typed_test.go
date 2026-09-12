package memcached

import (
	"testing"
	"time"
)

func TestParseStatsSettings(t *testing.T) {
	got := ParseStatsSettings([]Stat{
		{Name: []byte("maxbytes"), Value: []byte("64")},
		{Name: []byte("evictions"), Value: []byte("on")},
		{Name: []byte("tcpport"), Value: []byte("11211")},
		{Name: []byte("oldest_live"), Value: []byte("1700000000")},
	})
	equal(t, uint64(64), got.MaxBytes)
	isTrue(t, got.Evictions)
	equal(t, int64(11211), got.TCPPort)
	equal(t, time.Unix(1700000000, 0), got.OldestLive)
}

func TestParseStatsItemsClasses(t *testing.T) {
	got := ParseStatsItemsClasses([]Stat{
		{Name: []byte("items:1:number"), Value: []byte("3")},
		{Name: []byte("items:1:age"), Value: []byte("42")},
		{Name: []byte("items:1:evicted"), Value: []byte("1")},
		{Name: []byte("items:2:number"), Value: []byte("9")},
	})
	hasLen(t, got, 2)
	equal(t, uint64(1), got[0].Class)
	equal(t, uint64(3), got[0].Number)
	equal(t, 42*time.Second, got[0].Age)
	equal(t, uint64(1), got[0].Evicted)
	equal(t, uint64(2), got[1].Class)
}

func TestParseStatsTLSProxyExtstore(t *testing.T) {
	tlsStats := ParseStatsTLS([]Stat{
		{Name: []byte("ssl_handshake_errors"), Value: []byte("2")},
		{Name: []byte("ssl_min_version"), Value: []byte("tlsv1.2")},
		{Name: []byte("time_since_server_cert_refresh"), Value: []byte("9")},
	})
	equal(t, uint64(2), tlsStats.SSLHandshakeErrors)
	equal(t, "tlsv1.2", tlsStats.SSLMinVersion)
	equal(t, 9*time.Second, tlsStats.TimeSinceServerCertRefresh)

	proxy := ParseStatsProxy([]Stat{
		{Name: []byte("proxy_config_version"), Value: []byte("3")},
		{Name: []byte("proxy_foo"), Value: []byte("bar")},
		{Name: []byte("curr_items"), Value: []byte("1")},
	})
	equal(t, uint64(3), proxy.ConfigVersion)
	equal(t, "bar", proxy.Extra["proxy_foo"])

	ext := ParseStatsExtstore([]Stat{
		{Name: []byte("extstore_page_allocs"), Value: []byte("4")},
		{Name: []byte("extstore_custom"), Value: []byte("x")},
	})
	equal(t, uint64(4), ext.PageAllocs)
	equal(t, "x", ext.Extra["extstore_custom"])
}
