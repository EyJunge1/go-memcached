package memcached

import (
	"math"
	"strconv"
	"strings"
	"time"
)

// StatsSettings is a typed view of common `stats settings` keys.
// Unknown keys are ignored; zero means absent or unparsable.
type StatsSettings struct {
	MaxBytes        uint64
	MaxConns        int64
	TCPPort         int64
	UDPPort         int64
	Verbose         int64
	OldestLive      time.Time
	Evictions       bool
	ItemSizeMax     uint64
	MaxReqsPerEvent int64
	// Raw is a copy of the input slice.
	Raw []Stat
}

// StatsItemsClass is one `stats items` slab class row (prefix items:<class>:).
type StatsItemsClass struct {
	Class       uint64
	Number      uint64
	Age         time.Duration
	Evicted     uint64
	Outofmemory uint64
}

// ParseStatsSettings maps STAT pairs into StatsSettings.
func ParseStatsSettings(stats []Stat) StatsSettings {
	out := StatsSettings{Raw: append([]Stat(nil), stats...)}
	for _, s := range stats {
		val := string(s.Value)
		switch string(s.Name) {
		case "maxbytes":
			out.MaxBytes, _ = strconv.ParseUint(val, 10, 64)
		case "maxconns":
			out.MaxConns, _ = strconv.ParseInt(val, 10, 64)
		case "tcpport":
			out.TCPPort, _ = strconv.ParseInt(val, 10, 64)
		case "udpport":
			out.UDPPort, _ = strconv.ParseInt(val, 10, 64)
		case "verbosity":
			out.Verbose, _ = strconv.ParseInt(val, 10, 64)
		case "oldest_live":
			if sec, err := strconv.ParseInt(val, 10, 64); err == nil {
				out.OldestLive = time.Unix(sec, 0)
			}
		case "evictions":
			out.Evictions = val == "on" || val == "1"
		case "item_size_max":
			out.ItemSizeMax, _ = strconv.ParseUint(val, 10, 64)
		case "reqs_per_event":
			out.MaxReqsPerEvent, _ = strconv.ParseInt(val, 10, 64)
		}
	}
	return out
}

// ParseStatsItemsClasses extracts items:<id>:* counters into per-class rows.
func ParseStatsItemsClasses(stats []Stat) []StatsItemsClass {
	byClass := make(map[uint64]*StatsItemsClass, len(stats))
	order := make([]uint64, 0, len(stats))
	for _, s := range stats {
		name := string(s.Name)
		// items:<class>:<field>
		if len(name) < 8 || name[:6] != "items:" {
			continue
		}
		rest := name[6:]
		classStr, field, ok := splitOnce(rest, ':')
		if !ok {
			continue
		}
		class, err := strconv.ParseUint(classStr, 10, 64)
		if err != nil {
			continue
		}
		row, exists := byClass[class]
		if !exists {
			row = &StatsItemsClass{Class: class}
			byClass[class] = row
			order = append(order, class)
		}
		v, _ := strconv.ParseUint(string(s.Value), 10, 64)
		switch field {
		case "number":
			row.Number = v
		case "age":
			sec, err := strconv.ParseInt(string(s.Value), 10, 64)
			if err != nil || sec < 0 {
				continue
			}
			row.Age = time.Duration(sec) * time.Second
		case "evicted":
			row.Evicted = v
		case "outofmemory":
			row.Outofmemory = v
		}
	}
	out := make([]StatsItemsClass, 0, len(order))
	for _, c := range order {
		row := byClass[c]
		if row == nil {
			continue
		}
		out = append(out, *row)
	}
	return out
}

// StatsTLS is a typed view of common TLS-related STAT keys (default stats / tls builds).
type StatsTLS struct {
	SSLHandshakeErrors         uint64
	SSLProtoErrors             uint64
	SSLMinVersion              string
	SSLNewSessions             uint64
	TimeSinceServerCertRefresh time.Duration
	// Raw is a copy of the input slice.
	Raw []Stat
}

// ParseStatsTLS maps STAT pairs into StatsTLS.
func ParseStatsTLS(stats []Stat) StatsTLS {
	out := StatsTLS{Raw: append([]Stat(nil), stats...)}
	for _, s := range stats {
		val := string(s.Value)
		switch string(s.Name) {
		case "ssl_handshake_errors":
			out.SSLHandshakeErrors, _ = strconv.ParseUint(val, 10, 64)
		case "ssl_proto_errors":
			out.SSLProtoErrors, _ = strconv.ParseUint(val, 10, 64)
		case "ssl_min_version":
			out.SSLMinVersion = val
		case "ssl_new_sessions":
			out.SSLNewSessions, _ = strconv.ParseUint(val, 10, 64)
		case "time_since_server_cert_refresh":
			if sec, err := strconv.ParseInt(val, 10, 64); err == nil && sec >= 0 {
				const maxSec = math.MaxInt64 / int64(time.Second)
				if sec > maxSec {
					sec = maxSec
				}
				out.TimeSinceServerCertRefresh = time.Duration(sec) * time.Second
			}
		}
	}
	return out
}

// StatsProxy is a typed view of common proxy_* STAT keys.
type StatsProxy struct {
	ConfigVersion uint64
	ConfigBytes   uint64
	ConfigReload  uint64
	// Raw is a copy of the input slice.
	Raw []Stat
	// Extra holds other proxy_* keys not mapped above.
	Extra map[string]string
}

// ParseStatsProxy maps STAT pairs whose names start with proxy_ into StatsProxy.
func ParseStatsProxy(stats []Stat) StatsProxy {
	out := StatsProxy{Raw: append([]Stat(nil), stats...), Extra: make(map[string]string, len(stats))}
	for _, s := range stats {
		name := string(s.Name)
		if len(name) < 6 || name[:6] != "proxy_" {
			continue
		}
		val := string(s.Value)
		switch name {
		case "proxy_config_version":
			out.ConfigVersion, _ = strconv.ParseUint(val, 10, 64)
		case "proxy_config_bytes":
			out.ConfigBytes, _ = strconv.ParseUint(val, 10, 64)
		case "proxy_config_reload":
			out.ConfigReload, _ = strconv.ParseUint(val, 10, 64)
		default:
			out.Extra[name] = val
		}
	}
	return out
}

// StatsExtstore is a typed view of common extstore STAT keys.
type StatsExtstore struct {
	PageAllocs    uint64
	PageExpiries  uint64
	PageEvictions uint64
	PageReclaims  uint64
	PageOutBytes  uint64
	// Raw is a copy of the input slice.
	Raw   []Stat
	Extra map[string]string
}

// ParseStatsExtstore maps STAT pairs whose names start with extstore_ or are
// known extstore counters into StatsExtstore.
func ParseStatsExtstore(stats []Stat) StatsExtstore {
	out := StatsExtstore{Raw: append([]Stat(nil), stats...), Extra: make(map[string]string, len(stats))}
	for _, s := range stats {
		name := string(s.Name)
		val := string(s.Value)
		switch name {
		case "extstore_page_allocs", "page_allocs":
			out.PageAllocs, _ = strconv.ParseUint(val, 10, 64)
		case "extstore_page_expiries", "page_expiries":
			out.PageExpiries, _ = strconv.ParseUint(val, 10, 64)
		case "extstore_page_evictions", "page_evictions":
			out.PageEvictions, _ = strconv.ParseUint(val, 10, 64)
		case "extstore_page_reclaims", "page_reclaims":
			out.PageReclaims, _ = strconv.ParseUint(val, 10, 64)
		case "extstore_bytes_written", "bytes_written":
			out.PageOutBytes, _ = strconv.ParseUint(val, 10, 64)
		default:
			if len(name) >= 9 && name[:9] == "extstore_" {
				out.Extra[name] = val
			}
		}
	}
	return out
}

func splitOnce(s string, sep byte) (a, b string, ok bool) {
	return strings.Cut(s, string(sep))
}
