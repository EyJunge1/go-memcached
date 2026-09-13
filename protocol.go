package memcached

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Item represents a key-value pair to be got or stored.
type Item struct {
	Key   string
	Value []byte

	// Flags is the client flags stored with the item.
	Flags uint32
	// CAS is a unique value that is used to check-and-set operation.
	// It ONLY returns when you use `Gets` command.
	CAS uint64
}

func (i *Item) String() string {
	return "Item{" +
		"Key:" + i.Key +
		" Value:" + string(i.Value) +
		" Flags:" + strconv.FormatUint(uint64(i.Flags), 10) +
		" CAS:" + strconv.FormatUint(i.CAS, 10) +
		"}"
}

// MetaItem represents a key-value pair with meta information.
type MetaItem struct {
	Key   []byte
	Value []byte

	/* Meta */
	// CAS is a unique value that is used to check-and-set operation.
	// use MetaGetFlagReturnCAS() or MetaSetFlagReturnCAS() to get this value.
	CAS uint64
	// Flags is the client flags stored with the item when a value is returned.
	// use MetaGetFlagReturnClientFlags() to request it from the server.
	Flags uint32
	// TTL is the time-to-live of the item. -1 means never expire.
	// use MetaGetFlagReturnTTL() to get this value.
	TTL int64
	// LastAccessedTime is the time since item was last accessed in seconds.
	// use MetaGetFlagReturnLastAccessedTime() to get this value.
	LastAccessedTime int64
	// Size is the size of the stored item on success.
	// use MetaGetFlagReturnSize() or MetaSetFlagReturnSize() to get this value.
	Size uint64
	// Opaque is the opaque value when the token is decimal.
	// use MetaGetFlagOpaque(token) to get this value.
	Opaque uint64
	// OpaqueToken is the raw opaque token (up to 32 bytes).
	// use MetaGetFlagOpaqueToken(token) to request a string token.
	OpaqueToken string
	// HitBefore is the flag to return whether item has been hit before as a 0 or 1.
	// use MetaGetFlagReturnHitBefore() to get this value.
	HitBefore bool
	// Won is set when the server returned recache flag W.
	Won bool
	// Stale is set when the server returned stale flag X.
	Stale bool
	// RecacheBusy is set when the server returned winning-already-sent flag Z.
	RecacheBusy bool
	// CASMatched is true when MetaGetFlagCheckCAS was used with a value
	// request (v) and the server returned HD (CAS still matches).
	CASMatched bool
	gotHD      bool
}

func (m *MetaItem) String() string {
	return "MetaItem{" +
		"Key:" + string(m.Key) +
		" Value:" + string(m.Value) +
		" CAS:" + strconv.FormatUint(m.CAS, 10) +
		" Flags:" + strconv.FormatUint(uint64(m.Flags), 10) +
		" TTL:" + strconv.FormatInt(m.TTL, 10) +
		" LastAccessedTime:" + strconv.FormatInt(m.LastAccessedTime, 10) +
		" Size:" + strconv.FormatUint(m.Size, 10) +
		" Opaque:" + strconv.FormatUint(m.Opaque, 10) +
		" OpaqueToken:" + m.OpaqueToken +
		" HitBefore:" + strconv.FormatBool(m.HitBefore) +
		" Won:" + strconv.FormatBool(m.Won) +
		" Stale:" + strconv.FormatBool(m.Stale) +
		" RecacheBusy:" + strconv.FormatBool(m.RecacheBusy) +
		" CASMatched:" + strconv.FormatBool(m.CASMatched) +
		"}"
}

// MetaItemDebug represents a key-value pair with meta information for debug.
//
//	exp   = expiration time
//	la    = time in seconds since last access
//	cas   = CAS ID
//	fetch = whether an item has been fetched before
//	cls   = slab class id
//	size  = total size in bytes
type MetaItemDebug struct {
	Key            []byte // key
	TTL            int64  // exp, expiration time in seconds, -1 means never expire
	LastAccessTime int64  // la, time in seconds since last access
	CAS            uint64 // cas
	HitBefore      bool   // fetch
	SlabClassID    uint64 // cls
	Size           uint64 // size
}

func buildVersionCommand() (*request, *response) {
	req := buildRequest([]byte("version"), nil, []byte("version\r\n"))
	resp := buildLimitedLineResponse(1)
	return req, resp
}

func buildFlushAllCommand(delay time.Duration, noReply bool) (*request, *response) {
	b := newProtocolBuilder().AddString("flush_all")
	defer b.release()
	if delay > 0 {
		sec := delay / time.Second
		if sec == 0 {
			sec = 1
		}
		b.AddInt64(int64(sec))
	}
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	req := buildRequest([]byte("flush_all"), nil, b.AddCRLF().build())

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// buildStorageCommand constructs storage class command, including:
//
//	set/add/replace/append/prepend, etc.
//
// <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
// <data block>\r\n
const maxRelativeExpiry = 30 * 24 * time.Hour

// expiryToken maps a Go duration onto memcached's exptime integer.
// 0 stays 0 (never expire). Sub-second positives become 1. Negatives become -1
// (immediately expired). Values above 30 days become now+ttl Unix timestamps.
func expiryToken(d time.Duration) int64 {
	switch {
	case d == 0:
		return 0
	case d < 0:
		return -1
	case d < time.Second:
		return 1
	case d > maxRelativeExpiry:
		return nowFunc().Add(d).Unix()
	default:
		return int64(d / time.Second)
	}
}

func buildStorageCommand(
	command, key string,
	value []byte,
	flags uint32,
	exptime time.Duration,
	noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddString(key).                 // key
		AddUint(uint64(flags)).         // flags
		AddInt64(expiryToken(exptime)). // exptime
		AddInt(len(value))              // bytes
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().
		AddBytes(value). // data block
		AddCRLF().
		build()

	req := buildRequest([]byte(command), []byte(key), raw)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// delete <key> [noreply]\r\n
func buildDeleteCommand(key string, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString("delete").
		AddString(key)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := buildRequest([]byte("delete"), []byte(key), b.AddCRLF().build())

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// touch <key> <exptime> [noreply]\r\n
func buildTouchCommand(key string, expTime time.Duration, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString("touch").
		AddString(key).
		AddInt64(expiryToken(expTime))
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	req := buildRequest([]byte("touch"), []byte(key), b.AddCRLF().build())

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// cas <key> <flag> <exptime> <bytes> <cas unique> [noreply]\r\n
func buildCasCommand(
	key string, value []byte, flag uint32, expTime time.Duration, casUnique uint64, noReply bool,
) (*request, *response) {
	b := newProtocolBuilder().
		AddString("cas").               // command
		AddString(key).                 // key
		AddUint(uint64(flag)).          // flags
		AddInt64(expiryToken(expTime)). // exptime
		AddInt(len(value)).             // bytes
		AddUint(casUnique)              // cas unique
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().
		AddBytes(value). // data block
		AddCRLF().
		build()

	req := buildRequest([]byte("cas"), []byte(key), raw)

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// buildGetsCommand constructs gets command.
// get/gets <key>*\r\n
func buildGetsCommand(command string, keys ...string) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command)
	defer b.release()

	for _, key := range keys {
		b.AddString(key)
	}
	b.AddCRLF()

	var pickKey []byte
	if len(keys) == 1 {
		pickKey = []byte(keys[0])
	}
	req := buildRequest([]byte(command), pickKey, b.build())
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)

	return req, resp
}

// buildGetAndTouchCommand constructs get and touch command.
// gat/gats <exptime> <key>*\r\n
func buildGetAndTouchesCommand(command string, expiry time.Duration, keys ...string) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddInt64(expiryToken(expiry))
	defer b.release()

	for _, key := range keys {
		b.AddString(key)
	}

	b.AddCRLF()

	var pickKey []byte
	if len(keys) == 1 {
		pickKey = []byte(keys[0])
	}
	req := buildRequest([]byte(command), pickKey, b.build())
	resp := buildSpecEndLineResponse(_EndCRLFBytes, len(keys)*2+1)

	return req, resp
}

// parseValueItems parses the response from memcached server, the response
// is a list of items, each item is a key-value pair.
//
// For example:
// VALUE <key> <flags> <bytes> <cas unique>\r\n
// <data block>\r\n
// VALUE <key> <flags> <bytes> <cas unique>\r\n
// <data block>\r\n
// ...
// END\r\n
func parseValueItems(lines [][]byte, withoutEndLine, withCAS bool) (_ []*Item, err error) {
	n := len(lines)
	if withoutEndLine && n%2 != 0 {
		// n must be even
		return nil, fmt.Errorf("want times of 2 lines, got %d: %w", n, ErrMalformedResponse)
	}
	if !withoutEndLine && n%2 == 0 {
		// n must be odd
		return nil, fmt.Errorf("want odd lines, got %d: %w", n, ErrMalformedResponse)
	}

	var (
		rn      = n
		items   = make([]*Item, 0, (n/2)+1) // pre-alloc to avoid memory allocation
		dataLen uint64
	)

	if !withoutEndLine {
		// avoid iterate the last line "END\r\n"
		rn = n - 1
	}

	for i := 0; i < rn; i += 2 {
		line := trimCRLF(lines[i])
		if !bytes.HasPrefix(line, []byte("VALUE ")) {
			return nil, fmt.Errorf("unexpected retrieval line: %w", ErrMalformedResponse)
		}

		item := &Item{}
		dataLen, err = parseValueLine(line, item, withCAS)
		if err != nil {
			return nil, err
		}

		if i+1 >= n {
			return nil, fmt.Errorf("missing data block: %w", ErrMalformedResponse)
		}
		item.Value, err = dataBlockValue(lines[i+1], dataLen) //nolint:gosec // G602: i+1 is bounds-checked above
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

// dataBlockValue returns the payload of a length-framed VALUE/VA body.
// The reader stores size+CRLF; the value is the declared size, not a trim of the
// last CRLF (a value may itself end in CR/LF).
func dataBlockValue(raw []byte, size uint64) ([]byte, error) {
	if uint64(len(raw)) == size+2 && bytes.HasSuffix(raw, _CRLFBytes) {
		return raw[:size], nil
	}
	trimmed := trimCRLF(raw)
	if uint64(len(trimmed)) != size {
		return nil, fmt.Errorf("data block length mismatch: %w", ErrMalformedResponse)
	}
	return trimmed, nil
}

// parseValueLine extract item from VALUE line, like following:
// VALUE <key> <flags> <bytes> <cas unique> => Item{key, flags, cas}
//
// if withCas is false, VALUE line is:
// VALUE <key> <flags> <bytes> => Item{key, flags, 0}
func parseValueLine(line []byte, item *Item, withCas bool) (dataLen uint64, err error) {
	const (
		keyIndex     = 1
		flagsIndex   = 2
		dataLenIndex = 3
		casIndex     = 4
	)

	n := len(line)
	start := len(_ValueBytes)
	fieldStart := start
	nField := 0

	for i := start; i < n; i++ {
		if nField > 5 || (!withCas && nField > 4) {
			return 0, fmt.Errorf("invalid VALUE line: %w", ErrMalformedResponse)
		}

		if line[i] != ' ' && i != n-1 {
			continue
		}

		// another field starts from fieldStart to i, or the last field
		// the 'i' is the index of space or the last byte.
		switch nField {
		case keyIndex:
			item.Key = byteSliceToString(line[fieldStart:i])
		case flagsIndex:
			flags, err := parseUintFromBytes(line[fieldStart:i])
			if err != nil {
				return 0, fmt.Errorf("invalid flags: %w", ErrMalformedResponse)
			}
			if flags > math.MaxUint32 {
				return 0, fmt.Errorf("flags exceed uint32: %w", ErrMalformedResponse)
			}
			item.Flags = uint32(flags)
		case dataLenIndex:
			si := i
			if i == n-1 {
				si = i + 1
			}
			dataLen, err = parseUintFromBytes(line[fieldStart:si])
			if err != nil {
				return 0, fmt.Errorf("invalid data length: %w", ErrMalformedResponse)
			}
		case casIndex:
			si := i
			if i == n-1 {
				si = i + 1
			}
			item.CAS, err = parseUintFromBytes(line[fieldStart:si])
			if err != nil {
				return 0, fmt.Errorf("invalid CAS: %w", ErrMalformedResponse)
			}
		}

		fieldStart = i + 1
		nField++
	}

	if item.Key == "" {
		return 0, fmt.Errorf("empty VALUE key: %w", ErrMalformedResponse)
	}

	return dataLen, nil
}

// parseUintFromBytes parses slice of bytes to uint64.
//
// For example:
// []byte("1234567890") -> 1234567890
//
// If the slice of bytes is empty, it returns 0.
//
// If the slice of bytes contains non-digit characters, it returns an error.
func parseUintFromBytes(bs []byte) (uint64, error) {
	if len(bs) == 0 {
		return 0, nil
	}

	r := uint64(0)
	for _, b := range bs {
		if b < '0' || b > '9' {
			return 0, fmt.Errorf("invalid uint number: %w", ErrMalformedResponse)
		}

		digit := uint64(b - '0')
		if r > (math.MaxUint64-digit)/10 {
			return 0, fmt.Errorf("uint number overflows uint64: %w", ErrMalformedResponse)
		}
		r = r*10 + digit
	}

	return r, nil
}

// incr/decr <key> <value> [noreply]\r\n
func buildArithmeticCommand(command, key string, delta uint64, noReply bool) (*request, *response) {
	b := newProtocolBuilder().
		AddString(command).
		AddString(key).
		AddUint(delta)
	defer b.release()

	if noReply {
		b.AddBytes(_NoReplyBytes)
	}

	raw := b.AddCRLF().build()

	req := &request{
		cmd: []byte(command),
		key: []byte(key),
		raw: raw,
	}

	var resp *response
	if noReply {
		resp = buildNoReplyResponse()
	} else {
		resp = buildLimitedLineResponse(1)
	}

	return req, resp
}

// parseArithmeticResponse handles the response with incr/decr command.
// <value>\r\n
func parseArithmetic(line []byte) (uint64, error) {
	if len(line) == 0 {
		// noReply mode enabled
		return 0, nil
	}

	return strconv.ParseUint(string(trimCRLF(line)), 10, 64)
}

// Statistic represents the statistics of the memcached server.
type Statistic struct {
	PID                    int64
	Uptime                 int64 // seconds since the server started
	Time                   int64 // unix timestamp
	Version                string
	Libevent               string
	PointerSize            int
	RusageUser             float64 // seconds.microseconds
	RusageSystem           float64 // seconds.microseconds
	MaxConnections         int64
	CurrConnections        int64
	TotalConnections       int64
	RejectedConnections    int64
	ConnectionStructures   int64
	ResponseObjOOM         int64
	ResponseObjCount       int64
	ResponseObjBytes       int64
	ReadBufCount           int64
	ReadBufBytes           int64
	ReadBufBytesFree       int64
	ReadBufOOM             int64
	ReservedFDs            int64
	CmdGet                 int64
	CmdSet                 int64
	CmdFlush               int64
	CmdTouch               int64
	CmdMeta                int64
	GetHits                int64
	GetMisses              int64
	GetExpired             int64
	GetFlushed             int64
	DeleteMisses           int64
	DeleteHits             int64
	IncrMisses             int64
	IncrHits               int64
	DecrMisses             int64
	DecrHits               int64
	CasMisses              int64
	CasHits                int64
	CasBadval              int64
	TouchHits              int64
	TouchMisses            int64
	StoreTooLarge          int64
	StoreNoMemory          int64
	AuthCmds               int64
	AuthErrors             int64
	BytesRead              int64
	BytesWritten           int64
	LimitMaxbytes          int64
	AcceptingConns         bool
	ListenDisabledNum      int64
	TimeInListenDisabledUs int64
	Threads                int64
	ConnYields             int64
	HashPowerLevel         int64
	HashBytes              int64
	HashIsExpanding        bool

	SlabReassignRescues        int64
	SlabReassignChunkRescues   int64
	SlabReassignEvictionsNomem int64
	SlabReassignInlineReclaim  int64
	SlabReassignBusyItems      int64
	SlabReassignBusyDeletes    int64
	SlabReassignRunning        int64
	SlabsMoved                 int64

	LruCrawlerRunning    int64
	LruCrawlerStarts     int64
	LruMaintainerJuggles int64
	MallocFails          int64
	LogWorkerDropped     int64
	LogWorkerWritten     int64
	LogWatcherSkipped    int64
	LogWatcherSent       int64
	LogWatchers          int64
	UnexpectedNapiIDs    int64
	RoundRobinFallback   int64
	Bytes                int64
	CurrItems            int64
	TotalItems           int64
	SlabGlobalPagePool   int64
	ExpiredUnfetched     int64
	EvictedUnfetched     int64
	EvictedActive        int64
	Evictions            int64
	Reclaimed            int64
	CrawlerReclaimed     int64
	CrawlerItemsChecked  int64
	LrutailReflocked     int64

	MovesToCold    int64
	MovesToWarm    int64
	MovesWithinLRU int64

	DirectReclaims  int64
	LruBumpsDropped int64

	// Extra holds STAT keys that are not mapped to a typed field,
	// and known keys whose value could not be parsed.
	Extra map[string]string
}

func parseStats(lines [][]byte) (*Statistic, error) {
	if len(lines) == 0 {
		return nil, fmt.Errorf("empty response: %w", ErrMalformedResponse)
	}

	stat := &Statistic{}
	for _, line := range lines {
		line = trimCRLF(line)
		key, val, ok := cutStatLine(line)
		if !ok {
			continue
		}
		if !assignStat(stat, key, val) {
			if stat.Extra == nil {
				stat.Extra = make(map[string]string)
			}
			stat.Extra[key] = val
		}
	}
	return stat, nil
}

func cutStatLine(line []byte) (key, val string, ok bool) {
	const prefix = "STAT "
	if !bytes.HasPrefix(line, []byte(prefix)) {
		return "", "", false
	}
	rest := line[len(prefix):]
	name, value, found := bytes.Cut(rest, []byte{' '})
	if !found || len(name) == 0 {
		return "", "", false
	}
	return string(name), string(value), true
}

func assignStat(stat *Statistic, key, val string) bool {
	switch key {
	case "version":
		stat.Version = val
		return true
	case "libevent":
		stat.Libevent = val
		return true
	case "rusage_user":
		return setFloat(&stat.RusageUser, val)
	case "rusage_system":
		return setFloat(&stat.RusageSystem, val)
	case "accepting_conns":
		return setBool(&stat.AcceptingConns, val)
	case "hash_is_expanding":
		return setBool(&stat.HashIsExpanding, val)
	case "pointer_size":
		return setInt(&stat.PointerSize, val)
	case "pid":
		return setInt64(&stat.PID, val)
	case "uptime":
		return setInt64(&stat.Uptime, val)
	case "time":
		return setInt64(&stat.Time, val)
	case "max_connections":
		return setInt64(&stat.MaxConnections, val)
	case "curr_connections":
		return setInt64(&stat.CurrConnections, val)
	case "total_connections":
		return setInt64(&stat.TotalConnections, val)
	case "rejected_connections":
		return setInt64(&stat.RejectedConnections, val)
	case "connection_structures":
		return setInt64(&stat.ConnectionStructures, val)
	case "response_obj_oom":
		return setInt64(&stat.ResponseObjOOM, val)
	case "response_obj_count":
		return setInt64(&stat.ResponseObjCount, val)
	case "response_obj_bytes":
		return setInt64(&stat.ResponseObjBytes, val)
	case "read_buf_count":
		return setInt64(&stat.ReadBufCount, val)
	case "read_buf_bytes":
		return setInt64(&stat.ReadBufBytes, val)
	case "read_buf_bytes_free":
		return setInt64(&stat.ReadBufBytesFree, val)
	case "read_buf_oom":
		return setInt64(&stat.ReadBufOOM, val)
	case "reserved_fds":
		return setInt64(&stat.ReservedFDs, val)
	case "cmd_get":
		return setInt64(&stat.CmdGet, val)
	case "cmd_set":
		return setInt64(&stat.CmdSet, val)
	case "cmd_flush":
		return setInt64(&stat.CmdFlush, val)
	case "cmd_touch":
		return setInt64(&stat.CmdTouch, val)
	case "cmd_meta":
		return setInt64(&stat.CmdMeta, val)
	case "get_hits":
		return setInt64(&stat.GetHits, val)
	case "get_misses":
		return setInt64(&stat.GetMisses, val)
	case "get_expired":
		return setInt64(&stat.GetExpired, val)
	case "get_flushed":
		return setInt64(&stat.GetFlushed, val)
	case "delete_misses":
		return setInt64(&stat.DeleteMisses, val)
	case "delete_hits":
		return setInt64(&stat.DeleteHits, val)
	case "incr_misses":
		return setInt64(&stat.IncrMisses, val)
	case "incr_hits":
		return setInt64(&stat.IncrHits, val)
	case "decr_misses":
		return setInt64(&stat.DecrMisses, val)
	case "decr_hits":
		return setInt64(&stat.DecrHits, val)
	case "cas_misses":
		return setInt64(&stat.CasMisses, val)
	case "cas_hits":
		return setInt64(&stat.CasHits, val)
	case "cas_badval":
		return setInt64(&stat.CasBadval, val)
	case "touch_hits":
		return setInt64(&stat.TouchHits, val)
	case "touch_misses":
		return setInt64(&stat.TouchMisses, val)
	case "store_too_large":
		return setInt64(&stat.StoreTooLarge, val)
	case "store_no_memory":
		return setInt64(&stat.StoreNoMemory, val)
	case "auth_cmds":
		return setInt64(&stat.AuthCmds, val)
	case "auth_errors":
		return setInt64(&stat.AuthErrors, val)
	case "bytes_read":
		return setInt64(&stat.BytesRead, val)
	case "bytes_written":
		return setInt64(&stat.BytesWritten, val)
	case "limit_maxbytes":
		return setInt64(&stat.LimitMaxbytes, val)
	case "listen_disabled_num":
		return setInt64(&stat.ListenDisabledNum, val)
	case "time_in_listen_disabled_us":
		return setInt64(&stat.TimeInListenDisabledUs, val)
	case "threads":
		return setInt64(&stat.Threads, val)
	case "conn_yields":
		return setInt64(&stat.ConnYields, val)
	case "hash_power_level":
		return setInt64(&stat.HashPowerLevel, val)
	case "hash_bytes":
		return setInt64(&stat.HashBytes, val)
	case "slab_reassign_rescues":
		return setInt64(&stat.SlabReassignRescues, val)
	case "slab_reassign_chunk_rescues":
		return setInt64(&stat.SlabReassignChunkRescues, val)
	case "slab_reassign_evictions_nomem":
		return setInt64(&stat.SlabReassignEvictionsNomem, val)
	case "slab_reassign_inline_reclaim":
		return setInt64(&stat.SlabReassignInlineReclaim, val)
	case "slab_reassign_busy_items":
		return setInt64(&stat.SlabReassignBusyItems, val)
	case "slab_reassign_busy_deletes":
		return setInt64(&stat.SlabReassignBusyDeletes, val)
	case "slab_reassign_running":
		return setInt64(&stat.SlabReassignRunning, val)
	case "slabs_moved":
		return setInt64(&stat.SlabsMoved, val)
	case "lru_crawler_running":
		return setInt64(&stat.LruCrawlerRunning, val)
	case "lru_crawler_starts":
		return setInt64(&stat.LruCrawlerStarts, val)
	case "lru_maintainer_juggles":
		return setInt64(&stat.LruMaintainerJuggles, val)
	case "malloc_fails":
		return setInt64(&stat.MallocFails, val)
	case "log_worker_dropped":
		return setInt64(&stat.LogWorkerDropped, val)
	case "log_worker_written":
		return setInt64(&stat.LogWorkerWritten, val)
	case "log_watcher_skipped":
		return setInt64(&stat.LogWatcherSkipped, val)
	case "log_watcher_sent":
		return setInt64(&stat.LogWatcherSent, val)
	case "log_watchers":
		return setInt64(&stat.LogWatchers, val)
	case "unexpected_napi_ids":
		return setInt64(&stat.UnexpectedNapiIDs, val)
	case "round_robin_fallback":
		return setInt64(&stat.RoundRobinFallback, val)
	case "bytes":
		return setInt64(&stat.Bytes, val)
	case "curr_items":
		return setInt64(&stat.CurrItems, val)
	case "total_items":
		return setInt64(&stat.TotalItems, val)
	case "slab_global_page_pool":
		return setInt64(&stat.SlabGlobalPagePool, val)
	case "expired_unfetched":
		return setInt64(&stat.ExpiredUnfetched, val)
	case "evicted_unfetched":
		return setInt64(&stat.EvictedUnfetched, val)
	case "evicted_active":
		return setInt64(&stat.EvictedActive, val)
	case "evictions":
		return setInt64(&stat.Evictions, val)
	case "reclaimed":
		return setInt64(&stat.Reclaimed, val)
	case "crawler_reclaimed":
		return setInt64(&stat.CrawlerReclaimed, val)
	case "crawler_items_checked":
		return setInt64(&stat.CrawlerItemsChecked, val)
	case "lrutail_reflocked":
		return setInt64(&stat.LrutailReflocked, val)
	case "moves_to_cold":
		return setInt64(&stat.MovesToCold, val)
	case "moves_to_warm":
		return setInt64(&stat.MovesToWarm, val)
	case "moves_within_lru":
		return setInt64(&stat.MovesWithinLRU, val)
	case "direct_reclaims":
		return setInt64(&stat.DirectReclaims, val)
	case "lru_bumps_dropped":
		return setInt64(&stat.LruBumpsDropped, val)
	default:
		return false
	}
}

func setInt64(dst *int64, val string) bool {
	v, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false
	}
	*dst = v
	return true
}

func setInt(dst *int, val string) bool {
	v, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return false
	}
	*dst = int(v)
	return true
}

func setFloat(dst *float64, val string) bool {
	v, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return false
	}
	*dst = v
	return true
}

func setBool(dst *bool, val string) bool {
	v, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}
	*dst = v
	return true
}

func buildStatsCommand(subCommand string) (*request, *response) {
	b := newProtocolBuilder().
		AddString("stats")

	defer b.release()

	if subCommand != "" {
		b.AddString(subCommand)
	}

	raw := b.AddCRLF().
		build()

	req := buildRequest([]byte("stats"), nil, raw)
	if subCommand == "reset" {
		return req, buildLimitedLineResponse(1)
	}
	resp := buildSpecEndLineResponse(_EndCRLFBytes, 64)

	return req, resp
}

func parseStatsMap(lines [][]byte) (map[string]string, error) {
	out := make(map[string]string, len(lines))
	for _, line := range lines {
		trimmed := trimCRLF(line)
		if bytes.Equal(trimmed, []byte("END")) || bytes.Equal(trimmed, []byte("RESET")) {
			continue
		}
		if err := forecastCommonFaultLine(line); err != nil {
			return nil, err
		}
		key, val, ok := cutStatLine(trimmed)
		if !ok {
			continue
		}
		out[key] = val
	}
	return out, nil
}
