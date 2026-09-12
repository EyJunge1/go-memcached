package memcached

import (
	"bytes"
	"errors"
	"fmt"
)

const (
	_maxKeySize           = 250
	_defaultMaxValueBytes = 1 << 20
	_maxValueSize         = _defaultMaxValueBytes
)

var (
	_SpaceBytes    = []byte{' '}
	_SpaceByte     = byte(' ')
	_CRLFBytes     = []byte("\r\n")
	_NoReplyBytes  = []byte("noreply")
	_QuitCRLFBytes = []byte("quit\r\n")

	_OKCRLFBytes      = []byte("OK\r\n")
	_ValueBytes       = []byte("VALUE")
	_ValueSpaceBytes  = []byte("VALUE ")
	_VASpaceBytes     = []byte("VA ")
	_EndBytes         = []byte("END")
	_EndCRLFBytes     = []byte("END\r\n")
	_StoredBytes      = []byte("STORED")
	_StoredCRLFBytes  = []byte("STORED\r\n")
	_DeletedCRLFBytes = []byte("DELETED\r\n")
	_TouchedCRLFBytes = []byte("TOUCHED\r\n")
	_NotFoundBytes    = []byte("NOT_FOUND")
	_ExistsBytes      = []byte("EXISTS")
	_NotStoredBytes   = []byte("NOT_STORED")
	_VersionBytes     = []byte("VERSION")
	_VersionCmdBytes  = []byte("version")
	_VersionCRLFBytes = []byte("version\r\n")
	_StatsCmdBytes    = []byte("stats")

	_MetaMNCRLFBytes = []byte("MN\r\n")
	_MnCRLFBytes     = []byte("mn\r\n")
	_CRLFMnCRLFBytes = []byte("\r\nmn\r\n")
	_DeletedBytes    = []byte("DELETED")
	_TouchedBytes    = []byte("TOUCHED")
	_OKBytes         = []byte("OK")
	_RESETBytes      = []byte("RESET")
	_STATBytes       = []byte("STAT")
	_MNBytes         = []byte("MN")
	_HDBytes         = []byte("HD")
	_VABytes         = []byte("VA")
	_ENBytes         = []byte("EN")
	_NFBytes         = []byte("NF")
	_NSBytes         = []byte("NS")
	_EXBytes         = []byte("EX")
	_MEBytes         = []byte("ME")
	_HDSpaceBytes    = []byte("HD ")
	_MESpaceBytes    = []byte("ME ")

	_ClientErrorBytes = []byte("CLIENT_ERROR")
	_ServerErrorBytes = []byte("SERVER_ERROR")
	_ErrorBytes       = []byte("ERROR")

	_GetBytes           = []byte("get")
	_GetsBytes          = []byte("gets")
	_GatBytes           = []byte("gat")
	_GatsBytes          = []byte("gats")
	_SetBytes           = []byte("set")
	_AddBytes           = []byte("add")
	_ReplaceBytes       = []byte("replace")
	_AppendBytes        = []byte("append")
	_PrependBytes       = []byte("prepend")
	_CasBytes           = []byte("cas")
	_IncrBytes          = []byte("incr")
	_DecrBytes          = []byte("decr")
	_DeleteBytes        = []byte("delete")
	_TouchBytes         = []byte("touch")
	_MgBytes            = []byte("mg")
	_MsBytes            = []byte("ms")
	_MdBytes            = []byte("md")
	_MaBytes            = []byte("ma")
	_MnBytes            = []byte("mn")
	_MeBytes            = []byte("me")
	_FlushAllBytes      = []byte("flush_all")
	_WatchBytes         = []byte("watch")
	_LRUCrawlerBytes    = []byte("lru_crawler")
	_VerbosityBytes     = []byte("verbosity")
	_ShutdownBytes      = []byte("shutdown")
	_CacheMemLimitBytes = []byte("cache_memlimit")
	_RefreshCertsBytes  = []byte("refresh_certs")
	_SlabsBytes         = []byte("slabs")
	_LRUBytes           = []byte("lru")
)

var _verbBytes = map[string][]byte{
	"get":            _GetBytes,
	"gets":           _GetsBytes,
	"gat":            _GatBytes,
	"gats":           _GatsBytes,
	"set":            _SetBytes,
	"add":            _AddBytes,
	"replace":        _ReplaceBytes,
	"append":         _AppendBytes,
	"prepend":        _PrependBytes,
	"cas":            _CasBytes,
	"incr":           _IncrBytes,
	"decr":           _DecrBytes,
	"delete":         _DeleteBytes,
	"touch":          _TouchBytes,
	"mg":             _MgBytes,
	"ms":             _MsBytes,
	"md":             _MdBytes,
	"ma":             _MaBytes,
	"mn":             _MnBytes,
	"me":             _MeBytes,
	"version":        _VersionCmdBytes,
	"stats":          _StatsCmdBytes,
	"flush_all":      _FlushAllBytes,
	"watch":          _WatchBytes,
	"lru_crawler":    _LRUCrawlerBytes,
	"verbosity":      _VerbosityBytes,
	"shutdown":       _ShutdownBytes,
	"cache_memlimit": _CacheMemLimitBytes,
	"refresh_certs":  _RefreshCertsBytes,
	"slabs":          _SlabsBytes,
	"lru":            _LRUBytes,
}

// verbBytes returns a interned command verb, or a copy for unknown verbs.
func verbBytes(command string) []byte {
	if b, ok := _verbBytes[command]; ok {
		return b
	}
	return []byte(command)
}

// forecastCommonFaultLine maps a well-known status line to a sentinel, or nil.
func forecastCommonFaultLine(line []byte) error {
	t := trimCRLF(line)
	if isKnownNominalLine(t) {
		return nil
	}
	switch {
	case bytes.Equal(t, _NotFoundBytes):
		return ErrNotFound
	case bytes.Equal(t, _ExistsBytes):
		return ErrExists
	case bytes.Equal(t, _NotStoredBytes):
		return ErrNotStored
	}
	hit := mapWireLine(line, _wireApp)
	if hit.Class != _wireFault || errors.Is(hit.Err, ErrMalformedResponse) {
		return nil
	}
	return hit.Err
}

// isKnownNominalLine is true for success and data headers that must not be
// stringified just to decide they are not faults.
func isKnownNominalLine(t []byte) bool {
	switch {
	case bytes.Equal(t, _StoredBytes),
		bytes.Equal(t, _DeletedBytes),
		bytes.Equal(t, _TouchedBytes),
		bytes.Equal(t, _OKBytes),
		bytes.Equal(t, _EndBytes),
		bytes.Equal(t, _MNBytes),
		bytes.Equal(t, _HDBytes),
		bytes.Equal(t, _RESETBytes):
		return true
	case bytes.HasPrefix(t, _ValueBytes),
		bytes.HasPrefix(t, _VASpaceBytes),
		bytes.HasPrefix(t, _HDSpaceBytes),
		bytes.HasPrefix(t, _MESpaceBytes),
		bytes.Equal(t, _MEBytes),
		bytes.HasPrefix(t, _VersionBytes),
		bytes.HasPrefix(t, _STATBytes),
		isMetaStatusLine(t),
		isArithmeticLine(t):
		return true
	default:
		return false
	}
}

// isMetaStatusLine reports a two-letter meta CD, optionally followed by flags.
func isMetaStatusLine(t []byte) bool {
	if len(t) < 2 || (len(t) > 2 && t[2] != ' ') {
		return false
	}
	a, b := t[0], t[1]
	return (a == 'H' && b == 'D') ||
		(a == 'V' && b == 'A') ||
		(a == 'E' && b == 'N') ||
		(a == 'N' && b == 'F') ||
		(a == 'N' && b == 'S') ||
		(a == 'E' && b == 'X') ||
		(a == 'M' && b == 'N') ||
		(a == 'M' && b == 'E')
}

// isArithmeticLine reports an incr/decr decimal reply (spaces allowed).
func isArithmeticLine(t []byte) bool {
	seen := false
	for _, c := range t {
		if c == ' ' || c == '\t' {
			continue
		}
		if c < '0' || c > '9' {
			return false
		}
		seen = true
	}
	return seen
}

// prefixedProtocolError maps a prefix-matched status line onto sentinel.
func prefixedProtocolError(line, prefix []byte, sentinel error) error {
	trimmed := trimCRLF(line)
	if len(trimmed) == 0 || !bytes.HasPrefix(trimmed, prefix) {
		return nil
	}
	rest := trimmed[len(prefix):]
	if len(rest) > 0 && rest[0] != ' ' && rest[0] != ':' {
		return nil
	}
	msg := bytes.TrimSpace(bytes.TrimLeft(rest, " :"))
	const maxServerErrorTextBytes = 4 << 10
	if len(msg) > maxServerErrorTextBytes {
		msg = msg[:maxServerErrorTextBytes]
	}
	return fmt.Errorf("%s: %w", msg, sentinel)
}
