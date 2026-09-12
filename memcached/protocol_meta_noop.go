package memcached

// buildMetaNoopCommand encodes mn [P] [L]\r\n.
func buildMetaNoopCommand(f MetaNoop) (*request, *response) {
	b := newProtocolBuilder()
	defer b.release()
	b.AddBytes(_MnBytes)
	b.AddFlagString("P", f.ProxyHint)
	b.AddFlagString("L", f.ProxyPath)
	req := b.AddCRLF().request(_MnBytes, nil)
	resp := buildLimitedLineResponse(1)
	return req, resp
}
