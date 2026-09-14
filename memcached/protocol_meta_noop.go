package memcached

// buildMetaNoopCommand encodes mn [P] [L]\r\n.
func buildMetaNoopCommand(f MetaNoop) (*request, *response) {
	b := newProtocolBuilder().AddBytes(_MnBytes)
	defer b.release()
	b.AddFlagString("P", f.P)
	b.AddFlagString("L", f.L)
	req := b.AddCRLF().request(_MnBytes, nil)
	resp := buildLimitedLineResponse(1)
	return req, resp
}
