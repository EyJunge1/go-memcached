package memcached

// buildMetaNoopCommand encodes mn [P] [L]\r\n.
func buildMetaNoopCommand(f MetaNoop) (*request, *response) {
	b := newProtocolBuilder().AddString("mn")
	defer b.release()
	b.AddFlagString("P", f.P)
	b.AddFlagString("L", f.L)
	req := buildRequest([]byte("mn"), nil, b.AddCRLF().build())
	resp := buildLimitedLineResponse(1)
	return req, resp
}
