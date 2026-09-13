package memcached

func buildMetaNoopCommand() (*request, *response) {
	req := buildRequest([]byte("mn"), nil, []byte("mn\r\n"))
	resp := buildLimitedLineResponse(1)
	return req, resp
}
