package memcached

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
)

// appendDataBlock appends a header and its length-framed body, if any.
func (resp *response) appendDataBlock(rr protocolReader, header []byte) (consumed int, err error) {
	size, has, err := protocolDataBlockSize(header)
	if err != nil {
		return 0, fmt.Errorf("data block size: %w", err)
	}
	if !has {
		resp.rawLines = append(resp.rawLines, header)
		return 1, nil
	}
	n, err := cappedValueSize(size, resp.maxValueBytes)
	if err != nil {
		return 0, err
	}
	if n > math.MaxInt-2 {
		return 0, fmt.Errorf("data block too large: %w", ErrMalformedResponse)
	}
	payload, err := rr.readExact(n + 2)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return 0, fmt.Errorf("short data block: %w", ErrMalformedResponse)
		}
		return 0, fmt.Errorf("read data block: %w", err)
	}
	if !bytes.HasSuffix(payload, _CRLFBytes) {
		return 0, fmt.Errorf("data block missing CRLF: %w", ErrMalformedResponse)
	}
	if uint64(len(payload)) != size+2 {
		return 0, fmt.Errorf("data block length mismatch: %w", ErrMalformedResponse)
	}
	resp.rawLines = append(resp.rawLines, header)
	resp.rawLines = append(resp.rawLines, payload)
	refreshReadDeadlineIfIdle(resp, rr)
	return 2, nil
}

// read1 reads limitedLines protocol records. VALUE/VA bodies are length-framed
// and count as the next record.
func (resp *response) read1(rr protocolReader) error {
	sr, sliceOK := rr.(sliceReader)
	if resp.metaItem != nil {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}
		if err = forecastCommonFaultLine(line); err != nil {
			return err
		}
		if err = resp.fillMetaItem(rr, line); err != nil {
			return err
		}
		resp.metaFilled = true
		return nil
	}

	read := 0
	for read < int(resp.limitedLines) {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if err = forecastCommonFaultLine(line); err != nil {
			return err
		}
		if sliceOK {
			if _, has, sizeErr := protocolDataBlockSize(line); sizeErr == nil && has {
				line = bytes.Clone(line)
			}
		}

		consumed, err := resp.appendDataBlock(rr, line)
		if err != nil {
			return err
		}
		read += consumed
	}

	return nil
}

const _statsResponseBudget = 4 << 20

// read2 reads until specEndLine. Bodies are length-framed.
// collectFaults keeps EX/NF/NS/CLIENT_ERROR until MN.
func (resp *response) read2(rr protocolReader, collectFaults bool) error {
	sr, sliceOK := rr.(sliceReader)
	metaFilled := false
	var charge error
	for {
		line, err := readProtocolLine(rr, sr, sliceOK)
		if err != nil {
			return fmt.Errorf("dispatchRequest read: %w", err)
		}

		if bytes.Equal(trimCRLF(line), trimCRLF(resp.specEndLine)) {
			if resp.metaItem == nil && resp.metaPipeDst == nil && resp.statsDst == nil {
				if sliceOK {
					line = bytes.Clone(line)
				}
				resp.rawLines = append(resp.rawLines, line)
			}
			break
		}

		if err = forecastCommonFaultLine(line); err != nil {
			if !collectFaults || errors.Is(err, ErrServerError) {
				return err
			}
			if charge == nil {
				charge = err
			}
			if resp.metaItem != nil || resp.metaPipeDst != nil {
				continue
			}
		} else if resp.statsDst != nil {
			if !resp.statsFilled {
				*resp.statsDst = (*resp.statsDst)[:0]
				resp.statsFilled = true
				resp.statsBytes = 0
			}
			resp.statsBytes += len(line)
			if resp.statsBytes > _statsResponseBudget {
				return fmt.Errorf("stats response too large: %w", ErrMalformedResponse)
			}
			if err = consumeStatsLine(resp.statsDst, line, resp.statsDetail); err != nil {
				return err
			}
			continue
		} else if resp.metaPipeDst != nil {
			if !isMetaNoopLine(line) {
				if err = resp.fillMetaPipe(rr, line); err != nil {
					return err
				}
			}
			continue
		} else if resp.metaItem != nil && !metaFilled && !isMetaNoopLine(line) {
			if err = resp.fillMetaItem(rr, line); err != nil {
				return err
			}
			metaFilled = true
			resp.metaFilled = true
			continue
		}

		if resp.metaItem != nil {
			continue
		}
		if sliceOK {
			line = bytes.Clone(line)
		}
		if _, err = resp.appendDataBlock(rr, line); err != nil {
			return err
		}
	}

	return charge
}

func protocolDataBlockSize(line []byte) (size uint64, has bool, err error) {
	trimmed := trimCRLF(line)
	switch {
	case bytes.HasPrefix(trimmed, _ValueSpaceBytes):
		size, err = parseValueDataLen(trimmed)
		return size, true, err
	case bytes.HasPrefix(trimmed, _VASpaceBytes):
		rest := trimmed[3:]
		end := bytes.IndexByte(rest, ' ')
		if end < 0 {
			end = len(rest)
		}
		size, err = parseUintFromBytes(rest[:end])
		if err != nil {
			return 0, true, fmt.Errorf("invalid VA size: %w", ErrMalformedResponse)
		}
		return size, true, nil
	default:
		return 0, false, nil
	}
}

// cappedValueSize converts a protocol body size to int after the configured limit.
func cappedValueSize(size uint64, limit int) (int, error) {
	if limit <= 0 {
		limit = _defaultMaxValueBytes
	}
	if size > uint64(math.MaxInt) || size > uint64(limit) {
		return 0, fmt.Errorf("data block too large: %w", ErrMalformedResponse)
	}
	return int(size), nil
}

func readProtocolLine(rr protocolReader, sr sliceReader, sliceOK bool) ([]byte, error) {
	if sliceOK {
		return sr.readSlice('\n')
	}
	return rr.readLine('\n')
}
