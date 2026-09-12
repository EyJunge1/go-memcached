package memcached

import (
	"fmt"
	"strings"
)

// validWireToken rejects empty tokens and tokens with whitespace or controls.
func validWireToken(tok string) error {
	if tok == "" {
		return fmt.Errorf("extra token empty: %w", ErrInvalidArgument)
	}
	for i := 0; i < len(tok); i++ {
		if tok[i] <= ' ' || tok[i] == 0x7f {
			return fmt.Errorf("extra token invalid: %w", ErrInvalidArgument)
		}
	}
	return nil
}

// validMetaExtras rejects empty/control tokens and framing flags b/q/O.
// Framing must stay on BinaryKey, Quiet, and Opaque.
func validMetaExtras(extra []string) error {
	for _, tok := range extra {
		if err := validWireToken(tok); err != nil {
			return err
		}
		switch tok[0] {
		case 'b', 'q', 'O':
			return fmt.Errorf("extra framing flag %q: %w", tok, ErrInvalidArgument)
		}
	}
	return nil
}

// validTextExtras rejects empty/control tokens and the noreply keyword.
// Quiet must stay typed.
func validTextExtras(extra []string) error {
	for _, tok := range extra {
		if err := validWireToken(tok); err != nil {
			return err
		}
		if strings.EqualFold(tok, "noreply") {
			return fmt.Errorf("extra noreply: %w", ErrInvalidArgument)
		}
	}
	return nil
}

// appendExtraTokens writes each Extra token with a trailing space.
// Callers must validate first (validTextExtras / validMetaExtras).
func (b *protocolBuilder) appendExtraTokens(extra []string) {
	for _, tok := range extra {
		b.AddString(tok)
	}
}

// appendTextTrail validates Extra, writes tokens, then optional noreply.
func (b *protocolBuilder) appendTextTrail(extra []string, noReply bool) error {
	if err := validTextExtras(extra); err != nil {
		return err
	}
	b.appendExtraTokens(extra)
	if noReply {
		b.AddBytes(_NoReplyBytes)
	}
	return nil
}
