package utils

import (
	"io"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/transform"
)

// MaybeDecodeShiftJIS checks if the string is valid UTF-8 and decodes it from Shift-JIS if not
func MaybeDecodeShiftJIS(s string) string {
	if utf8.ValidString(s) {
		return s // already valid UTF-8
	}
	decoded, err := DecodeShiftJIS(s)
	if err != nil {
		return s // fallback to original string if decoding fails
	}
	return decoded
}

// DecodeShiftJIS decodes a Shift-JIS encoded string to UTF-8
func DecodeShiftJIS(input string) (string, error) {
	reader := transform.NewReader(strings.NewReader(input), japanese.ShiftJIS.NewDecoder())
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(decoded), nil
} 