package delivery

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrDuplicateHeader = errors.New("caller Header names must be unique case-insensitively")
	ErrInvalidHeader   = errors.New("caller Header name or value is invalid")
)

type HashInput struct {
	DestinationID      string
	DestinationVersion int64
	Method             string
	CallerHeaders      map[string]string
	Body               []byte
}

func RequestHash(input HashInput) ([sha256.Size]byte, error) {
	headers, err := CanonicalCallerHeaders(input.CallerHeaders)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)

	digest := sha256.New()
	writeHashField(digest, []byte(input.DestinationID))
	writeHashField(digest, []byte(strconv.FormatInt(input.DestinationVersion, 10)))
	writeHashField(digest, []byte(strings.ToUpper(input.Method)))
	for _, name := range names {
		writeHashField(digest, []byte(name))
		writeHashField(digest, []byte(headers[name]))
	}
	writeHashField(digest, input.Body)

	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func CanonicalCallerHeaders(input map[string]string) (map[string]string, error) {
	headers := make(map[string]string, len(input))
	for rawName, rawValue := range input {
		name := strings.ToLower(strings.TrimSpace(rawName))
		if !validHeaderName(name) || !validHeaderValue(rawValue) {
			return nil, ErrInvalidHeader
		}
		if _, exists := headers[name]; exists {
			return nil, ErrDuplicateHeader
		}
		headers[name] = trimOptionalWhitespace(rawValue)
	}
	return headers, nil
}

func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	const separators = "()<>@,;:\\\"/[]?={} \t"
	for _, character := range name {
		if character < 33 || character > 126 || strings.ContainsRune(separators, character) {
			return false
		}
	}
	return true
}

func validHeaderValue(value string) bool {
	for _, character := range value {
		if character == '\r' || character == '\n' || character == 0x7f ||
			(character < 0x20 && character != '\t') {
			return false
		}
	}
	return true
}

func writeHashField(digest hash.Hash, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = digest.Write(length[:])
	_, _ = digest.Write(value)
}

func trimOptionalWhitespace(value string) string {
	return strings.Trim(value, " \t")
}
