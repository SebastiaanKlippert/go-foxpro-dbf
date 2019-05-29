package dbf

import (
	"bytes"
	"errors"
	"io/ioutil"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
)

var ErrInvalidUTF8 = errors.New("invalid UTF-8 data")

// The charset decoding is all done in this file so you could use an different decoder

// Decoder is the interface as passed to OpenFile
type Decoder interface {
	Decode(in []byte) ([]byte, error)
}

// Win1250Decoder translates a Windows-1250 DBF to UTF8
type Win1250Decoder struct{}

// Decode decodes a Windows1250 byte slice to a UTF8 byte slice
func (d *Win1250Decoder) Decode(in []byte) ([]byte, error) {
	if utf8.Valid(in) {
		return in, nil
	}
	r := transform.NewReader(bytes.NewReader(in), charmap.Windows1250.NewDecoder())
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// UTF8Decoder assumes your DBF is in UTF8 so it does nothing
type UTF8Decoder struct{}

// Decode decodes a UTF8 byte slice to a UTF8 byte slice
func (d *UTF8Decoder) Decode(in []byte) ([]byte, error) {
	return in, nil
}

// UTF8Validator checks if valid UTF8 is read
type UTF8Validator struct{}

// Decode decodes a UTF8 byte slice to a UTF8 byte slice
func (d *UTF8Validator) Decode(in []byte) ([]byte, error) {
	if utf8.Valid(in) {
		return in, nil
	}
	return nil, ErrInvalidUTF8
}
