package dbf

import (
	"bytes"
	"testing"
)

func TestUTF8Decoder_Decode(t *testing.T) {
	dec := new(UTF8Decoder)
	in := []byte("Tésting ㇹ Д")
	b, err := dec.Decode(in)
	if err != nil {
		t.Fatalf("error in decode: %s", err)
	}
	if bytes.Equal(in, b) == false {
		t.Errorf("Want %s, have %s", string(in), string(b))
	}
}

func TestWin1250Decoder_Decode(t *testing.T) {
	dec := new(Win1250Decoder)
	in := []byte{0xC4, 0xF5}
	b, err := dec.Decode(in)
	if err != nil {
		t.Fatalf("error in decode: %s", err)
	}
	want := "Äő"
	if string(b) != want {
		t.Errorf("Want %s, have %s", want, string(b))
	}
}

func TestUTF8UTF8Validator_Decode(t *testing.T) {
	dec := new(UTF8Validator)

	// Test valid UTF-8 data
	in := []byte("Tésting ㇹ Д")
	b, err := dec.Decode(in)
	if err != nil {
		t.Fatalf("error in decode: %s", err)
	}
	if bytes.Equal(in, b) == false {
		t.Errorf("Want %s, have %s", string(in), string(b))
	}

	// Test invalid UTF-8
	in = []byte{0xff}
	_, err = dec.Decode(in)
	if err == nil {
		t.Fatalf("wanted an error in Decode, but have no error")
	}
	if err != ErrInvalidUTF8 {
		t.Fatalf("wanted error %s, have %s", ErrInvalidUTF8, err)
	}



}