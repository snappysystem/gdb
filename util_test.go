package gdb

import (
	"bytes"
	"testing"
)

func TestEncodeAndDecodeUint32(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint32{2345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeUint32(scratch, val)
	}

	for _, val := range data {
		var res uint32
		oldSize := len(scratch)
		res, scratch = DecodeUint32(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode to a wrong value ", res)
		}
	}
}

func TestEncodeAndDecodeUint64(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint64{876501232345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeUint64(scratch, val)
	}

	for _, val := range data {
		var res uint64
		oldSize := len(scratch)
		res, scratch = DecodeUint64(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode to a wrong value ", res)
		}
	}
}

func TestEncodeAndDecodeSlice(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...][]byte{
		[]byte("hello, world"),
		[]byte("this is go programming"),
		[]byte("gdb"),
	}

	for _, val := range data {
		scratch = EncodeSlice(scratch, val)
	}

	for _, val := range data {
		var res []byte
		oldSize := len(scratch)

		res, scratch = DecodeSlice(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if bytes.Compare(res, val) != 0 {
			t.Error("decode to a wrong value ", string(res))
		}
	}
}

func TestEncodeAndDecodeVarInt(t *testing.T) {
	scratch := make([]byte, 0)
	data := [...]uint64{876501232345, 34, 123456, 345}

	for _, val := range data {
		scratch = EncodeVarInt(scratch, val)
	}

	for _, val := range data {
		var res uint64
		oldSize := len(scratch)
		res, scratch = DecodeVarInt(scratch)

		if oldSize == len(scratch) {
			t.Error("Fails to decode")
		}

		if res != val {
			t.Error("decode a wrong value ", res, " expected ", val)
		}
	}
}

func TestEncodeDecodeMultiObjects(t *testing.T) {
	scratch := make([]byte, 0)

	scratch = EncodeUint32(scratch, uint32(45))
	scratch = EncodeUint64(scratch, uint64(123400004321))
	scratch = EncodeSlice(scratch, []byte("hello, world"))
	scratch = EncodeUint32(scratch, uint32(12))

	{
		var val uint32
		val, scratch = DecodeUint32(scratch)

		if val != uint32(45) {
			t.Error("fails to decode value")
		}
	}

	{
		var val uint64
		val, scratch = DecodeUint64(scratch)

		if val != uint64(123400004321) {
			t.Error("fails to decode value")
		}
	}

	{
		var val []byte
		val, scratch = DecodeSlice(scratch)

		if bytes.Compare(val, []byte("hello, world")) != 0 {
			t.Error("fails to decode value")
		}
	}

	{
		var val uint32
		val, scratch = DecodeUint32(scratch)

		if val != uint32(12) {
			t.Error("fails to decode value")
		}
	}
}
