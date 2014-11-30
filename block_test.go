package gdb

import (
	"bytes"
	"strconv"
	"testing"
)

func TestCreateAndEnumerateBlock(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// test entries are in the block
	order := &BytesSkiplistOrder{}
	iter := block.NewIterator(order)

	iter.SeekToFirst()
	if !iter.Valid() {
		t.Error("Fails to find the first element")
	}

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)

		if !iter.Valid() {
			t.Error("Fails to find the element")
		}

		if bytes.Compare(b, iter.Key()) != 0 {
			t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iterator pass the end")
	}
}

func TestBlockRandomSeek(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// seek to one entry in the block
	order := &BytesSkiplistOrder{}
	iter := block.NewIterator(order)
	iter.Seek([]byte("103"))

	for i := 103; i < 105; i++ {
		if !iter.Valid() {
			t.Error("iter ends prematurely")
		}
		s := strconv.Itoa(i)
		if string(iter.Key()) != s {
			t.Error("Fails to seek to ", s)
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter has extra value")
	}
}

func TestBlockBackwardSeek(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)

	for i := 100; i < 105; i++ {
		s := strconv.Itoa(i)
		b := []byte(s)
		builder.Add(b, b)
	}

	block, ok := builder.Finalize()
	if !ok {
		t.Error("Fails to build block")
	}

	// seek to one entry in the block
	order := &BytesSkiplistOrder{}
	iter := block.NewIterator(order)
	iter.Seek([]byte("103"))

	for i := 103; i >= 100; i-- {
		if !iter.Valid() {
			t.Error("iter ends prematurely")
		}
		s := strconv.Itoa(i)
		if string(iter.Key()) != s {
			t.Error("Fails to seek to ", s)
		}

		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter has extra value")
	}
}

func TestBlockEncodeDecode(t *testing.T) {
	// first build the block
	data := make([]byte, 4096)
	var endOffset uint32

	{
		builder := MakeBlockBuilder(data)

		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		endOffset = uint32(len(block.data))
	}

	{
		block := DecodeBlock(data, endOffset)

		// seek to one entry in the block
		order := &BytesSkiplistOrder{}
		iter := block.NewIterator(order)
		iter.Seek([]byte("103"))

		for i := 103; i >= 100; i-- {
			if !iter.Valid() {
				t.Error("iter ends prematurely")
			}
			s := strconv.Itoa(i)
			if string(iter.Key()) != s {
				t.Error("Fails to seek to ", s)
			}

			iter.Prev()
		}

		if iter.Valid() {
			t.Error("iter has extra value")
		}
	}
}

func TestCreateAndEnumerateMultiBlock(t *testing.T) {
	data := make([]byte, 4096)
	builder := MakeBlockBuilder(data)
	offset := uint32(0)

	// build first block
	{
		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		offset = offset + uint32(len(block.data))
	}

	// build second block
	{
		for i := 200; i < 205; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)
			builder.Add(b, b)
		}

		block, ok := builder.Finalize()
		if !ok {
			t.Error("Fails to build block")
		}

		offset = offset + uint32(len(block.data))
	}

	order := &BytesSkiplistOrder{}

	// test entries are in last block
	{
		block := DecodeBlock(data, offset)
		iter := block.NewIterator(order)

		iter.SeekToFirst()
		if !iter.Valid() {
			t.Error("Fails to find the first element")
		}

		for i := 200; i < 205; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)

			if !iter.Valid() {
				t.Error("Fails to find the element")
			}

			if bytes.Compare(b, iter.Key()) != 0 {
				t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator pass the end")
		}

		offset = offset - uint32(len(block.data))
	}

	// test entries are in first block
	{
		block := DecodeBlock(data, offset)
		iter := block.NewIterator(order)

		iter.SeekToFirst()
		if !iter.Valid() {
			t.Error("Fails to find the first element")
		}

		for i := 100; i < 105; i++ {
			s := strconv.Itoa(i)
			b := []byte(s)

			if !iter.Valid() {
				t.Error("Fails to find the element")
			}

			if bytes.Compare(b, iter.Key()) != 0 {
				t.Error("Mismatch ", string(b), " versus ", string(iter.Key()))
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator pass the end")
		}
	}
}
