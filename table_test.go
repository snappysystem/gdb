package gdb

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestBuildTableAndIterate(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndIterate"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// create a table builder
	fname := strings.Join([]string{root, "sstfile"}, "/")
	f := MakeLocalWritableFile(fname)
	if f == nil {
		t.Error("Fails to create a new file")
	}

	data1 := make([]byte, 64*1024)
	data2 := make([]byte, 4096)

	b := MakeTableBuilder(data1, data2, f)

	// build a table
	for i := 10000; i < 10256; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		b.Add(key, key)
	}

	order := &BytesSkiplistOrder{}
	res := b.Finalize(order)

	if res == nil {
		t.Error("Fails to get a table object")
	}

	// verify that data is correct
	iter := res.NewIterator()
	if iter == nil {
		t.Error("fails to get an iterator")
	}

	iter.SeekToFirst()

	for i := 10000; i < 10256; i++ {
		if !iter.Valid() {
			t.Error("Premature at the end")
		}

		key := string(iter.Key())
		val, err := strconv.Atoi(key)
		if err != nil {
			t.Error("fails convert string to integer")
		}
		if val != i {
			t.Error("key mismatch ", val, " expect ", i)
		}

		iter.Next()
	}

	if iter.Valid() {
		t.Error("iterator passes the end")
	}
}

func TestBuildTableAndRecover(t *testing.T) {
	root := "/tmp/table_test/testBuildTableAndRecover"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	fname := strings.Join([]string{root, "sstfile"}, "/")

	{
		// create a table builder
		f := MakeLocalWritableFile(fname)
		if f == nil {
			t.Error("Fails to create a new file")
		}

		data1 := make([]byte, 64*1024)
		data2 := make([]byte, 4096)

		b := MakeTableBuilder(data1, data2, f)

		// build a table
		for i := 10000; i < 10256; i++ {
			key := []byte(fmt.Sprintf("%d", i))
			b.Add(key, key)
		}

		order := &BytesSkiplistOrder{}
		res := b.Finalize(order)

		if res == nil {
			t.Error("Fails to get a table object")
		}
	}

	// verify that data is correct
	{
		f := MakeLocalSequentialFile(fname)
		if f == nil {
			t.Error("Fails to open table file for read")
		}

		defer f.Close()

		// get file size
		var fsize int64
		{
			fobj, err := os.Open(fname)
			if err != nil {
				t.Error("Fails to open a file")
			}

			fi, e2 := fobj.Stat()
			if e2 != nil {
				t.Error("Fails to stat a file")
			}

			fsize = fi.Size()
			fobj.Close()
		}

		buf := make([]byte, fsize)
		order := &BytesSkiplistOrder{}

		table := RecoverTable(f, buf, order)
		if table == nil {
			t.Error("Fails to recover from a table file")
		}

		iter := table.NewIterator()
		if iter == nil {
			t.Error("fails to get an iterator")
		}

		iter.SeekToFirst()

		for i := 10000; i < 10256; i++ {
			if !iter.Valid() {
				t.Error("Premature at the end")
			}

			key := string(iter.Key())
			val, err := strconv.Atoi(key)
			if err != nil {
				t.Error("fails convert string to integer")
			}
			if val != i {
				t.Error("key mismatch ", val, " expect ", i)
			}

			iter.Next()
		}

		if iter.Valid() {
			t.Error("iterator passes the end")
		}
	}
}
