package gdb

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestFileAppendAndReadBackSequentially(t *testing.T) {
	root := "/tmp/file_test/FileAppenedAndReadBackSequentially"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	s := "something, hello world"

	fileList := []string{root, "test.txt"}
	name := strings.Join(fileList, "/")

	{
		f := MakeLocalWritableFile(name)
		if f == nil {
			t.Error("Fails to create writable file ", name)
		}

		result := f.Append([]byte(s))
		if !result.Ok() {
			t.Error("Fails to append to a file")
		}

		result = f.Close()
		if !result.Ok() {
			t.Error("Fails to close a file")
		}
	}

	{
		f := MakeLocalSequentialFile(name)
		if f == nil {
			t.Error("Fails to open file")
		}

		defer f.Close()

		scratch := make([]byte, len(s))
		res, result := f.Read(scratch)

		if !result.Ok() {
			t.Error("fails to read from file")
		}

		if bytes.Compare([]byte(s), res) != 0 {
			t.Error("Read corrupted data")
		}
	}
}

func TestReadFileSkipSomething(t *testing.T) {
	root := "/tmp/file_test/ReadFileSkipSomething"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// prepare a write buffer
	s := make([]byte, 4096)
	for i := 0; i < len(s); i++ {
		res := int8(i % 10)
		s[i] = uint8('A' + res)
	}

	// prepare file name
	fileList := []string{root, "test.txt"}
	name := strings.Join(fileList, "/")

	{
		f := MakeLocalWritableFile(name)
		if f == nil {
			t.Error("Fails to create writable file ", name)
		}

		result := f.Append(s)
		if !result.Ok() {
			t.Error("Fails to append to a file")
		}

		result = f.Close()
		if !result.Ok() {
			t.Error("Fails to close a file")
		}
	}

	{
		f := MakeLocalSequentialFile(name)
		if f == nil {
			t.Error("Fails to open file")
		}

		defer f.Close()

		f.Skip(58)

		scratch := make([]byte, len(s)-58)
		res, result := f.Read(scratch)

		if !result.Ok() {
			t.Error("fails to read from file")
		}

		if bytes.Compare(s[58:], res) != 0 {
			t.Error("Read corrupted data")
		}
	}
}

func TestWritableFileMultiAppend(t *testing.T) {
	root := "/tmp/file_test/WriteFileMultiAppend"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// prepare a write buffer
	s := make([]byte, 4096)
	for i := 0; i < len(s); i++ {
		res := int8(i % 10)
		s[i] = uint8('A' + res)
	}

	// prepare file name
	fileList := []string{root, "test.txt"}
	name := strings.Join(fileList, "/")

	// Append a few bytes
	{
		f := MakeLocalWritableFile(name)
		if f == nil {
			t.Error("Fails to create writable file ", name)
		}

		result := f.Append(s[:58])
		if !result.Ok() {
			t.Error("Fails to append to a file")
		}

		result = f.Close()
		if !result.Ok() {
			t.Error("Fails to close a file")
		}
	}

	// Append more data
	{
		f := MakeLocalWritableFile(name)
		if f == nil {
			t.Error("Fails to create writable file ", name)
		}

		result := f.Append(s[58:])
		if !result.Ok() {
			t.Error("Fails to append to a file")
		}

		result = f.Close()
		if !result.Ok() {
			t.Error("Fails to close a file")
		}
	}

	{
		f := MakeLocalSequentialFile(name)
		if f == nil {
			t.Error("Fails to open file")
		}

		defer f.Close()

		scratch := make([]byte, len(s))
		res, result := f.Read(scratch)

		if !result.Ok() {
			t.Error("fails to read from file")
		}

		if bytes.Compare(s, res) != 0 {
			t.Error("Read corrupted data")
		}
	}
}

func TestRandomAccessFile(t *testing.T) {
	root := "/tmp/file_test/RandomAccessFile"

	os.RemoveAll(root)
	os.MkdirAll(root, os.ModePerm)

	// prepare a write buffer
	s := make([]byte, 4096)
	for i := 0; i < len(s); i++ {
		res := int8(i % 10)
		s[i] = uint8('A' + res)
	}

	// prepare file name
	fileList := []string{root, "test.txt"}
	name := strings.Join(fileList, "/")

	// create the file
	{
		f := MakeLocalWritableFile(name)
		if f == nil {
			t.Error("Fails to create writable file ", name)
		}

		result := f.Append(s)
		if !result.Ok() {
			t.Error("Fails to append to a file")
		}

		result = f.Close()
		if !result.Ok() {
			t.Error("Fails to close a file")
		}
	}

	// read from random location
	locations := []int64{58, 456, 2011, 3679}

	for _, loc := range locations {
		f := MakeLocalRandomAccessFile(name)
		if f == nil {
			t.Error("Fails to open file")
		}

		scratch := make([]byte, 16)
		res, result := f.Read(loc, scratch)

		if !result.Ok() {
			t.Error("fails to read from file")
		}

		if bytes.Compare(s[loc:loc+16], res) != 0 {
			t.Error("Read corrupted data")
		}

		f.Close()
	}
}
