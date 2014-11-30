package gdb

import (
	"strings"
	"testing"
)

func TestCreateAndRemoveDir(t *testing.T) {
	dir := "/tmp/env_test/testCreateAndRemoveDir"
	env := NativeEnv{}
	s := env.DeleteDir(dir)
	if !s.Ok() || env.FileExists(dir) {
		t.Error("Fails to delete dir")
	}

	s = env.CreateDir(dir)
	if !s.Ok() || !env.FileExists(dir) {
		t.Error("Fails to create dir")
	}
}

func TestRenameDir(t *testing.T) {
	dir := "/tmp/env_test/testRenameDir"
	env := NativeEnv{}
	target := strings.Join([]string{dir, ".new"}, "")

	env.DeleteDir(dir)
	env.DeleteDir(target)

	s := env.CreateDir(dir)
	if !s.Ok() || !env.FileExists(dir) {
		t.Error("Fails to create dir")
	}

	s = env.RenameFile(dir, target)
	if !s.Ok() || !env.FileExists(target) || env.FileExists(dir) {
		t.Error("Fails to name dir")
	}
}
