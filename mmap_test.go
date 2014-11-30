package gdb

import "testing"

func TestMmapAlloc(t *testing.T) {
	bytes, err := MmapAlloc(4096)
	if bytes == nil || err != nil {
		t.Error("Fails to allocate memory")
	}

	err = MmapDealloc(bytes)
	if err != nil {
		t.Error("Fails to deallocate ram!")
	}
}

func TestBigAlloc(t *testing.T) {
	bytes, err := MmapAlloc(1024 * 1024 * 512)
	if bytes == nil || err != nil {
		t.Error("Fails to allocate memory")
	}

	err = MmapDealloc(bytes)
	if err != nil {
		t.Error("Fails to deallocate ram!")
	}
}

func TestMultiAlloc(t *testing.T) {
	ss := [...]int{4096, 1024 * 1024, 128 * 1024}
	res := make([][]byte, 0, 4)

	for _, size := range ss {
		buf, err := MmapAlloc(size)
		if buf == nil || err != nil {
			t.Error("Fails to allocate memory")
		}
		res = append(res, buf)
	}

	for _, buf := range res {
		err := MmapDealloc(buf)
		if err != nil {
			t.Error("Fails to deallocate memory")
		}
	}
}

func TestSinglePoolAlloc(t *testing.T) {
	mp := MakePoolAllocator()
	x := mp.Allocate(512)
	if x == nil {
		t.Error("Fails to allocate block")
	}
}

func TestManyPoolAllocDealloc(t *testing.T) {
	mp := MakePoolAllocator()

	for i := 0; i < 2048; i++ {
		x := mp.Allocate(16 * 1024 * 1024)
		if x == nil {
			t.Error("Fails to allocate block")
		}
	}

	mp.DeallocateAll()
}
