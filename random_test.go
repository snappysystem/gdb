package gdb

import "testing"

func TestRandomGeneratorReturn0s(t *testing.T) {
	gen := makeRandomGenerator()
	count := 0

	for i := 0; i < 32; i++ {
		res := gen.get()
		if res == 0 {
			count++
		}
	}

	if count < 24 {
		t.Error("Too little counts ", count)
	}
}
