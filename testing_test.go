package gdb

import "testing"

func TestMakeRandomInt(t *testing.T) {
	ret := MakeRandomInt(0, 37, 100)
	if len(ret) != 100 {
		t.Error("Fails to generate required number of output")
	}
	for _, val := range ret {
		if val < 0 || val >= 37 {
			t.Error("Fails to generate correct random number", val)
		}
	}
}

func TestMakeRandomBytesSlice(t *testing.T) {
	ret := MakeRandomSlice(uint8('a'), uint8('z'), 4, 32, 100)
	if len(ret) != 100 {
		t.Error("Fails to generate required number of output")
	}

	for _, val := range ret {
		if len(val) < 4 || len(val) >= 32 {
			t.Error("length of slice is not in the range!")
		}
		for _, b := range val {
			if b < uint8('a') || b >= uint8('z') {
				t.Error("slice ", string(val), " is not qualified")
			}
		}
	}
}
