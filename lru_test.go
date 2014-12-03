package gdb

import (
	"fmt"
	"testing"
)

func TestLruUnbounded(t *testing.T) {
	vals := [...]int{5, 6, 7, 8, 9, 10}
	lru := NewLRU(len(vals) + 1)
	for _,v := range vals {
		s := fmt.Sprintf("%d", v)
		lru.Put([]byte(s), v)
	}

	for _,v := range vals {
		s := fmt.Sprintf("%d", v)
		val, found := lru.Get([]byte(s))
		if !found {
			t.Error("Fails to get an entry!")
		}
		iv := val.(int)
		if iv != v {
			t.Error("Fails to retrive the exact value!")
		}
	}
}

func TestLruEviction(t *testing.T) {
	vals := [...]int{5, 6, 7, 8, 9, 10, 12}
	lru := NewLRU(4)
	for _,v := range vals {
		s := fmt.Sprintf("%d", v)
		lru.Put([]byte(s), v)
	}

	for i := 0; i < len(vals) - 4; i++ {
		v := vals[i]
		s := fmt.Sprintf("%d", v)
		_, found := lru.Get([]byte(s))
		if found {
			t.Error("The entry ", v, " should be evicted")
		}
	}

	for i := len(vals) - 4; i < len(vals); i++ {
		v := vals[i]
		s := fmt.Sprintf("%d", v)
		val, found := lru.Get([]byte(s))
		if !found {
			t.Error("Fails to get an entry!")
		}
		iv := val.(int)
		if iv != v {
			t.Error("Fails to retrive the exact value!")
		}
	}
}

func TestLruRefresh(t *testing.T) {
	// define test behavior
	type Input struct {
		// the value to be accessed
		val int
		// true if add, false if read(access)
		add bool
		// if this is an access, true if the entry should present
		present bool
	}

	actions := [...]Input{
		{5, true, false},
		{6, true, false},
		{7, true, false},
		{5, false, true},
		{8, true, false},
		{5, false, true},
		{6, false, false},
		{7, false, true},
		{9, true, false},
		{7, false, true},
		{8, false, false},
	}

	lru := NewLRU(3)
	for _,v := range actions {
		s := fmt.Sprintf("%d", v.val)
		if v.add {
			lru.Put([]byte(s), v.val)
		} else {
			val,found := lru.Get([]byte(s))
			if v.present {
				if !found {
					t.Error("Fails to find entry ", v.val)
				}
				rval := val.(int)
				if rval != v.val {
					t.Error("The value mismatch")
				}
			} else {
				if found {
					t.Error("The entry should be evicted")
				}
			}
		}
	}
}
