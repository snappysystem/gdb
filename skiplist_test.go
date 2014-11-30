package gdb

import (
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

func TestSkiplistPutGetSome(t *testing.T) {
	slist := MakeSkiplist()
	data := [...]string{"hello", "world", "go", "language"}

	for _, s := range data {
		arr := []byte(s)
		_, ok := slist.Put(arr, arr)
		if !ok {
			t.Error("Fails to put ", s)
		}
	}

	for _, s := range data {
		arr := []byte(s)
		val, ok := slist.Get(arr)
		if !ok || bytes.Compare(arr, val) != 0 {
			t.Error("Fails to find key ", s)
		}
	}
}

func genRandomBytes() []byte {
	size := 2 + rand.Intn(16)
	ret := make([]byte, size)
	for i := 0; i < size; i++ {
		ret[i] = byte(rand.Intn(25)) + 'a'
	}

	return ret
}

func TestSkiplistPutGetMore(t *testing.T) {
	const numElements = 5000
	data := make([][]byte, 0, numElements)
	slist := MakeSkiplist()

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	for i, k := range data {
		val, ok := slist.Get(k)
		if !ok || bytes.Compare(val, k) != 0 {
			t.Error("Fails to find key ", i, " ", string(k))
		}
	}
}

func TestSkiplistPutPerf(t *testing.T) {
	const numElements = 2000

	data := make(map[string][]byte)
	slist := MakeSkiplist()

	mapTime := int64(0)
	skiplistTime := int64(0)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		str := string(key)

		{
			t1 := time.Now()
			data[str] = key
			t2 := time.Now()
			delta := t2.Sub(t1).Nanoseconds()
			mapTime = mapTime + delta
		}

		{
			t1 := time.Now()
			slist.Put(key, key)
			t2 := time.Now()
			delta := t2.Sub(t1).Nanoseconds()
			skiplistTime = skiplistTime + delta
		}
	}

	fmt.Println("map uses ", mapTime/numElements, " nanoseconds per op")
	fmt.Println("ski uses ", skiplistTime/numElements, " nanoseconds per op")
}

func TestSkiplistScanForwardSome(t *testing.T) {
	data := [...]string{"go", "hello", "world", "yellow"}
	slist := MakeSkiplist()

	for _, s := range data {
		bs := []byte(s)
		slist.Put(bs, bs)
	}

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToFirst()

	for _, s := range data {
		if !iter.Valid() {
			t.Error("Not valid at ", s)
		}
		if bytes.Compare(iter.Key(), []byte(s)) != 0 {
			t.Error("Got string ", string(iter.Key()))
		}
		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

func TestSkiplistScanBackwardSome(t *testing.T) {
	data := [...]string{"yellow", "world", "hello", "go"}
	slist := MakeSkiplist()

	for _, s := range data {
		bs := []byte(s)
		slist.Put(bs, bs)
	}

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToLast()

	for _, s := range data {
		if !iter.Valid() {
			t.Error("Not valid at ", s)
		}
		if bytes.Compare(iter.Key(), []byte(s)) != 0 {
			t.Error("Got string ", string(iter.Key()))
		}
		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

// struct to sort a slice of byte slices
type ByteSliceSorter struct {
	bytesList [][]byte
}

func MakeSortInterface(x [][]byte) sort.Interface {
	return &ByteSliceSorter{x}
}

func (a *ByteSliceSorter) Len() int {
	return len(a.bytesList)
}

func (a *ByteSliceSorter) Less(i, j int) bool {
	return bytes.Compare(a.bytesList[i], a.bytesList[j]) < 0
}

func (a *ByteSliceSorter) Swap(i, j int) {
	tmp := a.bytesList[i]
	a.bytesList[i] = a.bytesList[j]
	a.bytesList[j] = tmp
}

func TestSkiplistScanForwardMore(t *testing.T) {
	const numElements = 1000

	slist := MakeSkiplist()
	data := make([][]byte, 0, numElements)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	sort.Sort(MakeSortInterface(data))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToFirst()

	prev := make([]byte, 0)
	for _, bs := range data {
		if bytes.Compare(prev, bs) == 0 {
			continue
		}

		if !iter.Valid() {
			t.Error("Premature end of iteration")
		}

		if bytes.Compare(iter.Key(), bs) != 0 {
			t.Error("Fails to compare ", string(iter.Key()), " ", string(bs))
		}

		prev = bs
		iter.Next()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}

func TestSkiplistScanBackwardMore(t *testing.T) {
	const numElements = 1000

	slist := MakeSkiplist()
	data := make([][]byte, 0, numElements)

	for i := 0; i < numElements; i++ {
		key := genRandomBytes()
		data = append(data, key)
		slist.Put(key, key)
	}

	sort.Sort(MakeSortInterface(data))

	ro := &ReadOptions{}
	iter := slist.NewIterator(ro)
	iter.SeekToLast()

	prev := make([]byte, 0)
	for i := len(data) - 1; i >= 0; i-- {
		bs := data[i]
		if bytes.Compare(prev, bs) == 0 {
			continue
		}

		if !iter.Valid() {
			t.Error("Premature end of iteration")
		}

		if bytes.Compare(iter.Key(), bs) != 0 {
			t.Error("Fails to compare ", string(iter.Key()), " ", string(bs))
		}

		prev = bs
		iter.Prev()
	}

	if iter.Valid() {
		t.Error("iter should not be valid at this time")
	}
}
