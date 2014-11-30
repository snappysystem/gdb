package gdb

import (
	"bytes"
)

type BytesSkiplistOrder struct {
}

func (x BytesSkiplistOrder) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

type Skiplist struct {
	levels    []skiplistNode
	allocator *skiplistNodeAllocator
	gen       *randomGenerator
	order     Comparator
	numNodes  int
}

// Create a new skiplist. It can take up to 2 parameters:
// First optional parameter (Comparator): the customized comparator
// Second optional parameter (Allocator): a customized byte allocator
func MakeSkiplist(args ...interface{}) *Skiplist {
	ret := Skiplist{}

	switch len(args) {
	case 0:
		ret.order = &BytesSkiplistOrder{}
		ret.allocator = makeNodeAllocator()
	case 1:
		ret.order = args[0].(Comparator)
		ret.allocator = makeNodeAllocator()
	case 2:
		ret.order = args[0].(Comparator)
		ret.allocator = makeNodeAllocator(args[1].(*PoolAllocator))
	default:
		panic("args is either 0 or 1")
	}

	ret.levels = make([]skiplistNode, maxLevel+1)
	ret.gen = makeRandomGenerator()
	ret.numNodes = 0
	return &ret
}

// insert a key value pair into skip list. If the key is already
// in the list, the entry will not be updated. The orginal value
// of the key will be returned
func (a *Skiplist) Put(key []byte, val []byte) (old []byte, ok bool) {
	prevList, found := a.trace(key)
	if found {
		leaf := prevList[0].(*skiplistLeafNode)
		ok, old = false, leaf.value
		return
	}

	height := a.gen.get() + 1
	var child skiplistNode

	for i := 0; i < height; i++ {
		var newNode skiplistNode
		if i == 0 {
			newLeaf := a.allocator.newLeaf()
			newLeaf.value = val
			newNode = newLeaf
		} else {
			newPointer := a.allocator.newPointer()
			newNode = newPointer
		}

		newNode.setKey(key)

		if prevList[i] != nil {
			newNode.setNext(prevList[i].getNext())
			prevList[i].setNext(newNode)
		} else {
			newNode.setNext(a.levels[i])
			a.levels[i] = newNode
		}

		if child != nil {
			newNode.setChild(child)
		}

		child = newNode
	}

	ok = true
	return
}

// Look up a key in the skiplist. Return the corresponding value and true
// if the key is in the skiplist. Otherwise return an empty slice and
// false
func (a *Skiplist) Get(key []byte) (value []byte, ok bool) {
	prevList, ok := a.trace(key)
	if ok {
		leaf := prevList[0].(*skiplistLeafNode)
		value, ok = leaf.value, true
	} else {
		ok = false
	}
	return
}

func (a *Skiplist) NewIterator(opt *ReadOptions) Iterator {
	return makeSkiplistIter(a)
}

// Find out nodes in all levels that point a key either before @key or
// exactly point to @key. Return true if @key is in the skip list,
// otherwise false
func (a *Skiplist) trace(key []byte) (ret []skiplistNode, found bool) {
	numLevels := len(a.levels)
	ret = make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch a.order.Compare(cur.getKey(), key) {
		case -1:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		case 0:
			found = true
			ret[i] = cur
			i--
			cur = cur.getChild()
			prev = nil
		case 1:
			ret[i] = prev
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		default:
			panic("Invaid comparison value")
		}
	}

	return
}

func (a *Skiplist) traceBackward(key []byte) []skiplistNode {
	numLevels := len(a.levels)
	ret := make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch a.order.Compare(cur.getKey(), key) {
		case -1:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		case 0:
			fallthrough
		case 1:
			ret[i] = prev
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		default:
			panic("Invaid comparison value")
		}
	}

	return ret
}

func (a *Skiplist) locateLast() skiplistNode {
	numLevels := len(a.levels)
	ret := make([]skiplistNode, numLevels)
	var prev skiplistNode

	for cur, i := a.levels[numLevels-1], numLevels-1; i >= 0; {
		if cur == nil {
			i--
			if i >= 0 {
				cur = a.levels[i]
			} else {
				break
			}
			continue
		}

		switch {
		case cur.getNext() != nil:
			prev = cur
			cur = cur.getNext()
			if cur == nil {
				ret[i] = prev
				i--
				cur = prev.getChild()
				prev = nil
			}
		default:
			ret[i] = cur
			i--
			if prev != nil {
				cur = prev.getChild()
				prev = nil
			} else if i >= 0 {
				cur = a.levels[i]
			}
		}
	}

	return ret[0]
}

// Iterator class for skiplist
type skiplistIter struct {
	slist *Skiplist
	cur   skiplistNode
}

func makeSkiplistIter(s *Skiplist) *skiplistIter {
	ret := &skiplistIter{}
	ret.slist = s
	return ret
}

func (a *skiplistIter) Valid() bool {
	return a.cur != nil
}

func (a *skiplistIter) SeekToFirst() {
	a.cur = a.slist.levels[0]
}

func (a *skiplistIter) SeekToLast() {
	a.cur = a.slist.locateLast()
}

func (a *skiplistIter) Seek(key []byte) {
	traces, match := a.slist.trace(key)
	if match {
		a.cur = traces[0]
	} else if traces[0] != nil {
		a.cur = traces[0].getNext()
	}
}

func (a *skiplistIter) Next() {
	a.cur = a.cur.getNext()
}

func (a *skiplistIter) Prev() {
	key := a.cur.getKey()
	a.cur = a.slist.traceBackward(key)[0]
}

func (a *skiplistIter) Key() []byte {
	return a.cur.getKey()
}

func (a *skiplistIter) Value() []byte {
	leaf := a.cur.(*skiplistLeafNode)
	return leaf.value
}
