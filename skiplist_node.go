package gdb

import (
	"sync/atomic"
	"unsafe"
)

// general interface for a skiplistNode in skip list
type skiplistNode interface {
	getKey() []byte
	getNext() skiplistNode
	getChild() skiplistNode
	setKey(key []byte)
	setNext(next skiplistNode)
	setChild(child skiplistNode)
}

// The real skiplistNode in a skip list
type skiplistLeafNode struct {
	key   []byte
	value []byte
	next  *skiplistLeafNode
}

func (a *skiplistLeafNode) getKey() []byte {
	return a.key
}

func (a *skiplistLeafNode) getNext() skiplistNode {
	if a.next != nil {
		return a.next
	} else {
		return nil
	}
}

func (a *skiplistLeafNode) getChild() skiplistNode {
	return nil
}

func (a *skiplistLeafNode) setKey(key []byte) {
	a.key = key
}

func (a *skiplistLeafNode) setNext(next skiplistNode) {
	var val *skiplistLeafNode
	if next != nil {
		val = next.(*skiplistLeafNode)
	}
	dst := (*unsafe.Pointer)(unsafe.Pointer(&a.next))
	atomic.StorePointer(dst, unsafe.Pointer(val))
}

func (a *skiplistLeafNode) setChild(child skiplistNode) {
	panic("should not set child on leaf node")
}

// additional links in a skip list skiplistNode is
// represented by a skiplistPointerNode
type skiplistPointerNode struct {
	key   []byte
	next  *skiplistPointerNode
	child skiplistNode
}

func (a *skiplistPointerNode) getKey() []byte {
	return a.key
}

func (a *skiplistPointerNode) getNext() skiplistNode {
	if a.next != nil {
		return a.next
	} else {
		return nil
	}
}

func (a *skiplistPointerNode) getChild() skiplistNode {
	return a.child
}

func (a *skiplistPointerNode) setKey(key []byte) {
	a.key = key
}

func (a *skiplistPointerNode) setNext(next skiplistNode) {
	var val *skiplistPointerNode
	if next != nil {
		val = next.(*skiplistPointerNode)
	}
	dst := (*unsafe.Pointer)(unsafe.Pointer(&a.next))
	atomic.StorePointer(dst, unsafe.Pointer(val))
}

func (a *skiplistPointerNode) setChild(child skiplistNode) {
	a.child = child
}

type skiplistNodeAllocator struct {
	leafSize    int
	pointerSize int
	pool        *PoolAllocator
}

// init memory pool, must be called after the allocator is created
func makeNodeAllocator(bytesAlloc ...*PoolAllocator) *skiplistNodeAllocator {
	x1 := skiplistLeafNode{}
	x2 := skiplistPointerNode{}
	ret := &skiplistNodeAllocator{}
	ret.leafSize = int(unsafe.Sizeof(x1))
	ret.pointerSize = int(unsafe.Sizeof(x2))
	switch len(bytesAlloc) {
	case 0:
		ret.pool = MakePoolAllocator()
	case 1:
		ret.pool = bytesAlloc[0]
	default:
		panic("Can only take 0 or 1 parameter")
	}
	return ret
}

// allocate a new leaf
func (a *skiplistNodeAllocator) newLeaf() *skiplistLeafNode {
	b := a.pool.Allocate(a.leafSize)
	return (*skiplistLeafNode)(unsafe.Pointer(&b[0]))
}

// allocate a new pointer skiplistNode
func (a *skiplistNodeAllocator) newPointer() *skiplistPointerNode {
	b := a.pool.Allocate(a.pointerSize)
	return (*skiplistPointerNode)(unsafe.Pointer(&b[0]))
}

// deallocate all skiplistNodes
func (a *skiplistNodeAllocator) deallocateAll() {
	a.pool.DeallocateAll()
}
