package gdb

import "testing"

func TestAllocateNode(t *testing.T) {
	alloc := makeNodeAllocator()
	defer alloc.deallocateAll()

	for i := 0; i < 1000; i++ {
		l := alloc.newLeaf()
		if l == nil {
			t.Error("fails to get a new leaf skiplistNode!")
		}
	}
}

func TestTraverseLeaf(t *testing.T) {
	alloc := makeNodeAllocator()
	defer alloc.deallocateAll()

	a := alloc.newLeaf()
	b := alloc.newLeaf()
	c := alloc.newLeaf()

	a.next = b
	b.next = c

	var n skiplistNode
	n = a
	y := n.getNext().getNext()

	if c != y.(*skiplistLeafNode) {
		t.Error("Fails to traverse as a skiplistNode")
	}
}
