package gdb

// This implements a LRU cache algorithm.
type LRU struct {
	// lru elements that are in use
	active *LruEntry
	// how many elements in the cache
	numElements int
	// how many elements should be in cache, older ones will be purged
	maxElements int
	// make entries addressable by keys
	m map[string]*LruEntry
}

// create a new LRU cache
func NewLRU(maxElements int) *LRU {
	return &LRU{
		maxElements: maxElements,
		m: make(map[string]*LruEntry),
	}
}

// save an entry which can be addressed by "Key" in LRU cache
func (l *LRU) Put(key []byte, value interface{}) {
	var entry *LruEntry
	if l.numElements < l.maxElements {
		entry = &LruEntry{
			key: key,
			value: value,
		}
	} else {
		// reuse last entry
		entry = l.active.prev
		l.Del(entry.key)
		entry.key = key
		entry.value = value
	}

	// Even if we reuse an entry, a call to Del() above decreases the total
	// counter. So we have to increase the counter for both case here
	l.numElements++

	// setup map
	l.m[string(key)] = entry

	l.pushFront(entry)
}

// retrieve a cache entry, if it is not in cache yet, hit will be false
func (l *LRU) Get(key []byte) (value interface{}, hit bool) {
	var entry *LruEntry
	entry, hit = l.m[string(key)]
	if hit {
		value = entry.value
		l.Del(key)
		l.numElements++
		l.m[string(key)] = entry
		l.pushFront(entry)
	}
	return
}

// remove an entry indexed by "key"
func (l *LRU) Del(key []byte) {
	strKey := string(key)
	entry, found := l.m[strKey]
	if !found {
		return
	}

	if l.active == entry {
		if entry.next != entry {
			l.active = entry.next
		} else {
			l.active = nil
		}
	}

	if entry.next != entry {
		entry.next.prev = entry.prev
		entry.prev.next = entry.next
	}
	delete(l.m, strKey)
	l.numElements--
}

// insert an entry at the beginning
func (l *LRU) pushFront(entry *LruEntry) {
	if l.active != nil {
		entry.next = l.active
		entry.prev = l.active.prev

		l.active.prev = entry
		entry.prev.next = entry
	} else {
		entry.next = entry
		entry.prev = entry
	}

	l.active = entry
}

// define the element of a double linked list
type LruEntry struct {
	prev, next *LruEntry
	key []byte
	value interface{}
}
