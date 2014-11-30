package gdb

import (
	"unsafe"
)

// encode an integer value to the end of @scratch, returns
// the resulting slice.
func EncodeUint32(scratch []byte, val uint32) []byte {
	c, size := cap(scratch), len(scratch)
	if c < size+4 {
		newCap := c * 2
		if newCap < size+4 {
			newCap = size + 4
		}
		tmp := make([]byte, size, newCap)
		copy(tmp, scratch)
		scratch = tmp
	}

	scratch = scratch[:(size + 4)]
	*(*uint32)(unsafe.Pointer(&scratch[size])) = val
	return scratch
}

// encode an integer value to the end of @scratch, returns
// the resulting slice.
func EncodeUint64(scratch []byte, val uint64) []byte {
	c, size := cap(scratch), len(scratch)
	if c < size+8 {
		newCap := c * 2
		if newCap < size+8 {
			newCap = size + 8
		}
		tmp := make([]byte, size, newCap)
		copy(tmp, scratch)
		scratch = tmp
	}

	scratch = scratch[:(size + 8)]
	*(*uint64)(unsafe.Pointer(&scratch[size])) = val
	return scratch
}

// encode an integer value to a byte array, returns the resulting
// encoded slice
func EncodeVarInt(scratch []byte, val uint64) []byte {
	for true {
		switch {
		case val < 0xf0:
			return append(scratch, uint8(val))

		case val <= 0xffff:
			size := len(scratch)
			if cap(scratch) > size+2 {
				scratch = scratch[:(size + 3)]
				scratch[size] = 0xf1
				*(*uint16)(unsafe.Pointer((&scratch[size+1]))) = uint16(val)
				return scratch
			}
		case val <= 0xffffffff:
			size := len(scratch)
			if cap(scratch) > size+4 {
				scratch = scratch[:(size + 5)]
				scratch[size] = 0xf2
				*(*uint32)(unsafe.Pointer((&scratch[size+1]))) = uint32(val)
				return scratch
			}
		default:
			size := len(scratch)
			if cap(scratch) > size+8 {
				scratch = scratch[:(size + 9)]
				scratch[size] = 0xf3
				*(*uint64)(unsafe.Pointer((&scratch[size+1]))) = uint64(val)
				return scratch
			}
		}

		// increase the slice cap to make room for encoded data
		c := cap(scratch)
		if c == 0 {
			c = 1
		}
		tmp := make([]byte, len(scratch), 2*c)
		copy(tmp, scratch)
		scratch = tmp
	}

	panic("Should not reach here")
	return nil
}

// decode a integer value and return the slice after the bytes
// have been consumed by the decode process
func DecodeVarInt(data []byte) (val uint64, result []byte) {
	size := len(data)
	if size < 1 {
		result = data
		return
	}

	flag := data[0]
	switch {
	case flag < 0xf0:
		val = uint64(flag)
		result = data[1:]
	case flag == 0xf1:
		val = uint64(*(*uint16)(unsafe.Pointer(&data[1])))
		result = data[3:]
	case flag == 0xf2:
		val = uint64(*(*uint32)(unsafe.Pointer(&data[1])))
		result = data[5:]
	case flag == 0xf3:
		val = uint64(*(*uint64)(unsafe.Pointer(&data[1])))
		result = data[9:]
	default:
		panic("Bad flag value")
	}

	return
}

// encode a slice @data into @scratch, and return the resulting slice
func EncodeSlice(scratch []byte, data []byte) []byte {
	dsize := len(data)
	c, size := cap(scratch), len(scratch)
	if c < size+dsize+4 {
		newCap := c * 2
		if newCap < size+dsize+4 {
			newCap = size + dsize + 4
		}
		tmp := make([]byte, size, newCap)
		copy(tmp, scratch)
		scratch = tmp
	}

	scratch = scratch[:(size + dsize + 4)]
	*(*uint32)(unsafe.Pointer(&scratch[size])) = uint32(dsize)
	copy(scratch[size+4:], data)

	return scratch
}

// decode a uint32 value and return the slice after the bytes
// have been consumed by the decode process
func DecodeUint32(data []byte) (val uint32, result []byte) {
	if len(data) < 4 {
		result = data
	} else {
		val = *(*uint32)(unsafe.Pointer(&data[0]))
		result = data[4:]
	}
	return
}

// decode a uint64 value and return the slice after the bytes
// have been consumed by the decode process
func DecodeUint64(data []byte) (val uint64, result []byte) {
	if len(data) < 8 {
		result = data
	} else {
		val = *(*uint64)(unsafe.Pointer(&data[0]))
		result = data[8:]
	}
	return
}

// decode a slice and return it after the bytes
// have been consumed by the decode process
func DecodeSlice(data []byte) (val []byte, result []byte) {
	origin := len(data)
	if origin < 4 {
		result = data
		return
	}

	sliceLen := *(*uint32)(unsafe.Pointer(&data[0]))

	if uint32(origin) < sliceLen+4 {
		result = data
		return
	}

	val = data[4 : sliceLen+4]
	result = data[sliceLen+4:]
	return
}
