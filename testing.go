package gdb

import (
	"math/rand"
)

// Define a few utility functions that unit tests can use

// Creating random numbers in the range [low, high), creating @num
// number of such random number
func MakeRandomInt(low, high, num int) []int {
	ret := make([]int, num)
	depth := high - low

	for i := 0; i < num; i++ {
		val := rand.Intn(depth)
		ret[i] = low + val
	}

	return ret
}

// Creating @num random byte slices. Each of slice has minimum @minSize
// of bytes and maximum of @maxSize bytes.
// Each byte generated in this way will be in the range [minVal, maxVal)
func MakeRandomSlice(minVal, maxVal uint8, minSize, maxSize, num int) [][]byte {
	ret := make([][]byte, num)

	for i := 0; i < num; i++ {
		size := MakeRandomInt(minSize, maxSize, 1)[0]
		tmp := MakeRandomInt(int(minVal), int(maxVal), size)

		val := make([]byte, size)
		for j := 0; j < size; j++ {
			val[j] = uint8(tmp[j])
		}

		ret[i] = val
	}

	return ret
}
