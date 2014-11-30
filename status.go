package gdb

// Implement different Status

// Return a status that returns StatusOk
func MakeStatusOk() Status {
	return &StatusOk{}
}

// Return a status that returns StatusNotFound
func MakeStatusNotFound(msg string) Status {
	return StatusNotFound{msg: msg}
}

// Return a status that returns StatusCorruption
func MakeStatusCorruption(msg string) Status {
	return StatusCorruption{msg: msg}
}

// Return a status that returns StatusIoError
func MakeStatusIoError(msg string) Status {
	return StatusIoError{msg: msg}
}

// This structure is the base for all other status structs
type AllNegativeStatus struct {
}

func (a AllNegativeStatus) Ok() bool {
	return false
}

func (a AllNegativeStatus) IsNotFound() bool {
	return false
}

func (a AllNegativeStatus) IsCorruption() bool {
	return false
}

func (a AllNegativeStatus) IsIoError() bool {
	return false
}

func (a AllNegativeStatus) ToString() string {
	return ""
}

// implements ok status
type StatusOk struct {
	AllNegativeStatus
}

func (a StatusOk) Ok() bool {
	return true
}

// implement NotFound status
type StatusNotFound struct {
	AllNegativeStatus
	msg string
}

func (a StatusNotFound) IsNotFound() bool {
	return true
}

func (a StatusNotFound) ToString() string {
	return a.msg
}

// implement Corruption status
type StatusCorruption struct {
	AllNegativeStatus
	msg string
}

func (a StatusCorruption) IsCorruption() bool {
	return true
}

func (a StatusCorruption) ToString() string {
	return a.msg
}

// implement IoError status
type StatusIoError struct {
	AllNegativeStatus
	msg string
}

func (a StatusIoError) IsIoError() bool {
	return true
}

func (a StatusIoError) ToString() string {
	return a.msg
}
