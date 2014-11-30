package gdb

import (
	"fmt"
	"sort"
	"strings"
)

type FileInfo struct {
	size   uint32
	ref    uint32
	minKey []byte
	maxKey []byte
}

func (fi *FileInfo) Ref() {
	fi.ref = fi.ref + 1
}

func (fi *FileInfo) Unref(fh uint64, set *VersionSet) {
	fi.ref = fi.ref - 1
	switch {
	case fi.ref > 0:
		return
	case fi.ref == 0:
		// TODO: delete underlying file?
		delete(set.fileMap, fh)
	default:
		panic("file reference becomes negative!")
	}
}

func (fi *FileInfo) IsLogFile() bool {
	return fi.minKey == nil && fi.maxKey == nil
}

// TODO: should we really skip @ref field?
func (fi *FileInfo) EncodeTo(scratch []byte) []byte {
	scratch = EncodeUint32(scratch, fi.size)
	scratch = EncodeSlice(scratch, fi.minKey)
	scratch = EncodeSlice(scratch, fi.maxKey)
	return scratch
}

// decode from a byte buffer. Return the remaining slice. If the buffer
// cannot be decoded, return the original buffer
func (fi *FileInfo) DecodeFrom(buffer []byte) (res []byte) {
	fi.size, res = DecodeUint32(buffer)
	if len(res) == len(buffer) {
		return
	}

	oldLen := len(res)
	fi.minKey, res = DecodeSlice(res)
	if len(res) == oldLen {
		res = buffer
		return
	}

	oldLen = len(res)
	fi.maxKey, res = DecodeSlice(res)
	if len(res) == oldLen {
		res = buffer
		return
	}

	return
}

// describe a particular version (snapshot)
type Version struct {
	lastSequence uint64
	logFiles     []uint64
	levels       [][]uint64
	prev         *Version
	next         *Version
	set          *VersionSet
	ref          int
}

// Make a new version based on information from @origin
func MakeVersion(set *VersionSet, origin *Version) *Version {
	ret := &Version{}

	ret.set = set
	ret.lastSequence = origin.lastSequence

	ret.logFiles = make([]uint64, 0, len(origin.logFiles))
	for _, fh := range origin.logFiles {
		fi, ok := set.fileMap[fh]
		if !ok {
			panic("Fails to find the file info")
		}

		fi.Ref()
		ret.logFiles = append(ret.logFiles, fh)
	}

	ret.levels = make([][]uint64, 0, len(origin.levels))
	for _, l := range origin.levels {
		fs := make([]uint64, 0, len(l))
		for _, fh := range l {
			fs = append(fs, fh)
		}
		ret.levels = append(ret.levels, fs)
	}

	return ret
}

func (v *Version) Ref() {
	v.ref = v.ref + 1
}

func (v *Version) Apply(edit *VersionEdit) {
	v.lastSequence = edit.lastSequence
	v.set.nextFileNumber = edit.nextFileNumber

	logFilesAdded := make([]uint64, 0, 8)
	logFilesRemoved := make([]uint64, 0, 8)

	for _, add := range edit.adds {
		fi := &add.info
		v.set.fileMap[add.fileNumber] = *fi
		fi.Ref()
		if fi.IsLogFile() {
			logFilesAdded = append(logFilesAdded, add.fileNumber)
		}
	}

	for _, change := range edit.versionLevelChanges {
		if change.originLevel >= 0 {
			level := v.levels[change.originLevel]
			for idx, val := range level {
				// remove the file number from the level
				if val == change.fileNumber {
					last := len(level) - 1
					for ; idx < last; idx++ {
						level[idx] = level[idx+1]
					}
					v.levels[change.originLevel] = level[:last-1]
					break
				}
			}
		}
	}

	for _, change := range edit.versionLevelChanges {
		if change.newLevel >= 0 {
			level := v.levels[change.newLevel]
			info, ok := v.set.fileMap[change.fileNumber]
			if !ok {
				panic("Fails to find file info")
			}

			key := info.minKey
			size := len(level)

			idx := sort.Search(
				size,
				func(i int) bool {
					fh := level[i]
					fi, ok := v.set.fileMap[fh]
					if !ok {
						panic("Fails to find file handle!")
					}

					return v.set.comparator.Compare(fi.minKey, key) >= 0
				})

			// insert the new file number into the correct place
			level = append(level, uint64(0))
			for i := idx; i < size; i++ {
				level[i+1] = level[i]
			}

			level[idx] = change.fileNumber
			v.levels[change.newLevel] = level
		}
	}

	for _, fh := range edit.removes {
		fi, ok := v.set.fileMap[fh]
		if !ok {
			panic("Fails to find a file handle")
		}

		if fi.IsLogFile() {
			logFilesRemoved = append(logFilesRemoved, fh)
		}

		fi.Unref(fh, v.set)
	}

	for _, fh := range logFilesAdded {
		v.logFiles = append(v.logFiles, fh)
	}

	for _, fh := range logFilesRemoved {
		size := len(v.logFiles)
		for idx, val := range v.logFiles {
			if val == fh {
				for i := idx; i < size-1; i++ {
					v.logFiles[i] = v.logFiles[i+1]
				}
				v.logFiles = v.logFiles[:size-1]
				break
			}
		}
	}
}

// describe a file's level change. If originalLevel or newLevel
// is a negative value, that means there is no original or
// newLevel for this change
type VersionLevelChange struct {
	fileNumber  uint64
	originLevel int32
	newLevel    int32
}

func (change *VersionLevelChange) MoveLevel(originLevel, newLevel int32) {
	change.originLevel, change.newLevel = originLevel, newLevel
}

func (change *VersionLevelChange) RemoveLevel(level int32) {
	change.originLevel, change.newLevel = level, -1
}

func (change *VersionLevelChange) AddLevel(level int32) {
	change.originLevel, change.newLevel = -1, level
}

func (change *VersionLevelChange) EncodeTo(scratch []byte) []byte {
	scratch = EncodeUint64(scratch, change.fileNumber)
	scratch = EncodeUint32(scratch, uint32(change.originLevel))
	scratch = EncodeUint32(scratch, uint32(change.newLevel))
	return scratch
}

func (change *VersionLevelChange) DecodeFrom(buffer []byte) []byte {
	var res []byte
	change.fileNumber, res = DecodeUint64(buffer)
	if len(buffer) == len(res) {
		return buffer
	}

	var val uint32
	oldLen := len(res)
	val, res = DecodeUint32(res)
	if len(res) == oldLen {
		return buffer
	}
	change.originLevel = int32(val)

	oldLen = len(res)
	val, res = DecodeUint32(res)
	if len(res) == oldLen {
		return buffer
	}
	change.newLevel = int32(val)
	return res
}

// describe the event that adds a new file to the system
type VersionFileAdd struct {
	fileNumber uint64
	info       FileInfo
}

// describe changes made on top of a base version
type VersionEdit struct {
	adds                []VersionFileAdd
	removes             []uint64
	versionLevelChanges []VersionLevelChange
	lastSequence        uint64
	nextFileNumber      uint64
}

func (edit *VersionEdit) EncodeTo(scratch []byte) []byte {
	// encode map
	{
		num := len(edit.adds)
		scratch = EncodeUint32(scratch, uint32(num))

		for _, v := range edit.adds {
			scratch = EncodeUint64(scratch, v.fileNumber)
			scratch = (&v.info).EncodeTo(scratch)
		}
	}

	// encode removals
	{
		num := len(edit.removes)
		scratch = EncodeUint32(scratch, uint32(num))

		for _, v := range edit.removes {
			scratch = EncodeUint64(scratch, v)
		}
	}

	// encode level changes
	{
		num := len(edit.versionLevelChanges)
		scratch = EncodeUint32(scratch, uint32(num))

		for _, change := range edit.versionLevelChanges {
			scratch = (&change).EncodeTo(scratch)
		}
	}

	scratch = EncodeUint64(scratch, edit.lastSequence)
	scratch = EncodeUint64(scratch, edit.nextFileNumber)

	return scratch
}

// decode an edit from a binary buffer, returns the remaining
// buffer after decoding. If the buffer is malformed and nothing
// has been decoded, return false as second return value
func (edit *VersionEdit) DecodeFrom(buffer []byte) (ret []byte, ok bool) {
	var remaining []byte

	// decode adds
	{
		num, remaining := DecodeUint32(buffer)
		if len(remaining) == len(buffer) {
			return
		}

		for i := uint32(0); i < num; i++ {
			key, result := DecodeUint64(remaining)
			if len(result) == len(remaining) {
				return
			}

			fi := FileInfo{}
			result2 := fi.DecodeFrom(result)
			if len(result2) == len(result) {
				return
			}

			add := VersionFileAdd{key, fi}
			edit.adds = append(edit.adds, add)
			remaining = result2
		}
	}

	// decode removal
	{
		oldLen := len(remaining)
		num, remaining := DecodeUint32(remaining)
		if len(remaining) == oldLen {
			return
		}

		for i := uint32(0); i < num; i++ {
			key, result := DecodeUint64(remaining)
			if len(result) == len(remaining) {
				return
			}

			edit.removes = append(edit.removes, key)
			remaining = result
		}
	}

	// decode level changes
	{
		oldLen := len(remaining)
		num, remaining := DecodeUint32(remaining)
		if len(remaining) == oldLen {
			return
		}

		for i := uint32(0); i < num; i++ {
			change := VersionLevelChange{}
			oldLen = len(remaining)
			remaining = change.DecodeFrom(remaining)
			if len(remaining) == oldLen {
				return
			}

			edit.versionLevelChanges = append(edit.versionLevelChanges, change)
		}
	}

	{
		var result []byte
		edit.lastSequence, result = DecodeUint64(remaining)
		if len(result) == len(remaining) {
			return
		}

		remaining = result
	}

	{
		var result []byte
		edit.nextFileNumber, result = DecodeUint64(remaining)
		if len(result) == len(remaining) {
			return
		}

		ret, ok = result, true
	}

	return
}

type VersionSet struct {
	name           string
	lastSequence   uint64
	nextFileNumber uint64
	current        *Version
	base           *Version
	fileMap        map[uint64]FileInfo
	env            Env
	comparator     Comparator
	log            WritableFile
}

func MakeVersionSet(name string, env Env, c Comparator) *VersionSet {
	ret := &VersionSet{}

	ret.base = &Version{}
	ret.base.prev, ret.base.next = ret.base, ret.base

	ret.current = ret.base

	ret.name = name
	ret.env = env
	ret.comparator = c

	return ret
}

func (a *VersionSet) AddVersion(b *Version) {
	b.next = a.current
	b.prev = a.current.prev

	a.current.prev = b
	b.prev.next = b

	a.current = b
}

func (a *VersionSet) RemoveVersion(b *Version) {
	if b == a.base {
		panic("Cannot remove base version")
	}

	b.prev.next = b.next
	b.next.prev = b.prev
}

func (a *VersionSet) LogAndApply(e *VersionEdit) Status {
	var log WritableFile
	var name string

	// create a new version log file if we have not done so
	if a.log == nil {
		name = fmt.Sprintf("%s/version_%d.log", a.name, e.nextFileNumber)
		e.nextFileNumber = e.nextFileNumber + 1
		var s Status
		log, s = a.env.NewWritableFile(name)
		if !s.Ok() {
			return s
		}
	} else {
		log = a.log
	}

	{
		record := make([]byte, 0, 4096)
		record = e.EncodeTo(record)
		s := log.Append(record)

		if !s.Ok() {
			return s
		}
	}

	if a.current.ref == 0 {
		a.current.Apply(e)
	} else {
		newVersion := MakeVersion(a, a.current)
		newVersion.Apply(e)
		a.AddVersion(newVersion)
	}

	// create a new manifest file if we have not done so
	if a.log == nil {
		a.log = log
		manifest := fmt.Sprintf("%s/manifest", a.name)
		future := fmt.Sprintf("%s/manifest.future", a.name)

		a.env.DeleteFile(future)
		futureFile, status := a.env.NewWritableFile(future)
		if !status.Ok() {
			return status
		}

		futureFile.Append([]byte(name))
		futureFile.Close()

		status = a.env.DeleteFile(manifest)
		if !status.Ok() {
			return status
		}

		status = a.env.RenameFile(future, manifest)
		if !status.Ok() {
			return status
		}
	}

	return MakeStatusOk()
}

func (a *VersionSet) Recover() Status {
	manifest := strings.Join([]string{a.name, "manifest"}, "/")
	if !a.env.FileExists(manifest) {
		return MakeStatusCorruption("")
	}

	fileSize, status := a.env.GetFileSize(manifest)
	if !status.Ok() {
		return status
	}

	file, status2 := a.env.NewSequentialFile(manifest)
	if !status2.Ok() {
		return status2
	}

	defer file.Close()

	data := make([]byte, fileSize)
	res, status3 := file.Read(data)
	if !status3.Ok() {
		return status3
	}
	if len(res) != len(data) {
		return MakeStatusCorruption("")
	}

	versionLogName := string(res)
	return a.recoverFromLogFile(versionLogName)
}

func (a *VersionSet) recoverFromLogFile(name string) Status {
	logFile, status := a.env.NewSequentialFile(name)
	if !status.Ok() {
		return status
	}

	defer logFile.Close()

	version := MakeVersion(a, a.current)
	buffer := [4096]byte{}
	reader := Reader{logFile, 0, true}

	for true {
		record, result := reader.ReadRecord(buffer[:])
		switch result {
		case ReadStatusOk:
			edit := VersionEdit{}
			_, ok := edit.DecodeFrom(record)
			if !ok {
				return MakeStatusCorruption("")
			}

			version.Apply(&edit)

		case ReadStatusEOF:
			a.AddVersion(version)
			return MakeStatusOk()

		case ReadStatusCorruption:
			return MakeStatusCorruption("")

		default:
			panic("unexpected result")
		}
	}

	panic("should not reach here")
	return MakeStatusCorruption("")
}
