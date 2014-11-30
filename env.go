package gdb

import "os"

type NativeEnv struct {
}

func (a NativeEnv) NewSequentialFile(name string) (f SequentialFile, s Status) {
	f = MakeLocalSequentialFile(name)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) NewRandomAccessFile(n string) (f RandomAccessFile, s Status) {
	f = MakeLocalRandomAccessFile(n)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) NewWritableFile(name string) (f WritableFile, s Status) {
	f = MakeLocalWritableFile(name)
	if f == nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) FileExists(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
	} else {
		f.Close()
	}
	return true
}

func (a NativeEnv) GetChildren(dir string) (list []string, s Status) {
	f, err := os.Open(dir)
	if err != nil {
		s = MakeStatusIoError("")
		return
	}

	list, err = f.Readdirnames(0)
	if err != nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
	}
	return
}

func (a NativeEnv) DeleteFile(name string) Status {
	err := os.Remove(name)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) CreateDir(dir string) Status {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) DeleteDir(dir string) Status {
	err := os.RemoveAll(dir)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}

func (a NativeEnv) GetFileSize(name string) (size uint64, s Status) {
	fi, err := os.Stat(name)
	if err != nil {
		s = MakeStatusIoError("")
	} else {
		s = MakeStatusOk()
		size = uint64(fi.Size())
	}
	return
}

func (a NativeEnv) RenameFile(src string, target string) Status {
	err := os.Rename(src, target)
	if err != nil {
		return MakeStatusIoError("")
	} else {
		return MakeStatusOk()
	}
}
