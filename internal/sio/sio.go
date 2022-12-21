package sio

import (
	"io/fs"
	"os"
)

const maxAttempts = 5

type File struct {
	File *os.File
}

func OpenFile(fileName string, flag int, perm fs.FileMode) (*File, error) {
	file, err := os.OpenFile(fileName, flag, perm)
	if err != nil {
		return nil, err
	}

	return &File{File: file}, nil
}

func Open(fileName string) (*File, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	return &File{File: file}, err
}

func (file *File) ReadAt(out []byte, off int64) (int, error) {
	n, err := file.File.ReadAt(out, off)

	for i, attempts := n, 0; err != nil; i, attempts = i+n, attempts+1 {
		if attempts == maxAttempts {
			return 0, err
		}
		off += int64(i)
		n, err = file.File.ReadAt(out[i:], int64(off))
	}

	return len(out), nil
}

func (file *File) Write(out []byte) (int, error) {
	n, err := file.File.Write(out)

	for i, attempts := n, 0; err != nil; i, attempts = i+n, attempts+1 {
		if attempts == maxAttempts {
			return 0, err
		}
		n, err = file.File.Write(out[i:])
	}

	return len(out), nil
}
