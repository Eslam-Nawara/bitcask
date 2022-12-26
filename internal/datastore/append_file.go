package datastore

import (
	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
	"github.com/Eslam-Nawara/bitcask/internal/sio"
)

const (
	// Merge represents that the file type is a merge file.
	Merge AppendType = 0
	// Merge represents that the file type is an active file.
	Active AppendType = 1

	// maxFileSize represents the maximum size for each file.
	maxFileSize = 10 * 1024
)

type (
	// AppendType represents the type of the append file.
	AppendType int

	// AppendFile contains the metadata about the append file.
	AppendFile struct {
		fileWrapper *sio.File
		hintWrapper *sio.File
		fileName    string
		filePath    string
		fileFlags   int
		appendType  AppendType
		currentPos  int
		currentSize int
	}
)

func (appendFile *AppendFile) WriteData(key, value string, tStamp int64) (int, error) {
	rec := recfmt.CompressDataFileRec(key, value, tStamp)

	if appendFile.fileWrapper == nil || len(rec)+appendFile.currentSize > maxFileSize {
		err := appendFile.newAppendFile()
		if err != nil {
			return 0, err
		}
	}

	n, err := appendFile.fileWrapper.Write(rec)
	if err != nil {
		return 0, err
	}

	writePos := appendFile.currentPos
	appendFile.currentPos += n
	appendFile.currentSize += n

	return writePos, nil
}

func (a *AppendFile) WriteHint(key string, rec recfmt.KeyDirRec) error {
	return nil
}

// Name returns the name of the append file.
func (appendFile *AppendFile) Name() string {
	return appendFile.fileName
}

// Sync flushes the data written to the append file to the disk.
func (appendFile *AppendFile) Sync() error {
	if appendFile.fileWrapper != nil {
		return appendFile.fileWrapper.File.Sync()
	}

	return nil
}

// Close closes the append file and its associated hint file if exists.
func (appendFile *AppendFile) Close() {
	if appendFile.fileWrapper != nil {
		appendFile.fileWrapper.File.Close()
		if appendFile.appendType == Merge {
			appendFile.hintWrapper.File.Close()
		}
	}
}

func (a *AppendFile) newAppendFile() error {
	return nil
}
