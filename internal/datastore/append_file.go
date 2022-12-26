package datastore

import "github.com/Eslam-Nawara/bitcask/internal/sio"

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
