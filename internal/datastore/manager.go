package datastore

import (
	"errors"
	"fmt"
	"os"
	"path"

	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
	"github.com/Eslam-Nawara/bitcask/internal/sio"
	"github.com/gofrs/flock"
)

const (
	// ExclusiveLock is an option to make the datastore lock exclusive.
	ExclusiveLock LockMode = 0
	// SharedLock is an option to make the datastore lock shared.
	SharedLock LockMode = 1

	// TompStone is a special value to mark the deleted values.
	TompStone = "8890fc70294d02dbde257989e802451c2276be7fb177c3ca4399dc4728e4e1e0"

	// lockFile is the name of the file used to lock the datastore directory.
	lockFile = ".lck"
)

var (
	// errAccessDenied happens when a bitcask process tries to access to the datastore
	// when the directory is locked.
	errAccessDenied = errors.New("access denied: datastore is locked")

	// ErrKeyNotExist happens when accessing value does not exist.
	ErrKeyNotExist = errors.New("key does not exist")
)

type (
	// LockMode represents the lock mode of the directory.
	LockMode int

	// DataStore represents and contains the metadata of the datastore directory.
	DataStore struct {
		path    string
		lckMode LockMode
		flck    *flock.Flock
	}
)

func NewDataStore(dataStorePath string, mode LockMode) (*DataStore, error) {
	datastore := &DataStore{
		path:    dataStorePath,
		lckMode: mode,
	}

	dir, dirErr := os.Open(dataStorePath)
	if dirErr != nil && !os.IsNotExist(dirErr) {
		return nil, dirErr
	}
	defer dir.Close()

	if dirErr == nil {
		acquired, err := datastore.openDataStoreDir()
		if !acquired {
			err = errAccessDenied
		}
		if err != nil {
			return nil, err
		}
	} else if mode == ExclusiveLock {
		err := datastore.createDataStoreDir()
		if err != nil {
			return nil, err
		}
	} else {
		return nil, dirErr
	}
	return datastore, nil
}

func NewAppendFile(dataStorePath string, fileFlags int, appendType AppendType) *AppendFile {
	return &AppendFile{
		filePath:   dataStorePath,
		fileFlags:  fileFlags,
		appendType: appendType,
	}
}

func (d *DataStore) ReadValueFromFile(fileId, key string, valuePos, valueSize uint32) (string, error) {
	buff := make([]byte, recfmt.DataFileHdrSize+uint32(len(key))+valueSize)

	f, err := sio.Open(path.Join(d.path, fileId))
	if err != nil {
		return "", err
	}
	defer f.File.Close()

	f.ReadAt(buff, int64(valuePos))
	data, _, err := recfmt.ExtractDataFileRec(buff)
	if err != nil {
		return "", err
	}

	if data.Value == TompStone {
		return "", fmt.Errorf("%s: %s", data.Key, ErrKeyNotExist)
	}

	return data.Value, nil
}

func (dataStore *DataStore) Path() string {
	return dataStore.path
}

// Close frees the acquired lock on the datastore directory.
func (dataStore *DataStore) Close() {
	dataStore.flck.Unlock()
}

func (dataStore *DataStore) openDataStoreDir() (bool, error) {
	acquired, err := dataStore.acquireFileLock()
	if err != nil {
		return false, err
	}

	return acquired, nil
}

func (dataStore *DataStore) createDataStoreDir() error {
	err := os.MkdirAll(dataStore.path, os.FileMode(0777))
	if err != nil {
		return err
	}

	_, err = dataStore.acquireFileLock()
	if err != nil {
		return err
	}
	return nil
}

func (dataStore *DataStore) acquireFileLock() (bool, error) {
	err, ok := errors.New(""), false
	dataStore.flck = flock.New(path.Join(dataStore.path, lockFile))

	switch dataStore.lckMode {
	case ExclusiveLock:
		ok, err = dataStore.flck.TryLock()
	case SharedLock:
		ok, err = dataStore.flck.TryRLock()
	}

	if err != nil {
		return false, err
	}
	return ok, nil
}
