package bitcask

import (
	"os"
	"path"
	"time"

	"github.com/Eslam-Nawara/bitcask/internal/datastore"
	"github.com/Eslam-Nawara/bitcask/internal/keydir"
	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
)

func parseUsrOpts(opts []ConfigOpt) options {
	usrOpts := options{
		syncOption:       SyncOnDemand,
		accessPermission: ReadOnly,
	}

	for _, opt := range opts {
		switch opt {
		case SyncOnPut:
			usrOpts.syncOption = SyncOnPut
		case ReadWrite:
			usrOpts.accessPermission = ReadWrite
		}
	}

	return usrOpts
}

func (bitcask *Bitcask) setPermessions(dataStorePath string) (keydir.KeyDirPrivacy, datastore.LockMode) {
	var privacy keydir.KeyDirPrivacy
	var lockMode datastore.LockMode

	if bitcask.usrOpts.accessPermission == ReadWrite {
		privacy = keydir.PrivateKeyDir
		lockMode = datastore.ExclusiveLock
		fileFlags := os.O_CREATE | os.O_RDWR
		if bitcask.usrOpts.syncOption == SyncOnPut {
			fileFlags |= os.O_SYNC
		}
		bitcask.fileFlags = fileFlags
		bitcask.activeFile = datastore.NewAppendFile(dataStorePath, bitcask.fileFlags, datastore.Active)
	} else {
		privacy = keydir.SharedKeyDir
		lockMode = datastore.SharedLock
	}

	return privacy, lockMode
}

func (bitcask *Bitcask) listOldFiles() ([]string, error) {
	oldFiles := make([]string, 0)

	dataStore, err := os.Open(bitcask.dataStore.Path())
	if err != nil {
		return nil, err
	}
	defer dataStore.Close()

	bitcask.accessMu.Lock()
	files, err := dataStore.Readdir(0)
	bitcask.accessMu.Unlock()
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		fileName := file.Name()
		if fileName[0] != '.' && fileName != bitcask.activeFile.Name() && fileName != "keydir" {
			oldFiles = append(oldFiles, fileName)
		}
	}

	return oldFiles, nil
}

// mergeWrite performs a writing to the created merge file.
// returns the new record about the written data
// returns error if the data is deleted and will not be written again or on any system failures.
func (bitcask *Bitcask) mergeWrite(mergeFile *datastore.AppendFile, key string) (recfmt.KeyDirRec, error) {
	rec := bitcask.keyDir[key]

	value, err := bitcask.dataStore.ReadValueFromFile(rec.FileId, key, rec.ValuePos, rec.ValueSize)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}

	tStamp := time.Now().UnixMicro()

	n, err := mergeFile.WriteData(key, value, tStamp)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}

	newRec := recfmt.KeyDirRec{
		FileId:    mergeFile.Name(),
		ValuePos:  uint32(n),
		ValueSize: uint32(len(value)),
		TStamp:    tStamp,
	}

	err = mergeFile.WriteHint(key, newRec)
	if err != nil {
		return recfmt.KeyDirRec{}, err
	}
	return newRec, nil
}

// deleteOldFiles deletes all files passed to it.
func (bitcask *Bitcask) deleteOldFiles(files []string) error {
	for _, file := range files {
		err := os.Remove(path.Join(bitcask.dataStore.Path(), file))
		if err != nil {
			return err
		}
	}

	return nil
}
