package bitcask

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Eslam-Nawara/bitcask/internal/datastore"
	"github.com/Eslam-Nawara/bitcask/internal/keydir"
	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
)

const (
	// ReadOnly gives the bitcask process a read only permission.
	ReadOnly ConfigOpt = 0
	// ReadWrite gives the bitcask process read and write permissions.
	ReadWrite ConfigOpt = 1
	// SyncOnPut makes the bitcask flush all the writes directly to the disk.
	SyncOnPut ConfigOpt = 2
	// SyncOnDemand gives the user the control on whenever to do flush operation.
	SyncOnDemand ConfigOpt = 3
)

// errRequireWrite happens whenever a user with ReadOnly permission tries to do a writing operation.
var errRequireWrite = errors.New("require write permission")

type (
	// ConfigOpt represents the config options the user can have.
	ConfigOpt int

	// options groups the config options passed to Open.
	options struct {
		syncOption       ConfigOpt
		accessPermission ConfigOpt
	}

	// Bitcask represents the bitcask object.
	// Bitcask contains the metadata needed to manipulate the bitcask datastore.
	// User creates an object of it to use the bitcask.
	// Provides several methods to manipulate the datastore data.
	Bitcask struct {
		keyDir     keydir.KeyDir
		usrOpts    options
		accessMu   sync.Mutex
		readerCnt  int32
		dataStore  *datastore.DataStore
		activeFile *datastore.AppendFile
		fileFlags  int
	}
)

func Open(dataStorePath string, opts ...ConfigOpt) (*Bitcask, error) {
	b := &Bitcask{}
	b.usrOpts = parseUsrOpts(opts)

	privacy, lockMode := b.setPermessions(dataStorePath)

	dataStore, err := datastore.NewDataStore(dataStorePath, lockMode)
	if err != nil {
		return nil, err
	}

	keyDir, err := keydir.NewKeyDir(dataStorePath, privacy)
	if err != nil {
		return nil, err
	}

	b.dataStore = dataStore
	b.keyDir = keyDir

	return b, nil
}

func (b *Bitcask) Get(key string) (string, error) {
	var value string
	var err error

	if b.readerCnt == 0 {
		b.accessMu.Lock()
	}
	atomic.AddInt32(&b.readerCnt, 1)

	rec, isExist := b.keyDir[key]
	if !isExist {
		value = ""
		err = fmt.Errorf("%s: %s", key, datastore.ErrKeyNotExist)
	} else {
		value, err = b.dataStore.ReadValueFromFile(rec.FileId, key, rec.ValuePos, rec.ValueSize)
	}

	atomic.AddInt32(&b.readerCnt, -1)
	if b.readerCnt == 0 {
		b.accessMu.Unlock()
	}

	return value, err
}

func (bitcask *Bitcask) Put(key, value string) error {
	if bitcask.usrOpts.accessPermission == ReadOnly {
		return fmt.Errorf("Put: %s", errRequireWrite)
	}
	tStamp := time.Now().UnixMicro()

	bitcask.accessMu.Lock()
	defer bitcask.accessMu.Unlock()

	n, err := bitcask.activeFile.WriteData(key, value, tStamp)
	if err != nil {
		return err
	}

	bitcask.keyDir[key] = recfmt.KeyDirRec{
		FileId:    bitcask.activeFile.Name(),
		ValuePos:  uint32(n),
		ValueSize: uint32(len(value)),
		TStamp:    tStamp,
	}

	return nil
}

func (bitcask *Bitcask) Delete(key string) error {
	if bitcask.usrOpts.accessPermission == ReadOnly {
		return fmt.Errorf("Delete: %s", errRequireWrite)
	}

	_, err := bitcask.Get(key)
	if err != nil {
		return err
	}

	bitcask.Put(key, datastore.TompStone)

	return nil
}

func (bitcask *Bitcask) ListKeys() []string {
	res := make([]string, 0)

	if bitcask.readerCnt == 0 {
		bitcask.accessMu.Lock()
	}
	atomic.AddInt32(&bitcask.readerCnt, 1)

	for key := range bitcask.keyDir {
		res = append(res, key)
	}

	atomic.AddInt32(&bitcask.readerCnt, -1)
	if bitcask.readerCnt == 0 {
		bitcask.accessMu.Unlock()
	}

	return res
}

func (bitcask *Bitcask) Fold(fn func(string, string, any) any, acc any) any {
	if bitcask.readerCnt == 0 {
		bitcask.accessMu.Lock()
	}
	atomic.AddInt32(&bitcask.readerCnt, 1)

	for key := range bitcask.keyDir {
		value, _ := bitcask.Get(key)
		acc = fn(key, value, acc)
	}

	atomic.AddInt32(&bitcask.readerCnt, -1)
	if bitcask.readerCnt == 0 {
		bitcask.accessMu.Unlock()
	}

	return acc
}

func (bitcask *Bitcask) Merge() error {
	if bitcask.usrOpts.accessPermission == ReadOnly {
		return fmt.Errorf("Merge: %s", errRequireWrite)
	}

	oldFiles, err := bitcask.listOldFiles()
	if err != nil {
		return err
	}

	bitcask.accessMu.Lock()
	newKeyDir := keydir.KeyDir{}
	mergeFile := datastore.NewAppendFile(bitcask.dataStore.Path(), bitcask.fileFlags, datastore.Merge)
	defer mergeFile.Close()

	for key, rec := range bitcask.keyDir {
		if rec.FileId != bitcask.activeFile.Name() {
			newRec, err := bitcask.mergeWrite(mergeFile, key)
			if err != nil {
				if !strings.HasSuffix(err.Error(), datastore.ErrKeyNotExist.Error()) {
					bitcask.accessMu.Unlock()
					return err
				}
			} else {
				newKeyDir[key] = newRec
			}
		} else {
			newKeyDir[key] = rec
		}
	}

	bitcask.keyDir = newKeyDir
	bitcask.accessMu.Unlock()
	bitcask.deleteOldFiles(oldFiles)

	return nil
}

func (bitcask *Bitcask) Sync() error {
	if bitcask.usrOpts.accessPermission == ReadOnly {
		return fmt.Errorf("Sync: %s", errRequireWrite)
	}

	return bitcask.activeFile.Sync()
}

func (bitcask *Bitcask) Close() {
	if bitcask.usrOpts.accessPermission == ReadWrite {
		bitcask.Sync()
		bitcask.activeFile.Close()
	}
	bitcask.dataStore.Close()
}
