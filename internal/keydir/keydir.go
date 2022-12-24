package keydir

import (
	"io/fs"
	"os"
	"path"

	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
	"github.com/Eslam-Nawara/bitcask/internal/sio"
)

type (
	KeyDirPrivacy int

	fileType int
	KeyDir   map[string]recfmt.KeydirRec
)

const (
	keyDirFile                 = "keydir"
	SharedKeyDir KeyDirPrivacy = 1
)

func New(dataStorePath string, privacy KeyDirPrivacy) (KeyDir, error) {
	keyDir := KeyDir{}

	okay, err := keyDir.buildFromKeydirFile(dataStorePath)
	if err != nil {
		return nil, err
	}
	if okay {
		return keyDir, nil
	}

	err = keyDir.buildFromDataStoreFiles(dataStorePath)
	if err != nil {
		return nil, err
	}

	if privacy == SharedKeyDir {
		keyDir.share(dataStorePath)
	}

	return keyDir, nil
}

func (keyDir KeyDir) buildFromKeydirFile(dataStorePath string) (bool, error) {
	data, err := os.ReadFile(path.Join(dataStorePath, keyDirFile))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}

	okay, err := isOld(dataStorePath)
	if err != nil || !okay {
		return false, nil
	}

	n := len(data)
	for i := 0; i < n; {
		key, rec, recLen := recfmt.ExtractKeyDirRec(data[i:])
		keyDir[key] = rec
		i += recLen
	}

	return true, nil
}

func (keyDir KeyDir) buildFromDataStoreFiles(dataStorePath string) error {
	dataStore, err := os.Open(dataStorePath)
	if err != nil {
		return err
	}
	defer dataStore.Close()
	files, err := dataStore.Readdir(0)
	if err != nil {
		return err
	}
	fileNames := extractFileNames(files)

	err = keyDir.parseFiles(dataStorePath, categorizeFiles(fileNames))
	if err != nil {
		return err
	}

	return nil
}

func extractFileNames(files []fs.FileInfo) []string {
	fileNames := make([]string, 0)
	for _, file := range files {
		if file.Name()[0] != '.' {
			fileNames = append(fileNames, file.Name())
		}
	}
	return fileNames
}

func (keyDir KeyDir) share(dataStorePath string) error {
	flags := os.O_CREATE | os.O_RDWR | os.O_TRUNC
	perm := os.FileMode(0666)
	file, err := sio.OpenFile(path.Join(dataStorePath, "keydir"), flags, perm)
	if err != nil {
		return err
	}

	for key, rec := range keyDir {
		buff := recfmt.CompressKeyDirRec(key, rec)
		_, err := file.Write(buff)
		if err != nil {
			return err
		}
	}
	return nil
}

func isOld(dataStorePath string) (bool, error) {
	return false, nil
}

func (k KeyDir) parseFiles(dataStorePath string, files map[string]fileType) error {
	return nil
}

func (k KeyDir) parseDataFile(dataStorePath, name string) error {
	return nil
}

// parseHintFile parses the data from hint files.
// return and error on system failures.
func (k KeyDir) parseHintFile(dataStorePath, name string) error {
	return nil
}

// categorizeFiles specifies whether the file is data or hint file.
func categorizeFiles(allFiles []string) map[string]fileType {
	return nil
}
