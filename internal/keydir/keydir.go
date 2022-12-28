package keydir

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"

	"github.com/Eslam-Nawara/bitcask/internal/recfmt"
	"github.com/Eslam-Nawara/bitcask/internal/sio"
)

const (
	// PrivateKeyDir specifies that the keydir is owned by a writer process and will not be shared.
	PrivateKeyDir KeyDirPrivacy = 0
	// SharedKeyDir specifies that the keydir is owned by a reader proccess and will
	// available writers to used it instead of parsing the whole datastore files.
	SharedKeyDir KeyDirPrivacy = 1

	// keyDirFile is the name of the file used to share the keydir map.
	keyDirFile = "keydir"

	// data represents that the file is a data file.
	data fileType = 0
	// hint represents that the file is a hint file.
	hint fileType = 1
)

type (
	// fileType specifies whether the file is a data or hint file.
	fileType int

	// KeyDirPrivacy specifies whether the keydir is private or shared.
	KeyDirPrivacy int

	// KeyDir represents the map used by the bitcask.
	KeyDir map[string]recfmt.KeyDirRec
)

func NewKeyDir(dataStorePath string, privacy KeyDirPrivacy) (KeyDir, error) {
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
	dataStoreStat, err := os.Stat(dataStorePath)
	if err != nil {
		return false, err
	}

	keydirStat, err := os.Stat(path.Join(dataStorePath, "keydir"))
	if err != nil {
		return false, err
	}

	return keydirStat.ModTime().Before(dataStoreStat.ModTime()), nil
}

func (keyDir KeyDir) parseFiles(dataStorePath string, files map[string]fileType) error {
	for FileName, fType := range files {
		switch fType {
		case data:
			err := keyDir.parseDataFile(dataStorePath, FileName)
			if err != nil {
				return err
			}
		case hint:
			err := keyDir.parseHintFile(dataStorePath, FileName)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (keyDir KeyDir) parseDataFile(dataStorePath, fileName string) error {
	data, err := os.ReadFile(path.Join(dataStorePath, fileName))
	if err != nil {
		return err
	}

	n := len(data)
	for i := 0; i < n; {
		rec, recLen, err := recfmt.ExtractDataFileRec(data[i:])
		if err != nil {
			return err
		}

		old, exists := keyDir[rec.Key]
		if !exists || old.TStamp < rec.TStamp {
			keyDir[rec.Key] = recfmt.KeyDirRec{
				FileId:    fileName,
				ValuePos:  uint32(i),
				ValueSize: rec.ValueSize,
				TStamp:    rec.TStamp,
			}
		}
		i += int(recLen)
	}

	return nil
}

func (keyDir KeyDir) parseHintFile(dataStorePath, fileName string) error {
	data, err := os.ReadFile(path.Join(dataStorePath, fileName))
	if err != nil {
		return err
	}

	n := len(data)
	for i := 0; i < n; {
		key, rec, recLen := recfmt.ExtractHintFileRec(data[i:])
		rec.FileId = fmt.Sprintf("%s.data", strings.Trim(fileName, ".hint"))
		keyDir[key] = rec
		i += recLen
	}

	return nil
}

func categorizeFiles(allFiles []string) map[string]fileType {
	res := make(map[string]fileType)

	hintFiles := make(map[string]int)
	for _, file := range allFiles {
		if strings.HasSuffix(file, ".hint") {
			fileWithoutExt := strings.Trim(file, ".hint")
			hintFiles[fileWithoutExt] = 1
			res[file] = hint
		}
	}

	for _, file := range allFiles {
		if strings.HasSuffix(file, ".data") {
			if _, okay := hintFiles[strings.Trim(file, ".data")]; !okay {
				res[file] = data
			}
		}
	}

	return res
}
