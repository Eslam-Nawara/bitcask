package recfmt

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const dataFileHdrSize = 18

var errDataCorruption = errors.New("corrution detected: datastore files are corrupted")

type DataFileRec struct {
	Key       string
	Value     string
	TStamp    int64
	KeySize   uint16
	ValueSize uint32
}

func CompressDataFileRec(key, value string, tStamp int64) []byte {
	buff := make([]byte, dataFileHdrSize+len(key)+len(value))

	binary.LittleEndian.PutUint64(buff[4:], uint64(tStamp))
	binary.LittleEndian.PutUint16(buff[12:], uint16(len(key)))
	binary.LittleEndian.PutUint32(buff[14:], uint32(len(value)))
	copy(buff[dataFileHdrSize:], []byte(key))
	copy(buff[dataFileHdrSize+len(key):], []byte(value))

	checkSum := crc32.ChecksumIEEE(buff[4:])
	binary.LittleEndian.PutUint32(buff, checkSum)

	return buff
}

func ExtractDataFileRec(buff []byte) (*DataFileRec, uint32, error) {
	parsedSum := binary.LittleEndian.Uint32(buff)
	tStamp := binary.LittleEndian.Uint64(buff[4:])
	keySize := binary.LittleEndian.Uint16(buff[12:])
	valueSize := binary.LittleEndian.Uint32(buff[14:])
	key := string(buff[dataFileHdrSize : dataFileHdrSize+keySize])
	valueOffset := uint32(dataFileHdrSize + keySize)
	value := string(buff[valueOffset : valueOffset+valueSize])

	err := validateCheckSum(parsedSum, buff[4:dataFileHdrSize+uint32(keySize)+valueSize])
	if err != nil {
		return nil, 0, err
	}

	return &DataFileRec{
		Key:       key,
		Value:     value,
		TStamp:    int64(tStamp),
		KeySize:   keySize,
		ValueSize: valueSize,
	}, dataFileHdrSize + valueSize + uint32(keySize), nil
}

// validateCheckSum runs the validate check on the data.
// return an error if the data is corrupted.
func validateCheckSum(parsedSum uint32, rec []byte) error {
	wantedSum := crc32.ChecksumIEEE(rec)
	if parsedSum != wantedSum {
		return errDataCorruption
	}

	return nil
}
