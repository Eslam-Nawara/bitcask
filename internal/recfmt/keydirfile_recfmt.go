package recfmt

import (
	"encoding/binary"
	"strconv"
)

const keydirFileHdrSize = 26

type KeyDirRec struct {
	FileId    string
	ValuePos  uint32
	ValueSize uint32
	TStamp    int64
}

// CompressKeyDirRec compresses the given data into a keydir file record.
func CompressKeyDirRec(key string, rec KeyDirRec) []byte {
	keySize := len(key)
	buff := make([]byte, keydirFileHdrSize+keySize)
	fid, _ := strconv.ParseUint(rec.FileId, 10, 64)
	binary.LittleEndian.PutUint64(buff, fid)
	binary.LittleEndian.PutUint16(buff[8:], uint16(keySize))
	binary.LittleEndian.PutUint32(buff[10:], rec.ValueSize)
	binary.LittleEndian.PutUint32(buff[14:], rec.ValuePos)
	binary.LittleEndian.PutUint64(buff[18:], uint64(rec.TStamp))
	copy(buff[26:], []byte(key))

	return buff
}

// ExtractKeyDirRec extracts the keydir file record into a keydir record.
// Return the keydir record and its length in the file.
func ExtractKeyDirRec(buff []byte) (string, KeyDirRec, int) {
	fileId := strconv.FormatUint(binary.LittleEndian.Uint64(buff), 10)
	keySize := binary.LittleEndian.Uint16(buff[8:])
	valueSize := binary.LittleEndian.Uint32(buff[10:])
	valuePos := binary.LittleEndian.Uint32(buff[14:])
	tStamp := binary.LittleEndian.Uint64(buff[18:])
	key := string(buff[26 : keySize+26])

	return key, KeyDirRec{
		FileId:    fileId,
		ValuePos:  valuePos,
		ValueSize: valueSize,
		TStamp:    int64(tStamp),
	}, keydirFileHdrSize + int(keySize)
}
