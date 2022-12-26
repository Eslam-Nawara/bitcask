package recfmt

import (
	"encoding/binary"
)

const hintFileHdrSize = 18

// type HintFileRec struct {
// 	key       string
// 	keySize   uint16
// 	tStamp    int64
// 	valuePos  uint32
// 	valueSize uint32
// }

func CompressHintFileRec(key string, rec KeyDirRec) []byte {
	buff := make([]byte, hintFileHdrSize+len(key))
	binary.LittleEndian.PutUint64(buff, uint64(rec.TStamp))
	binary.LittleEndian.PutUint16(buff[8:], uint16(len(key)))
	binary.LittleEndian.PutUint32(buff[10:], rec.ValueSize)
	binary.LittleEndian.PutUint32(buff[14:], rec.ValuePos)
	copy(buff[18:], []byte(key))
	return buff
}

func ExtractHintFileRec(buff []byte) (string, KeyDirRec, int) {
	tStamp := binary.LittleEndian.Uint64(buff)
	keySize := binary.LittleEndian.Uint16(buff[8:])
	valueSize := binary.LittleEndian.Uint32(buff[10:])
	valuePos := binary.LittleEndian.Uint32(buff[14:])
	key := string(buff[hintFileHdrSize : hintFileHdrSize+keySize])

	return key, KeyDirRec{
		ValuePos:  valuePos,
		ValueSize: valueSize,
		TStamp:    int64(tStamp),
	}, hintFileHdrSize + int(keySize)
}
