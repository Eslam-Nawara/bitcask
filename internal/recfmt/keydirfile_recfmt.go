package recfmt

type KeydirRec struct {
	FileId    string
	ValuePos  uint32
	ValueSize uint32
	TStamp    int64
}
