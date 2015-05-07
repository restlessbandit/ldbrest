package libldbrest

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
)

type oplist []*struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

var errBadBatch = errors.New("bad write batch")

func applyBatch(ops oplist) error {
	batch := &leveldb.Batch{}

	for _, op := range ops {
		switch op.Op {
		case "put":
			batch.Put([]byte(op.Key), []byte(op.Value))
		case "delete":
			batch.Delete([]byte(op.Key))
		default:
			return errBadBatch
		}
	}

	return db.Write(batch, nil)
}
