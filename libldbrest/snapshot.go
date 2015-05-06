package libldbrest

import (
	"os"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

func makeSnap(destpath string) error {
	dest, err := leveldb.OpenFile(destpath, nil)
	if err != nil {
		return err
	}
	failed := false
	defer func() {
		dest.Close()
		if failed {
			os.RemoveAll(destpath)
		}
	}()

	snap, err := db.GetSnapshot()
	if err != nil {
		return err
	}
	defer snap.Release()

	iter := snap.NewIterator(nil, &opt.ReadOptions{
		DontFillCache: true,
	})
	defer iter.Release()

	batch := &leveldb.Batch{}
	var i uint
	for iter.First(); iter.Valid(); iter.Next() {
		batch.Put(iter.Key(), iter.Value())
		i++

		if i%1000 == 0 {
			err = dest.Write(batch, nil)
			if err != nil {
				failed = true
				return err
			}
			batch.Reset()
		}
	}

	if i%1000 != 0 {
		err = dest.Write(batch, nil)
		if err != nil {
			failed = true
			return err
		}
	}

	return nil
}
