package libldbrest

import (
	"log"

	"github.com/syndtr/goleveldb/leveldb"
)

var (
	db *leveldb.DB
)

// OpenDB intializes global vars for the leveldb database.
// Be sure and call CleanupDB() to free those resources.
func OpenDB(dbpath string) {
	var err error
	db, err = leveldb.OpenFile(dbpath, nil)
	if err != nil {
		log.Fatalf("opening leveldb: %s", err)
	}
}

// CleanupDB frees the global vars associated with the open leveldb.
func CleanupDB() {
	db.Close()
	db = nil
}
