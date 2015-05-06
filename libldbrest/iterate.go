package libldbrest

import (
	"bytes"

	"github.com/syndtr/goleveldb/leveldb/opt"
)

func iterate(start []byte, include_start, backwards bool, handle func([]byte, []byte) (bool, error)) error {
	iter := db.NewIterator(
		nil,
		&opt.ReadOptions{
			DontFillCache: true,
		},
	)

	if bytes.Equal(start, []byte{}) {
		if backwards {
			iter.Last()
		} else {
			iter.First()
		}
	} else {
		iter.Seek(start)
	}

	var proceed func() bool

	if backwards {
		proceed = iter.Prev

		// Iterator.Seek() seeks to the first key >= its argument, but going
		// backwards we need the last key <= the arg, so adjust accordingly
		if !iter.Valid() {
			iter.Last()
		} else if !include_start && !bytes.Equal(iter.Key(), start) {
			iter.Prev()
		}
	} else {
		proceed = iter.Next
	}

	first := true

	for ; iter.Valid(); proceed() {
		if first && !include_start && bytes.Equal(iter.Key(), start) {
			first = false
			continue
		}
		first = false

		stop, err := handle(iter.Key(), iter.Value())
		if err != nil {
			return err
		}

		if stop {
			return nil
		}
	}

	return nil
}

func iterateUntil(start, end []byte, max int, include_start, include_end, backwards bool, handle func([]byte, []byte) error) (bool, error) {
	var (
		i    int
		more bool
	)

	oob := func(key []byte) (bool, bool) { // returns (valid_now, check_more)
		cmp := bytes.Compare(key, end)
		switch {
		case cmp == 0:
			return include_end, false
		case cmp == -1 && backwards || cmp == 1 && !backwards:
			return false, false
		default:
			return true, true
		}
	}

	err := iterate(start, include_start, backwards, func(key, value []byte) (bool, error) {
		if i >= max {
			// exceeded max count, indicate if there's more before "end"
			more, _ = oob(key)
			return true, nil
		}
		i++

		valid, next := oob(key)
		if !valid {
			return true, nil
		}

		if err := handle(key, value); err != nil {
			return true, err
		}

		return !next, nil
	})

	return more, err
}

func iterateN(start []byte, max int, include_start, backwards bool, handle func([]byte, []byte) error) error {
	var i int
	return iterate(start, include_start, backwards, func(key, value []byte) (bool, error) {
		if i >= max {
			return true, nil
		}
		i++
		return false, handle(key, value)
	})
}
