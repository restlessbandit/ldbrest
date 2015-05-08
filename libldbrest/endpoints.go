package libldbrest

import (
	"bytes"
	"io"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/ugorji/go/codec"
)

const (
	ABSMAX       = 1000
	msgpackCType = "application/msgpack"
)

var msgpack = &codec.MsgpackHandle{}

// InitRouter creates an *httprouter.Router and sets the endpoints to run the
// ldbrest server
func InitRouter(prefix string) *httprouter.Router {
	router := &httprouter.Router{
		// precision in urls -- I'd rather know when my client is wrong
		RedirectTrailingSlash: false,
		RedirectFixedPath:     false,

		HandleMethodNotAllowed: true,
		PanicHandler:           handlePanics,
	}

	router.GET(prefix+"/key/*name", getItem)
	router.PUT(prefix+"/key/*name", setItem)
	router.DELETE(prefix+"/key/*name", deleteItem)

	router.POST(prefix+"/keys", getItems)
	router.GET(prefix+"/iterate", iterItems)
	router.POST(prefix+"/batch", batchSetItems)

	router.GET(prefix+"/property/:name", getLDBProperty)
	router.POST(prefix+"/snapshot", makeLDBSnapshot)

	return router
}

// retrieve single keys
func getItem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	key := p.ByName("name")[1:]
	val, err := db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		failCode(w, http.StatusNotFound)
	} else if err != nil {
		failErr(w, err)
	} else {
		w.Header().Set("Content-Type", msgpackCType)
		codec.NewEncoder(w, msgpack).Encode(keyval{key, string(val)})
	}
}

// set single keys (value goes in the body)
func setItem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, r.Body); err != nil {
		failErr(w, err)
		return
	}

	err := db.Put([]byte(p.ByName("name")[1:]), buf.Bytes(), nil)
	if err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// delete a key by name
func deleteItem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := db.Delete([]byte(p.ByName("name")[1:]), nil)
	if err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// retrieve a given set of keys
// (must be a POST to accept a request body, but we aren't changing server-side data)
func getItems(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct {
		Keys []string `json:"keys" codec:"keys"`
	}{}

	err := codec.NewDecoder(r.Body, msgpack).Decode(req)
	if err != nil {
		failErr(w, err)
		return
	}

	results := make([]*keyval, 0, len(req.Keys))
	for _, key := range req.Keys {
		val, err := db.Get([]byte(key), nil)
		if err == leveldb.ErrNotFound {
			continue
		} else if err != nil {
			failErr(w, err)
			return
		} else if val != nil {
			results = append(results, &keyval{key, string(val)})
		}
	}

	w.Header().Set("Content-Type", msgpackCType)
	codec.NewEncoder(w, msgpack).Encode(multiResponse{nil, results})
}

// fetch a contiguous range of keys and their values
func iterItems(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	q := r.URL.Query()
	start := q.Get("start")
	end := q.Get("end")

	var (
		max int
		err error
	)
	maxs := q.Get("max")
	if maxs == "" {
		max = ABSMAX
	} else if max, err = strconv.Atoi(maxs); err != nil {
		failErr(w, err)
		return
	}
	if max > ABSMAX {
		max = ABSMAX
	}

	// by default we traverse forwards and
	// include "start" but not "end" (like go slicing)
	ignore_start := q.Get("include_start") == "no"
	include_end := q.Get("include_end") == "yes"
	backwards := q.Get("forward") == "no"

	var (
		data = make([]*keyval, 0)
		more bool
	)

	var once func([]byte, []byte) error
	once = func(key, value []byte) error {
		data = append(data, &keyval{string(key), string(value)})
		return nil
	}

	if end == "" {
		err = iterateN([]byte(start), max, !ignore_start, backwards, once)
		more = false
	} else {
		more, err = iterateUntil([]byte(start), []byte(end), max, !ignore_start, include_end, backwards, once)
	}

	if err != nil {
		failErr(w, err)
		return
	}
	w.Header().Set("Content-Type", msgpackCType)
	codec.NewEncoder(w, msgpack).Encode(&multiResponse{&more, data})
}

// atomically write a batch of updates
func batchSetItems(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct {
		Ops oplist `json:"ops" codec:"ops"`
	}{}

	err := codec.NewDecoder(r.Body, msgpack).Decode(req)
	if err != nil {
		failErr(w, err)
		return
	}

	err = applyBatch(req.Ops)
	if err == errBadBatch {
		failCode(w, http.StatusBadRequest)
	} else if err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// get a leveldb property
func getLDBProperty(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	name := p.ByName("name")
	prop, err := db.GetProperty(name)
	if err == leveldb.ErrNotFound {
		failCode(w, http.StatusNotFound)
	} else if err != nil {
		failErr(w, err)
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(prop))
	}
}

// copy the whole db via a point-in-time snapshot
func makeLDBSnapshot(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct {
		Destination string `json:"destination" codec:"destination"`
	}{}
	err := codec.NewDecoder(r.Body, msgpack).Decode(req)
	if err != nil {
		failErr(w, err)
		return
	}

	if err := makeSnap(req.Destination); err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

type keyval struct {
	Key   string `json:"key" codec:"key"`
	Value string `json:"value" codec:"value"`
}

type multiResponse struct {
	More *bool     `json:"more,omitempty" codec:"more,omitempty"`
	Data []*keyval `json:"data" codec:"data"`
}
