package libldbrest

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strconv"

	"github.com/julienschmidt/httprouter"
)

const (
	ABSMAX = 1000
)

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
	b, err := db.Get(ro, []byte(p.ByName("name")[1:]))
	if err != nil {
		failErr(w, err)
	} else if b == nil {
		failCode(w, http.StatusNotFound)
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(b)
	}
}

// set single keys (value goes in the body)
func setItem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	buf := &bytes.Buffer{}
	if _, err := io.Copy(buf, r.Body); err != nil {
		failErr(w, err)
		return
	}

	err := db.Put(wo, []byte(p.ByName("name")[1:]), buf.Bytes())
	if err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// delete a key by name
func deleteItem(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	err := db.Delete(wo, []byte(p.ByName("name")[1:]))
	if err != nil {
		failErr(w, err)
	} else {
		w.WriteHeader(http.StatusNoContent)
	}
}

// retrieve a given set of keys
// (must be a POST to accept a request body, but we aren't changing server-side data)
func getItems(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct{ Keys []string }{}

	err := json.NewDecoder(r.Body).Decode(req)
	if err != nil {
		failErr(w, err)
		return
	}

	results := make(map[string]string, len(req.Keys))
	for _, key := range req.Keys {
		val, err := db.Get(ro, []byte(key))
		if err != nil {
			failErr(w, err)
			return
		} else if val != nil {
			results[key] = string(val)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
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

	// by default we traverse forwards,
	// include "start" but not "end" (like go slicing),
	// and include values in the response data
	ignore_start := q.Get("include_start") == "no"
	include_end := q.Get("include_end") == "yes"
	backwards := q.Get("forward") == "no"
	skip_values := q.Get("include_values") == "no"

	type keyval struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type wrapper struct {
		More bool          `json:"more"`
		Data []interface{} `json:"data"` // either keyvals or just string keys
	}

	var (
		data = make([]interface{}, 0)
		more bool
	)

	var once func([]byte, []byte) error
	if skip_values {
		once = func(key, value []byte) error {
			data = append(data, string(key))
			return nil
		}
	} else {
		once = func(key, value []byte) error {
			data = append(data, &keyval{string(key), string(value)})
			return nil
		}
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
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(&wrapper{more, data})
}

// atomically write a batch of updates
func batchSetItems(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct{ Ops oplist }{}

	err := json.NewDecoder(r.Body).Decode(req)
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
	prop := db.PropertyValue(p.ByName("name"))
	if prop == "" {
		failCode(w, http.StatusNotFound)
	} else {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(prop))
	}
}

// copy the whole db via a point-in-time snapshot
func makeLDBSnapshot(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	req := &struct {
		Destination string
	}{}
	err := json.NewDecoder(r.Body).Decode(req)
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
