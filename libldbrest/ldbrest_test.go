package libldbrest

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/ugorji/go/codec"
)

func TestMultiGet(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	k1 := "1"
	k2 := "2"

	app := newAppTester(t)
	app.put(k1, k1)
	app.put(k2, k2)

	expectNItems := 2
	itemMap := app.multiGet([]string{k1, k2})
	if len(itemMap) != expectNItems {
		t.Fatalf("Expected len(itemMap) to be %d, got %d\n", expectNItems, len(itemMap))
	}

	if itemMap[k1] != k1 {
		t.Fatalf("Expected itemMap[%s] to be '%s', got '%s'\n", k1, k1, itemMap[k1])
	}

	if itemMap[k2] != k2 {
		t.Fatalf("Expected itemMap[%s] to be '%s', got '%s'\n", k2, k2, itemMap[k2])
	}
}

func TestMultiGetMissingKey(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	k1 := "1"
	k2 := "2"

	kMissing := "3"

	app := newAppTester(t)
	app.put(k1, k1)
	app.put(k2, k2)

	expectNItems := 2
	itemMap := app.multiGet([]string{k1, k2, kMissing})
	if len(itemMap) != expectNItems {
		t.Fatalf("Expected len(itemMap) to be %d, got %d\n", expectNItems, len(itemMap))
	}

	if itemMap[k1] != k1 {
		t.Fatalf("Expected itemMap[%s] to be '%s', got '%s'\n", k1, k1, itemMap[k1])
	}

	if itemMap[k2] != k2 {
		t.Fatalf("Expected itemMap[%s] to be '%s', got '%s'\n", k2, k2, itemMap[k2])
	}

	if val, ok := itemMap[kMissing]; ok {
		t.Fatalf("Expected itemMap[%s] to not exist, got '%s'\n", kMissing, val)
	}
}

func TestKeyPutGet(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)

	app.put("foo", "bar")
	val := app.get("foo")

	if val != "bar" {
		t.Fatalf("wrong 'foo' value: %s", val)
	}

	found, _ := app.maybeGet("baz")
	if found {
		t.Fatal("found 'baz' when we shouldn't have")
	}
}

func TestDelete(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)

	app.put("a", "A")

	if !app.del("a") {
		t.Fatal("failed to DELETE existing key")
	}

	if !app.del("b") {
		t.Fatal("failed to DELETE non-existing key")
	}

	found, _ := app.maybeGet("a")
	if found {
		t.Fatal("DELETE didn't remove a key")
	}
}

func TestIteration(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)

	app.put("a", "A")
	app.put("b", "B")
	app.put("c", "C")
	app.put("d", "D")

	/*
		[b, d)
	*/
	rr := app.doReq("GET", "http://domain/iterate?start=b&include_start=yes&end=d", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kresp := &multiResponseMore{}
	if err := codec.NewDecoder(rr.Body, msgpack).Decode(kresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kresp.Data) == 2, "wrong # of returned keys: %d", len(kresp.Data))
	assert(t, kresp.Data[0].Key == "b", "wrong returned key: %s", kresp.Data[0])
	assert(t, kresp.Data[1].Key == "c", "wrong returned key: %s", kresp.Data[1])
	assert(t, !*kresp.More, "ldbrest falsely reporting 'more'")

	/*
		keys and vals [0, 2)
	*/
	rr = app.doReq("GET", "http://domain/iterate?max=2", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kvresp := &multiResponseMore{}
	if err := codec.NewDecoder(rr.Body, msgpack).Decode(kvresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kvresp.Data) == 2, "wrong # of keyvals: %d", len(kvresp.Data))
	assert(t, kvresp.Data[0].Key == "a", "wrong first key: %s", kvresp.Data[0].Key)
	assert(t, kvresp.Data[0].Value == "A", "wrong first value: %s", kvresp.Data[0].Value)
	assert(t, kvresp.Data[1].Key == "b", "wrong second key: %s", kvresp.Data[1].Key)
	assert(t, kvresp.Data[1].Value == "B", "wrong second value: %s", kvresp.Data[1].Value)
	assert(t, !*kvresp.More, "ldbrest falsely reporting 'more'")

	/*
		keys and vals [a, d] with max 3 (trigger 'more')
	*/
	rr = app.doReq("GET", "http://domain/iterate?start=a&end=d&include_end=yes&max=3", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	*kvresp.More = false
	kvresp.Data = nil
	if err := codec.NewDecoder(rr.Body, msgpack).Decode(kvresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kvresp.Data) == 3, "wrong # of keyvals: %d", len(kvresp.Data))
	assert(t, *kvresp.More, "'more' should be true")
	assert(t, kvresp.Data[0].Key == "a", "wrong data[0].Key: %s", kvresp.Data[0].Key)
	assert(t, kvresp.Data[1].Key == "b", "wrong data[1].Key: %s", kvresp.Data[1].Key)
	assert(t, kvresp.Data[2].Key == "c", "wrong data[2].Key: %s", kvresp.Data[2].Key)
	assert(t, kvresp.Data[0].Value == "A", "wrong data[0].Value: %s", kvresp.Data[0].Value)
	assert(t, kvresp.Data[1].Value == "B", "wrong data[1].Value: %s", kvresp.Data[1].Value)
	assert(t, kvresp.Data[2].Value == "C", "wrong data[2].Value: %s", kvresp.Data[2].Value)

	/*
		keys only [d, a] in reverse with max 2 (trigger 'more')
	*/
	rr = app.doReq("GET", "http://domain/iterate?start=d&forward=no&max=2&end=a&include_end=yes", "")
	if rr.Code != 200 {
		t.Fatalf("bad GET /iterate response: %d", rr.Code)
	}
	kresp.More = nil
	kresp.Data = nil
	if err := codec.NewDecoder(rr.Body, msgpack).Decode(kresp); err != nil {
		t.Fatal(err)
	}
	assert(t, len(kresp.Data) == 2, "wrong # of keys: %d", len(kresp.Data))
	assert(t, *kresp.More, "'more' should be true (reverse)")
	assert(t, kresp.Data[0].Key == "d", "wrong data[0]: %s", kresp.Data[0])
	assert(t, kresp.Data[1].Key == "c", "wrong data[1]: %s", kresp.Data[1])
}

func TestBatch(t *testing.T) {
	dbpath := setup(t)
	defer cleanup(dbpath)

	app := newAppTester(t)
	app.put("foo", "bar")

	if !app.batch(oplist{
		{"put", "a", "A"},
		{"put", "b", "B"},
		{"delete", "foo", ""},
	}) {
		t.Fatal("batch call failed")
	}

	if app.get("a") != "A" || app.get("b") != "B" {
		t.Fatal("puts in the batch didn't go through")
	}

	if found, _ := app.maybeGet("foo"); found {
		t.Fatal("delete in the batch didn't go through")
	}
}

func setup(tb testing.TB) string {
	dirpath, err := ioutil.TempDir("", "ldbrest_test")
	if err != nil {
		tb.Fatal(err)
	}

	db, err = leveldb.OpenFile(dirpath, &opt.Options{
		ErrorIfExist: true,
	})
	if err != nil {
		os.RemoveAll(dirpath)
		tb.Fatal(err)
	}

	return dirpath
}

func cleanup(path string) {
	if db != nil {
		db.Close()
	}
	os.RemoveAll(path)
}

func assert(tb testing.TB, cond bool, msg string, args ...interface{}) {
	if !cond {
		tb.Fatalf(msg, args...)
	}
}

type appTester struct {
	app http.Handler
	tb  testing.TB
}

func newAppTester(tb testing.TB) *appTester {
	return &appTester{app: InitRouter(""), tb: tb}
}

func (app *appTester) doReq(method, url, body string) *httptest.ResponseRecorder {
	var bodyReader io.Reader
	if body == "" {
		bodyReader = nil
	} else {
		bodyReader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		app.tb.Fatal(err)
	}

	rr := httptest.NewRecorder()
	app.app.ServeHTTP(rr, req)
	rr.Flush()
	return rr
}

func (app *appTester) put(key, value string) {
	rr := app.doReq("PUT", fmt.Sprintf("http://domain/key/%s", key), value)
	if rr.Code != 204 {
		app.tb.Fatalf("non-204 PUT /key/X response: %d", rr.Code)
	}
}

func (app *appTester) maybeGet(key string) (bool, string) {
	rr := app.doReq("GET", fmt.Sprintf("http://domain/key/%s", key), "")

	switch rr.Code {
	case http.StatusNotFound:
		return false, ""
	case http.StatusOK:
		ct := rr.HeaderMap.Get("Content-Type")
		if ct != msgpackCType {
			app.tb.Fatalf("non 'application/msgpack' 200 GET /key/%s response: %s", key, ct)
		}
	default:
		app.tb.Fatalf("questionable GET /key/%s response: %d", key, rr.Code)
	}

	req := &keyval{}
	err := codec.NewDecoder(rr.Body, msgpack).Decode(req)
	if err != nil {
		app.tb.Fatalf("bad msgpack GET /key/%s", key)
	}

	return true, req.Value
}

func (app *appTester) get(key string) string {
	found, value := app.maybeGet(key)
	if !found {
		app.tb.Fatalf("failed to find key %s", key)
	}
	return value
}

func (app *appTester) multiGet(keys []string) map[string]string {
	reqBody := map[string][]string{
		"Keys": keys,
	}

	bytesOut := make([]byte, 0)
	err := codec.NewEncoderBytes(&bytesOut, msgpack).Encode(reqBody)
	if err != nil {
		app.tb.Fatalf("Error: msgpack marshal: %s\n  request body was: %#v\n", err.Error(), reqBody)
	}

	rr := app.doReq("POST", "http://domain/keys", string(bytesOut))

	if rr.Code == http.StatusOK {
		ct := rr.HeaderMap.Get("Content-Type")
		if ct != "application/msgpack" {
			app.tb.Fatalf("non 'application/msgpack' 200 POST /keys response: %s\n  keys: %v\n", ct, keys)
		}
	} else {
		app.tb.Fatalf("questionable GET /keys, keys: %v, response: %d", keys, rr.Code)
	}

	items := &multiResponse{}
	err = codec.NewDecoderBytes(rr.Body.Bytes(), msgpack).Decode(items)
	if err != nil {
		app.tb.Fatalf("Error: msgpack unmarshal: %s\n  keys: %v\n  response body: %s", err.Error(), keys, rr.Body.String())
	}

	results := map[string]string{}
	for _, kv := range items.Data {
		results[kv.Key] = kv.Value
	}

	return results
}

func (app *appTester) del(key string) bool {
	rr := app.doReq("DELETE", fmt.Sprintf("http://domain/key/%s", key), "")
	return rr.Code == 204
}

func (app *appTester) batch(ops oplist) bool {
	bytesOut := make([]byte, 0)

	err := codec.NewEncoderBytes(&bytesOut, msgpack).Encode(struct {
		Ops oplist `json:"ops"`
	}{ops})
	if err != nil {
		app.tb.Fatalf("json ops Marshal: %v", err)
	}

	rr := app.doReq("POST", "http://domain/batch", string(bytesOut))
	return rr.Code == 204
}
