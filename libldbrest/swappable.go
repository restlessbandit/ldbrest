package libldbrest

import (
	"net/http"
	"sync/atomic"
)

// An atomic.Value that deals specifically with http.Handlers, and which can act
// as an http.Handler itself by grabbing and running the currently held Handler.
type SwappableHandler struct {
	holder atomic.Value
}

func (sh *SwappableHandler) Store(handler http.Handler) {
	sh.holder.Store(&handlerWrapper{handler})
}

func (sh *SwappableHandler) Load() http.Handler {
	return sh.holder.Load().(http.Handler)
}

func (sh *SwappableHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sh.Load().ServeHTTP(w, r)
}

// For a given atomic.Value, only instances of the same type can be Store()d,
// and this specifically applies to the concrete type (they can't just be the
// same interface type). So this is a thin http.Handler wrapper that serves as
// the single concrete type for SwappableHandler's atomic.Value.
type handlerWrapper struct {
	http.Handler
}
