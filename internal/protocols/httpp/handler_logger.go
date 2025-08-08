package httpp

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"

	"github.com/bluenviron/mediamtx/internal/logger"
)

type loggerWriter struct {
	w      http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (w *loggerWriter) Header() http.Header {
	return w.w.Header()
}

func (w *loggerWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	w.buf.Write(b)
	return w.w.Write(b)
}

func (w *loggerWriter) WriteHeader(statusCode int) {
	w.status = statusCode
	w.w.WriteHeader(statusCode)
}

// Hijack implements http.Hijacker by delegating to the underlying writer.
func (w *loggerWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if h, ok := w.w.(http.Hijacker); ok {
		return h.Hijack()
	}
	return nil, nil, fmt.Errorf("hijacker not supported")
}

// Flush implements http.Flusher by delegating to the underlying writer.
func (w *loggerWriter) Flush() {
	if f, ok := w.w.(http.Flusher); ok {
		f.Flush()
	}
}

// CloseNotify implements http.CloseNotifier by delegating to the underlying writer.
// Deprecated: http.CloseNotifier is deprecated, but some stacks still use it.
func (w *loggerWriter) CloseNotify() <-chan bool {
	if cn, ok := w.w.(http.CloseNotifier); ok {
		return cn.CloseNotify()
	}
	ch := make(chan bool)
	close(ch)
	return ch
}

// Push implements http.Pusher by delegating to the underlying writer.
func (w *loggerWriter) Push(target string, opts *http.PushOptions) error {
	if p, ok := w.w.(http.Pusher); ok {
		return p.Push(target, opts)
	}
	return http.ErrNotSupported
}

func (w *loggerWriter) dump() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %d %s\n", "HTTP/1.1", w.status, http.StatusText(w.status))
	w.w.Header().Write(&buf) //nolint:errcheck
	buf.Write([]byte("\n"))
	if w.buf.Len() > 0 {
		fmt.Fprintf(&buf, "(body of %d bytes)", w.buf.Len())
	}
	return buf.String()
}

// log requests and responses.
type handlerLogger struct {
	http.Handler
	log logger.Writer
}

func (h *handlerLogger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	byts, _ := httputil.DumpRequest(r, true)
	h.log.Log(logger.Debug, "[conn %v] [c->s] %s", r.RemoteAddr, string(byts))

	logw := &loggerWriter{w: w}

	h.Handler.ServeHTTP(logw, r)

	h.log.Log(logger.Debug, "[conn %v] [s->c] %s", r.RemoteAddr, logw.dump())
}
