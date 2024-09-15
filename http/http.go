// Package http is middleware for net/http
package http

import (
	"net/http"
	"sync/atomic"
	"time"

	"github.com/matsuu/middleware-parquetlogger/common"
)

// A Logger defines parameters for logging.
type Logger struct {
	common.Logger
}

type myResponseWriter struct {
	http.ResponseWriter
	status int
	size   int64
}

func (mw *myResponseWriter) Write(buf []byte) (int, error) {
	n, err := mw.ResponseWriter.Write(buf)
	atomic.AddInt64(&mw.size, int64(n))
	return n, err
}
func (mw *myResponseWriter) WriteHeader(code int) {
	mw.status = code
	mw.ResponseWriter.WriteHeader(code)
}

func (mw *myResponseWriter) Status() int {
	return mw.status
}

func (mw *myResponseWriter) Size() int64 {
	return atomic.LoadInt64(&mw.size)
}

// Middleware returns logger middleware.
func (pl *Logger) Middleware(next http.Handler) http.Handler {
	now := time.Now
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Before
		start := now()
		mw := &myResponseWriter{
			ResponseWriter: w,
		}

		// Next
		next.ServeHTTP(mw, r)

		// After
		latency := now().Sub(start)
		row := common.RowType{
			StartTime:     start,
			Latency:       latency,
			Protocol:      r.Proto,
			RemoteAddr:    r.RemoteAddr,
			Host:          r.Host,
			Method:        r.Method,
			URL:           r.URL.String(),
			Pattern:       r.Pattern,
			Referer:       r.Referer(),
			UserAgent:     r.UserAgent(),
			Status:        mw.Status(),
			ContentLength: r.ContentLength,
			ResponseSize:  mw.Size(),
			RequestHeader: r.Header,
		}
		pl.Send(row)
	})
}
