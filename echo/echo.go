// Package echo is middleware for echo framework
package echo

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// RowType contains extracted values from logger.
type RowType struct {
	StartTime       time.Time           `parquet:",delta"`
	Latency         time.Duration       `parquet:",delta"`
	Protocol        string              `parquet:",dict"`
	RemoteAddr      string              `parquet:",dict"`
	Host            string              `parquet:",dict"`
	Method          string              `parquet:",dict"`
	URL             string              `parquet:",dict"`
	Pattern         string              `parquet:",dict"`
	Status          int                 `parquet:",dict"`
	RequestSize     int64               `parquet:",delta"`
	ResponseSize    int64               `parquet:",delta"`
	RequestHeaders  map[string][]string `parquet:","`
	ResponseHeaders map[string][]string `parquet:","`
	Error           *string             `parquet:","`
}

// A Logger defines parameters for logging.
type Logger struct {
	ch       chan RowType
	exportCh chan string
	doneCh   chan error
}

// NewLogger returns a new Logger.
func NewLogger() *Logger {
	pl := &Logger{
		ch:     make(chan RowType, 64),
		doneCh: make(chan error),
	}
	go func() {
		pl.doneCh <- pl.run()
	}()
	return pl
}

func (pl *Logger) run() error {
	if pl.ch == nil {
		log.Fatal("No channel is defined in Logger. Please use NewLogger")
	}
	f, err := os.CreateTemp("", ".parquet-logger-*.parquet")
	if err != nil {
		log.Fatalf("Failed to create tempfile: %v", err)
	}
	os.Remove(f.Name())
	w := parquet.NewGenericWriter[RowType](f, parquet.Compression(parquet.LookupCompressionCodec(format.Snappy)))

	pl.exportCh = make(chan string)
	var filename string

L:
	for {
		select {
		case row, ok := <-pl.ch:
			if !ok {
				break L
			}
			if _, err := w.Write([]RowType{row}); err != nil {
				log.Printf("Failed to write parquet: %v", err)
			}
		case filename = <-pl.exportCh:
			break L
		}
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("Failed to close parquet writer: %w", err)
	}
	if filename != "" {
		out, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("Failed to create %s: %w", filename, err)
		}
		if err := f.Sync(); err != nil {
			return fmt.Errorf("Failed to sync tempfile: %w", err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("Failed to seek tempfile: %w", err)
		}
		if _, err := io.Copy(out, f); err != nil {
			return fmt.Errorf("Failed to copy from %s to %s: %v", f.Name(), out.Name(), err)
		}
		if err := out.Close(); err != nil {
			return fmt.Errorf("Failed to close %s: %w", out.Name(), err)
		}
		log.Printf("Succeed to export %s", filename)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("Failed to close tempfile: %w", err)
	}
	return nil
}

// Export exports parquet file.
func (pl *Logger) Export(filename string) error {
	pl.exportCh <- filename
	go func() {
		pl.doneCh <- pl.run()
	}()
	return <-pl.doneCh
}

func (pl *Logger) send(row RowType) {
	select {
	case pl.ch <- row:
	default:
		log.Printf("Failed to add to channel: Capacity limit reached. Consider increasing the channel size.")
	}
}

// Middleware returns logger middleware.
func (pl *Logger) Middleware() echo.MiddlewareFunc {
	now := time.Now
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Before
			req := c.Request()
			res := c.Response()
			start := now()

			// Next
			err := next(c)

			//After
			latency := now().Sub(start)

			row := RowType{
				StartTime:       start,
				Latency:         latency,
				Protocol:        req.Proto,
				RemoteAddr:      c.RealIP(),
				Host:            req.Host,
				Method:          req.Method,
				URL:             req.RequestURI,
				Pattern:         c.Path(),
				Status:          res.Status,
				RequestSize:     req.ContentLength,
				ResponseSize:    res.Size,
				RequestHeaders:  req.Header,
				ResponseHeaders: res.Header(),
			}
			if err != nil {
				var httpErr *echo.HTTPError
				if errors.As(err, &httpErr) {
					row.Status = httpErr.Code
					errStr := fmt.Sprintf("%v", httpErr.Message)
					row.Error = &errStr
				} else {
					errStr := err.Error()
					row.Error = &errStr
				}
			}
			pl.send(row)
			return err
		}
	}
}
