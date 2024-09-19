// Package gin is middleware for Gin Web Framework
package gin

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/gin-gonic/gin"
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
func (pl *Logger) Middleware() gin.HandlerFunc {
	now := time.Now
	return func(c *gin.Context) {
		// Before
		start := now()
		// Next
		c.Next()

		// After
		latency := now().Sub(start)
		row := RowType{
			StartTime:       start,
			Latency:         latency,
			Protocol:        c.Request.Proto,
			RemoteAddr:      c.Request.RemoteAddr,
			Host:            c.Request.Host,
			Method:          c.Request.Method,
			URL:             c.Request.URL.String(),
			Pattern:         c.FullPath(),
			Status:          c.Writer.Status(),
			RequestSize:     c.Request.ContentLength,
			ResponseSize:    int64(c.Writer.Size()),
			RequestHeaders:  c.Request.Header,
			ResponseHeaders: c.Writer.Header(),
		}
		pl.send(row)
	}
}
