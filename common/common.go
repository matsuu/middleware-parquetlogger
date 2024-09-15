// Package common is common parquet writer
package common

import (
	"io"
	"log"
	"os"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// RowType contains extracted values from logger.
type RowType struct {
	StartTime     time.Time           `parquet:",delta"`
	Latency       time.Duration       `parquet:",delta"`
	Protocol      string              `parquet:",dict"`
	RemoteAddr    string              `parquet:",dict"`
	Host          string              `parquet:",dict"`
	Method        string              `parquet:",dict"`
	URL           string              `parquet:",dict"`
	Pattern       string              `parquet:",dict"`
	Referer       string              `parquet:",dict"`
	UserAgent     string              `parquet:",dict"`
	Status        int                 `parquet:",dict"`
	ContentLength int64               `parquet:",delta"`
	ResponseSize  int64               `parquet:",delta"`
	RequestHeader map[string][]string `parquet:","`
}

// A Logger defines parameters for logging.
type Logger struct {
	ch       chan RowType
	exportCh chan string
}

// NewLogger returns a new Logger.
func NewLogger() *Logger {
	pl := &Logger{
		ch: make(chan RowType, 64),
	}
	go pl.run()
	return pl
}

func (pl *Logger) run() {
	if pl.ch == nil {
		log.Fatal("No channel is defined in Logger. Please use NewLogger")
	}
	f, err := os.CreateTemp("", ".parquet-logger-")
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
		log.Fatalf("Failed to close parquet writer: %v", err)
	}
	if filename != "" {
		out, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Failed to create %s: %w", filename, err)
		}
		if err := f.Sync(); err != nil {
			log.Fatalf("Failed to sync tempfile: %w", err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			log.Fatalf("Failed to seek tempfile: %w", err)
		}
		if _, err := io.Copy(out, f); err != nil {
			log.Fatalf("Failed to copy from %s to %s: %v", f.Name(), out.Name(), err)
		}
		if err := out.Close(); err != nil {
			log.Fatalf("Failed to close %s: %v", out.Name(), err)
		}
		log.Printf("Succeed to export %s", filename)
	}
	if err := f.Close(); err != nil {
		log.Fatalf("Failed to close tempfile: %w", err)
	}
}

// Export exports parquet file.
func (pl *Logger) Export(filename string) {
	pl.exportCh <- filename
	go pl.run()
}

func (pl *Logger) Send(row RowType) {
	select {
	case pl.ch <- row:
	default:
		log.Printf("Failed to add to channel: Capacity limit reached. Consider increasing the channel size.")
	}
}
