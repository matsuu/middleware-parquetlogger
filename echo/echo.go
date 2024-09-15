// Package echo is middleware for echo framework
package echo

import (
	"errors"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/matsuu/middleware-parquetlogger/common"
)

// A Logger defines parameters for logging.
type Logger struct {
	common.Logger
}

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

			row := common.RowType{
				StartTime:     start,
				Latency:       latency,
				Protocol:      req.Proto,
				RemoteAddr:    c.RealIP(),
				Host:          req.Host,
				Method:        req.Method,
				URL:           req.RequestURI,
				Pattern:       c.Path(),
				Referer:       req.Referer(),
				UserAgent:     req.UserAgent(),
				Status:        res.Status,
				ContentLength: req.ContentLength,
				ResponseSize:  res.Size,
				RequestHeader: req.Header,
			}
			if err != nil {
				var httpErr *echo.HTTPError
				if errors.As(err, &httpErr) {
					row.Status = httpErr.Code
				}
			}
			pl.Send(row)
			return err
		}
	}
}
