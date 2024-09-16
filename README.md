# Usage


## echo

```go
import (
	"github.com/labstack/echo/v4"
	pl "github.com/matsuu/middleware-parquetlogger/echo"
)

func main() {
	e := echo.New()

	pLogger := pl.NewLogger()
	e.Use(pLogger.Middleware())
	go func() {
		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGUSR1)
		for range sig {
			pLogger.Export("/tmp/log.parquet")
		}
	}()
}
```

## net/http

```go
import (
	"net/http"

	pl "github.com/matsuu/middleware-parquetlogger/http"
)

func main() {
	e := echo.New()

	pLogger := pl.NewLogger()
	go func() {
		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGUSR1)
		for range sig {
			pLogger.Export("/tmp/log.parquet")
		}
	}()

	http.Handle("/", pLogger.Middleware(helloFunc))
	http.ListenAndServe(":8000", nil)
}
```

## gin

```go
import (
	"github.com/gin-gonic/gin"
	pl "github.com/matsuu/middleware-parquetlogger/gin"
)

func main() {
	r := gin.Default()

	pl := NewLogger()
	r.Use(pl.Middleware())
	go func() {
		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGUSR1)
		for range sig {
			pLogger.Export("/tmp/log.parquet")
		}
	}()

	// ...

	r.Run()
}
```

## fasthttp

```go
import (
	"github.com/fasthttp/router"
	pl "github.com/matsuu/middleware-parquetlogger/fasthttp"
	"github.com/valyala/fasthttp"
)

func main() {
	r := router.New()
	r.SaveMatchedRoutePath = true

	// ...

	pl := NewLogger()
	go func() {
		sig := make(chan os.Signal)
		signal.Notify(sig, syscall.SIGUSR1)
		for range sig {
			pLogger.Export("/tmp/log.parquet")
		}
	}()
	fasthttp.ListenAndServe(":8080", pl.Middleware(r.Handler))
}
```

# Analyze

```sh
cat parquet.sql | duckdb -cmd "SET VARIABLE path = '/tmp/log.parquet'" > result.md
```
