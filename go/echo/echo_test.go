package echo

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

func TestMiddleware(t *testing.T) {
	e := echo.New()

	pl := NewLogger()
	e.Use(pl.Middleware())

	e.GET("/user/:id", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("Hello, %s world!", c.Param("id")))
	})
	e.POST("/user", func(c echo.Context) error {
		return c.String(http.StatusOK, fmt.Sprintf("Hello, %s world!", c.FormValue("id")))
	})

	listen := "localhost:8989"
	filename := "/tmp/echo.parquet"

	go func() {
		e.Logger.Fatal(e.Start(listen))
	}()
	time.Sleep(1 * time.Second)

	if res, err := http.Get(fmt.Sprintf("http://%s/user/http-client", listen)); err != nil {
		t.Fatalf("Failed to get /user/http-client: %v", err)
	} else {
		if buf, err := io.ReadAll(res.Body); err != nil {
			t.Fatalf("Failed to readAll from body: %v", err)
		} else {
			t.Logf("get /user/http-client: %s", buf)
		}
		res.Body.Close()
	}

	if res, err := http.PostForm(fmt.Sprintf("http://%s/user", listen), url.Values{"id": {"http-client-post"}}); err != nil {
		t.Fatalf("Failed to post /user: %v", err)
	} else {
		if buf, err := io.ReadAll(res.Body); err != nil {
			t.Fatalf("Failed to readAll from body: %v", err)
		} else {
			t.Logf("post /user: %s", buf)
		}
		res.Body.Close()
	}

	if res, err := http.Get(fmt.Sprintf("http://%s/404", listen)); err != nil {
		t.Fatalf("Failed to get /404: %v", err)
	} else {
		if buf, err := io.ReadAll(res.Body); err != nil {
			t.Fatalf("Failed to readAll from body: %v", err)
		} else {
			t.Logf("get /404: %s", buf)
		}
		res.Body.Close()
	}

	cmd := exec.Command("curl", "-s", fmt.Sprintf("http://%s/user/curl?a=b", listen))
	if stdoutStdErr, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to exec curl: %v", err)
	} else {
		t.Logf("curl: %s\n", stdoutStdErr)
	}

	if err := pl.Export(filename); err != nil {
		t.Fatalf("Failed to export %s: %v", filename, err)
	}
	if _, err := os.Stat(filename); err != nil {
		t.Fatalf("Failed to export %s: %v", filename, err)
	}
	cmd = exec.Command("duckdb", "-cmd", ".mode line", "-c", fmt.Sprintf("FROM read_parquet('%s')", filename))
	if stdoutStdErr, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to exec duckdb: %v, %s", err, stdoutStdErr)
	} else {
		t.Logf("duckdb: %s\n", stdoutStdErr)
	}
}
