// Package fasthttp is middleware for fasthttp
package fasthttp

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func TestMiddleware(t *testing.T) {
	r := router.New()
	r.SaveMatchedRoutePath = true

	r.GET("/user/{id}", func(ctx *fasthttp.RequestCtx) {
		fmt.Fprintf(ctx, "Hello, %s world!", ctx.UserValue("id"))
	})
	r.POST("/user", func(ctx *fasthttp.RequestCtx) {
		fmt.Fprintf(ctx, "Hello, %s world!", ctx.FormValue("id"))
	})

	listen := "localhost:8989"
	filename := "/tmp/fasthttp.parquet"

	pl := NewLogger()
	go fasthttp.ListenAndServe(listen, pl.Middleware(r.Handler))
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
