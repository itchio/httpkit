package htfs_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itchio/httpkit/htfs"
)

// stallingServer serves a 64-byte file but sends only the first few bytes
// of any range, then holds the connection open until the client goes away.
func stallingServer(t *testing.T) *httptest.Server {
	const size = 64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := int64(0)
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			start, _ = strconv.ParseInt(strings.TrimSuffix(strings.TrimPrefix(rng, "bytes="), "-"), 10, 64)
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
		w.Header().Set("Content-Length", strconv.FormatInt(size-start, 10))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("abcd"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
}

func Test_FileContextCancelsStalledRead(t *testing.T) {
	server := stallingServer(t)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := defaultSettings(t)
	res := &itchtfs{url: server.URL}
	f, err := htfs.Open(ctx, res.GetURL, res.NeedsRenewal, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	done := make(chan error, 1)
	go func() {
		buf := make([]byte, 16)
		_, err := f.ReadAt(buf, 0)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("read returned before cancel: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not return after cancel")
	}
}

func Test_FileContextCancelsBeforeConnect(t *testing.T) {
	server := stallingServer(t)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	settings := defaultSettings(t)
	res := &itchtfs{url: server.URL}
	_, err := htfs.Open(ctx, res.GetURL, res.NeedsRenewal, settings)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// flakyServer stalls the first request's headers and serves the rest.
func flakyServer(t *testing.T, data []byte) (*httptest.Server, *int32) {
	var calls int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			<-r.Context().Done()
			return
		}
		http.ServeContent(w, r, "file", time.Time{}, bytes.NewReader(data))
	})), &calls
}

func Test_FileRequestTimeoutIsRetried(t *testing.T) {
	data := []byte("0123456789abcdef")
	server, calls := flakyServer(t, data)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := defaultSettings(t)
	settings.Client = &http.Client{Timeout: 100 * time.Millisecond}
	res := &itchtfs{url: server.URL}
	f, err := htfs.Open(ctx, res.GetURL, res.NeedsRenewal, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, 4)
	n, err := f.ReadAt(buf, 4)
	if err != nil || n != 4 || string(buf) != "4567" {
		t.Fatalf("n=%d err=%v buf=%q", n, err, buf)
	}
	if got := atomic.LoadInt32(calls); got != 2 {
		t.Fatalf("expected the timed-out request to be retried once, got %d requests", got)
	}
}

func Test_FileContextCancelsBackoff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := defaultSettings(t)
	settings.RetrySettings.NoSleep = false
	res := &itchtfs{url: server.URL}

	done := make(chan error, 1)
	go func() {
		_, err := htfs.Open(ctx, res.GetURL, res.NeedsRenewal, settings)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("open returned before cancel: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("open did not return after cancel during backoff")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("expected no request after cancel, got %d", got)
	}
}

func Test_FileContextStopsURLRenewal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var getURLCalls atomic.Int32
	getURL := func(context.Context) (string, error) {
		if getURLCalls.Add(1) == 1 {
			return server.URL, nil
		}
		return "", &net.OpError{Op: "dial", Err: errors.New("refused")}
	}
	needsRenewal := func(*http.Response, []byte) bool { return true }

	settings := defaultSettings(t)
	settings.RetrySettings.NoSleep = false

	done := make(chan error, 1)
	go func() {
		_, err := htfs.Open(ctx, getURL, needsRenewal, settings)
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("open returned before cancel: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("open did not return after cancel during renewal backoff")
	}
	if got := getURLCalls.Load(); got != 2 {
		t.Fatalf("expected no renewal call after cancel, got %d getURL calls", got)
	}
}
