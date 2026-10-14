package uploader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"

	"github.com/itchio/randsource/fullyrandom"
	"github.com/stretchr/testify/assert"
)

func Test_ChunkUploader(t *testing.T) {
	assert := assert.New(t)
	loudTests := os.Getenv("LOUD_TESTS") == "1"
	log := func(format string, a ...any) {
		if loudTests {
			log.Printf(format, a...)
		} else {
			t.Logf(format, a...)
		}
	}

	server := makeTestServer(t, log)
	server.settings.latency = 200 * time.Millisecond
	server.settings.bandwidthBytesPerSec = 10 * 1024 * 1024 // 10 MB/s
	ru := NewResumableUpload(context.Background(), server.URL, WithConsumer(&state.Consumer{
		OnMessage: func(lvl string, msg string) {
			log("[%s] %s", lvl, msg)
		},
	}))

	ref := new(bytes.Buffer)
	mw := io.MultiWriter(ref, ru)

	for range 16 {
		tmust(t, fullyrandom.Write(mw, 1*1024*1024, time.Now().UnixNano()))
		time.Sleep(500 * time.Millisecond)
	}
	tmust(t, ru.Close())

	assert.EqualValues(ref.Bytes(), server.state.data)
	log("num blocks stored: %+v", server.state.numBlocksStored)
}

// A server that never answers a PUT: without cancellation the upload
// would block in Write or Close forever.
func Test_ChunkUploaderCancel(t *testing.T) {
	arrived := make(chan struct{})
	var arrivedOnce sync.Once
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arrivedOnce.Do(func() { close(arrived) })
		// the server only notices the client leaving once the body has
		// been read; release is there so Close can't hang if it doesn't
		io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ru := NewResumableUpload(ctx, server.URL)

	result := make(chan error, 1)
	go func() {
		for range 64 {
			if err := fullyrandom.Write(ru, 1*1024*1024, time.Now().UnixNano()); err != nil {
				result <- err
				return
			}
		}
		result <- ru.Close()
	}()

	select {
	case <-arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("no PUT reached the server")
	}
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not stop after cancel")
	}
}

type fakeGCS struct {
	*httptest.Server
	state struct {
		data            []byte
		head            int64
		numBlocksStored []int64
	}
	settings struct {
		latency              time.Duration
		bandwidthBytesPerSec int64
	}
}

func makeTestServer(t *testing.T, log func(msg string, a ...any)) *fakeGCS {
	fg := &fakeGCS{}

	var chunkSize int64 = 256 * 1024

	fg.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fg.settings.latency > 0 {
			log("Sleeping %s (to simulate latency)", fg.settings.latency)
			time.Sleep(fg.settings.latency)
		}

		switch r.Method {
		case "PUT":
			log("Putting...")
			contentRange := r.Header.Get("content-range")
			if !strings.HasPrefix(contentRange, "bytes ") {
				w.WriteHeader(400)
				fmt.Fprintf(w, "Missing 'bytes ' prefix in content-range header")
				return
			}

			contentRange = strings.TrimPrefix(contentRange, "bytes ")

			log("contentRange: %s", contentRange)
			slashTokens := strings.Split(contentRange, "/")
			storedString := slashTokens[0]
			totalString := slashTokens[1]

			storedTokens := strings.SplitN(storedString, "-", 2)
			start, err := strconv.ParseInt(storedTokens[0], 10, 64)
			tmust(t, err)
			end, err := strconv.ParseInt(storedTokens[1], 10, 64)
			tmust(t, err)
			end++

			sentBytes := int64(end - start)
			if sentBytes%chunkSize != 0 {
				w.WriteHeader(400)
				fmt.Fprintf(w, "Sent bytes (%d) were not a multiple of chunk size (%d)", sentBytes, chunkSize)
				return
			}

			total, _ := strconv.ParseInt(totalString, 10, 64)

			log("start=%d, end=%d, total=%d", start, end, total)
			committedRange := &httpRange{
				start: 0,
				end:   end,
			}
			w.Header().Set("range", committedRange.String())

			if totalString != "*" {
				log("last block!")
				w.WriteHeader(200)
			} else {
				log("committing blocks...")
				w.WriteHeader(308)
			}

			defer r.Body.Close()
			buf, err := io.ReadAll(r.Body)
			tmust(t, err)
			fg.state.data = append(fg.state.data, buf...)
			fg.state.head += int64(len(buf))
			fg.state.numBlocksStored = append(fg.state.numBlocksStored, sentBytes/chunkSize)

			if fg.settings.bandwidthBytesPerSec > 0 {
				bps := fg.settings.bandwidthBytesPerSec
				sleepDuration := time.Millisecond * time.Duration(float64(sentBytes)/float64(bps)*1000.0)
				log("Sleeping %s (to simulating %s bandwidth)", sleepDuration, united.FormatBPS(bps, time.Second))
				time.Sleep(sleepDuration)
			}

			return
		default:
			log("Dunno what to do with request: %#v", r)
			w.WriteHeader(400)
			return
		}
	}))

	return fg
}

// must shows a complete error stack and fails a test immediately
// if err is non-nil
func tmust(t *testing.T, err error) {
	if err != nil {
		t.Helper()
		t.Errorf("%+v", err)
		t.FailNow()
	}
}
