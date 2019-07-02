package uploader

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/itchio/headway/state"
	"github.com/itchio/headway/united"

	"github.com/itchio/savior/fullyrandom"
	"github.com/stretchr/testify/assert"
)

func Test_ChunkUploader(t *testing.T) {
	assert := assert.New(t)
	loudTests := os.Getenv("LOUD_TESTS") == "1"
	log := func(format string, a ...interface{}) {
		if loudTests {
			log.Printf(format, a...)
		} else {
			t.Logf(format, a...)
		}
	}

	server := makeTestServer(t, log)
	server.settings.latency = 200 * time.Millisecond
	server.settings.bandwidthBytesPerSec = 10 * 1024 * 1024 // 10 MB/s
	ru := NewResumableUpload(server.URL)
	ru.SetConsumer(&state.Consumer{
		OnMessage: func(lvl string, msg string) {
			log("[%s] %s", lvl, msg)
		},
	})

	ref := new(bytes.Buffer)
	mw := io.MultiWriter(ref, ru)

	for i := 0; i < 16; i++ {
		tmust(t, fullyrandom.Write(mw, 1*1024*1024, time.Now().UnixNano()))
		time.Sleep(500 * time.Millisecond)
	}
	tmust(t, ru.Close())

	assert.EqualValues(ref.Bytes(), server.state.data)
	log("num blocks stored: %+v", server.state.numBlocksStored)
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

func makeTestServer(t *testing.T, log func(msg string, a ...interface{})) *fakeGCS {
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
			buf, err := ioutil.ReadAll(r.Body)
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
