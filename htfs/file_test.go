package htfs_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/itchio/httpkit/htfs"

	"github.com/itchio/httpkit/retrycontext"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

type itchtfs struct {
	url string
}

func (ifs *itchtfs) Scheme() string {
	return "itchtfs"
}

func (ifs *itchtfs) MakeResource(u *url.URL) (htfs.GetURLFunc, htfs.NeedsRenewalFunc, error) {
	return ifs.GetURL, ifs.NeedsRenewal, nil
}

func (ifs *itchtfs) GetURL() (string, error) {
	return ifs.url, nil
}

func (ifs *itchtfs) NeedsRenewal(res *http.Response, body []byte) bool {
	return false
}

func defaultSettings(t *testing.T) *htfs.Settings {
	return &htfs.Settings{
		Client: http.DefaultClient,
		RetrySettings: &retrycontext.Settings{
			MaxTries: 5,
			NoSleep:  true,
		},
		Log: func(msg string) {
			t.Helper()
			t.Logf(msg)
		},
		LogLevel: 2,
	}
}

func Test_OpenRemoteDownloadBuild(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{})
	defer storageServer.CloseClientConnections()

	ifs := &itchtfs{storageServer.URL}

	u, err := url.Parse("itchtfs:///upload/187770/download/builds/6996?api_key=foo")
	assert.NoError(t, err)

	getURL, needsRenewal, err := ifs.MakeResource(u)
	assert.NoError(t, err)

	f, err := htfs.Open(getURL, needsRenewal, defaultSettings(t))
	assert.NoError(t, err)

	s, err := f.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	readFakeData, err := ioutil.ReadAll(f)
	assert.NoError(t, err)
	assert.Equal(t, len(fakeData), len(readFakeData))
	assert.Equal(t, fakeData, readFakeData)

	readBytes, err := f.ReadAt(readFakeData, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(fakeData), readBytes)
	assert.Equal(t, fakeData, readFakeData)

	err = f.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}
}

func newSimple(t *testing.T, url string) (*htfs.File, error) {
	getURL := func() (string, error) {
		return url, nil
	}

	needsRenewal := func(res *http.Response, body []byte) bool {
		return false
	}

	hf, err := htfs.Open(getURL, needsRenewal, defaultSettings(t))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return hf, nil
}

func Test_File(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{})
	defer storageServer.CloseClientConnections()

	f, err := newSimple(t, storageServer.URL)
	assert.NoError(t, err)

	s, err := f.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	buf := make([]byte, 4)
	readBytes, err := f.ReadAt(buf, 4)
	assert.NoError(t, err)
	assert.Equal(t, len(buf), readBytes)
	assert.Equal(t, []byte("bbbb"), buf)

	err = f.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}
}

func Test_FileNotFound(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
		simulateNotFound: true,
	})
	defer storageServer.CloseClientConnections()

	_, err := newSimple(t, storageServer.URL)
	assert.Error(t, err)
	assert.True(t, errors.Cause(err) == htfs.ErrNotFound)
}

func Test_FileNoRange(t *testing.T) {
	fakeData := getBigFakeData()

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
		simulateNoRangeSupport: true,
	})
	defer storageServer.CloseClientConnections()

	hf, err := newSimple(t, storageServer.URL)
	assert.NoError(t, err)

	b := make([]byte, 4)
	_, err = hf.ReadAt(b, 3*1024*1024)
	assert.Error(t, err)
	if err != nil {
		se, ok := errors.Cause(err).(*htfs.ServerError)
		assert.True(t, ok)
		if ok {
			assert.EqualValues(t, htfs.ServerErrorCodeNoRangeSupport, se.Code)
		}
	}
}

func Test_File503(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
		simulateOtherStatus: 503,
	})
	defer storageServer.CloseClientConnections()

	_, err := newSimple(t, storageServer.URL)
	assert.Error(t, err)
}

type codeDisruption struct {
	code    int
	message string
}

func Test_FileCodeDisruptions(t *testing.T) {
	fakeData := []byte("aaaabbbb")

	codeDisruptions := []codeDisruption{
		{429, "Too Many Requests"},
		{500, "Internal Server Error"},
		{502, "Bad Gateway"},
		{503, "Service Unavailable"},
	}

	for _, cd := range codeDisruptions {
		storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
			disruption: &storageDisruption{
				streak: 3,
				handler: func(w http.ResponseWriter) {
					http.Error(w, cd.message, cd.code)
				},
			},
		})
		defer storageServer.CloseClientConnections()

		_, err := newSimple(t, storageServer.URL)
		assert.NoError(t, err)
	}

	func() {
		storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
			disruption: &storageDisruption{
				streak: 6, // one over default retry count
				handler: func(w http.ResponseWriter) {
					http.Error(w, "Just messing with you", 503)
				},
			},
		})
		defer storageServer.CloseClientConnections()

		_, err := newSimple(t, storageServer.URL)
		assert.Error(t, err)
	}()

	func() {
		storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
			disruption: &storageDisruption{
				streak: 1, // only one non-retriable should be enough
				handler: func(w http.ResponseWriter) {
					http.Error(w, "I'm a teapot", 418)
				},
			},
		})
		defer storageServer.CloseClientConnections()

		_, err := newSimple(t, storageServer.URL)
		assert.Error(t, err)
	}()
}

func Test_FileURLRenewal(t *testing.T) {
	fakeData := make([]byte, 16)

	ctx := &fakeStorageContext{
		requiredT: 1,
	}
	storageServer := fakeStorage(t, fakeData, ctx)
	defer storageServer.CloseClientConnections()

	serverBaseURL, err := url.Parse(storageServer.URL)
	assert.NoError(t, err)

	giveExpired := false
	renewalsAdvertised := 0
	renewalsDone := 0

	getURL := func() (string, error) {
		renewalsDone++
		sbuv := *serverBaseURL
		newURL := &sbuv
		query := newURL.Query()

		t := ctx.requiredT
		if giveExpired {
			t = 0
			giveExpired = false
		}

		query.Set("t", fmt.Sprintf("%d", t))
		newURL.RawQuery = query.Encode() // apparently needed for URL.String() to behave
		return newURL.String(), nil
	}

	needsRenewal := func(res *http.Response, body []byte) bool {
		if res.StatusCode == 400 {
			renewalsAdvertised++
			return true
		}
		return false
	}

	settings := defaultSettings(t)
	settings.ForbidBacktracking = true
	hf, err := htfs.Open(getURL, needsRenewal, settings)
	assert.NoError(t, err)

	assert.EqualValues(t, 1, ctx.numGET, "expected number of GET requests")
	assert.EqualValues(t, 0, renewalsAdvertised, "expected number of renewals advertised")
	assert.EqualValues(t, 1, renewalsDone, "expected number of renewals done")

	readBuf := make([]byte, 1)

	iteration := 0

	for off := int64(15); off >= 0; off-- {
		iteration++
		readBytes, rErr := hf.ReadAt(readBuf, off)
		assert.NoError(t, rErr)
		assert.EqualValues(t, 1, readBytes)

		assert.EqualValues(t, iteration+iteration-1, ctx.numGET, "number of GET requests")
		assert.EqualValues(t, iteration-1, renewalsAdvertised, "number of renewals advertised")
		assert.EqualValues(t, iteration, renewalsDone, "number of renewals done")

		ctx.requiredT++
	}

	ctx.requiredT--

	readBuf2 := make([]byte, 15)
	readBytes, rErr := hf.ReadAt(readBuf2, 1)
	assert.NoError(t, rErr)
	assert.EqualValues(t, len(readBuf2), readBytes)

	assert.EqualValues(t, iteration+iteration-1, ctx.numGET, "number of GET requests")
	assert.EqualValues(t, iteration-1, renewalsAdvertised, "number of renewals advertised")
	assert.EqualValues(t, iteration, renewalsDone, "number of renewals done")

	// now start with an expired URL
	renewalsDone = 0
	renewalsAdvertised = 0
	giveExpired = true

	ctx.requiredT = 3000

	hf, err = htfs.Open(getURL, needsRenewal, defaultSettings(t))
	assert.NoError(t, err)

	assert.EqualValues(t, 1, renewalsAdvertised, "number of renewals advertised")
	assert.EqualValues(t, 2, renewalsDone, "number of renewals done")
}

var _bigFakeData []byte

// returns 4MB's worth of random data
func getBigFakeData() []byte {
	if _bigFakeData == nil {
		src := rand.NewSource(time.Now().UnixNano())
		prng := rand.New(src)
		_bigFakeData = make([]byte, 4*1024*1024)
		_, err := prng.Read(_bigFakeData)
		if err != nil {
			panic(err)
		}
	}
	return _bigFakeData
}

func Test_FileSequentialReads(t *testing.T) {
	testSequentialReads(t, false)
}

func Test_FileSequentialReadsWithBacktracking(t *testing.T) {
	testSequentialReads(t, true)
}

func testSequentialReads(t *testing.T, backtracking bool) {
	fakeData := getBigFakeData()

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{})
	defer storageServer.CloseClientConnections()

	hf, err := newSimple(t, storageServer.URL)
	hf.ForbidBacktracking = !backtracking
	assert.NoError(t, err)

	hf.ReaderStaleThreshold = time.Millisecond * time.Duration(100)

	readBuf := make([]byte, 256)
	offset := int64(0)
	readIndex := 0

	sequentialReadStop := int64(len(readBuf) * 10)

	for offset < sequentialReadStop {
		readIndex++

		if readIndex%4 == 0 {
			offset += int64(len(readBuf))
			continue
		}

		readBytes, rErr := hf.ReadAt(readBuf, offset)
		assert.NoError(t, rErr)
		assert.Equal(t, len(readBuf), readBytes)

		offset += int64(readBytes)
	}

	expectedNumReaders := 1
	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// forcing to provision a new reader (except if backtracking)
	readBytes, err := hf.ReadAt(readBuf, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	if !backtracking {
		expectedNumReaders += 1
	}

	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// re-using the first one
	readBytes, err = hf.ReadAt(readBuf, sequentialReadStop+int64(len(readBuf)))
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// forcing a third one
	readBytes, err = hf.ReadAt(readBuf, int64(len(fakeData))-int64(len(readBuf)))
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	expectedNumReaders += 1
	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// re-using second one
	readBytes, err = hf.ReadAt(readBuf, int64(len(readBuf)))
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// and again, skipping a few
	readBytes, err = hf.ReadAt(readBuf, int64(len(readBuf)*3))
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	// wait for readers to become stale
	time.Sleep(time.Millisecond * time.Duration(200))

	// now just read something random, should be back to 1 reader
	readBytes, err = hf.ReadAt(readBuf, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(readBuf), readBytes)

	expectedNumReaders = 1
	assert.Equal(t, expectedNumReaders, hf.NumReaders())

	err = hf.Close()
	assert.NoError(t, err)
}

func Test_FileConcurrentReadAt(t *testing.T) {
	fakeData := []byte("abcdefghijklmnopqrstuvwxyz")

	storageServer := fakeStorage(t, fakeData, &fakeStorageContext{
		delay: 10 * time.Millisecond,
	})
	defer storageServer.CloseClientConnections()

	hf, err := newSimple(t, storageServer.URL)
	assert.NoError(t, err)

	s, err := hf.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(fakeData)), s.Size())

	done := make(chan bool)
	errs := make(chan error)

	rand.Seed(0xDEADBEEF)
	for i := range rand.Perm(len(fakeData)) {
		go func(i int) {
			buf := make([]byte, 1)
			readBytes, rErr := hf.ReadAt(buf, int64(i))
			if rErr != nil {
				errs <- rErr
				return
			}

			assert.Equal(t, readBytes, 1)
			assert.Equal(t, string(buf), string(fakeData[i:i+1]))

			done <- true
		}(i)
	}

	maxReaders := 0

	for i := 0; i < len(fakeData); i++ {
		numReaders := hf.NumReaders()
		if numReaders > maxReaders {
			maxReaders = numReaders
		}

		select {
		case rErr := <-errs:
			t.Fatal(rErr)
			t.FailNow()
		case <-done:
			// good!
		}
	}

	t.Logf("maximum number of readers: %d (total reads: %d)", maxReaders, len(fakeData))

	err = hf.Close()
	if err != nil {
		t.Fatal(err)
		t.FailNow()
	}

	assert.Equal(t, 0, hf.NumReaders())
}

////////////////////////
// fake storage
////////////////////////

const expiredURLMessage = "Signed URL Expired"

type fakeStorageContext struct {
	delay                  time.Duration
	simulateNoRangeSupport bool
	simulateNotFound       bool
	simulateOtherStatus    int
	requiredT              int64
	numGET                 int
	numHEAD                int
	disruption             *storageDisruption
}

type disruptionHandlerFunc func(w http.ResponseWriter)

type storageDisruption struct {
	// how many errors to return in a row before succeeding
	streak int

	// what to do when the disruption happens
	handler disruptionHandlerFunc

	// internal
	counter int
}

func fakeStorage(t *testing.T, content []byte, ctx *fakeStorageContext) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ctx.simulateNotFound {
			w.WriteHeader(404)
			return
		}

		if ctx.simulateOtherStatus != 0 {
			w.WriteHeader(ctx.simulateOtherStatus)
			return
		}

		disrupt := ctx.disruption
		if disrupt != nil {
			if disrupt.counter < disrupt.streak {
				disrupt.handler(w)
				disrupt.counter++
				return
			}
			disrupt.counter = 0
		}

		hasExpired := false

		if ctx.requiredT > 0 {
			t := r.URL.Query().Get("t")
			if t != "" {
				tVal, err := strconv.ParseInt(t, 10, 64)
				if err == nil {
					if tVal < ctx.requiredT {
						hasExpired = true
					}
				}
			}
		}

		if r.Method == "HEAD" {
			ctx.numHEAD++
			if hasExpired {
				http.Error(w, expiredURLMessage, 400)
				return
			}

			w.Header().Set("content-length", fmt.Sprintf("%d", len(content)))
			w.WriteHeader(200)
			return
		}

		if r.Method != "GET" {
			http.Error(w, "Invalid method", 400)
			return
		}

		ctx.numGET++
		if hasExpired {
			http.Error(w, expiredURLMessage, 400)
			return
		}

		time.Sleep(ctx.delay)

		w.Header().Set("content-type", "application/octet-stream")
		rangeHeader := r.Header.Get("Range")

		start := int64(0)
		end := int64(len(content)) - 1

		if rangeHeader == "" || ctx.simulateNoRangeSupport {
			w.WriteHeader(200)
		} else {
			equalTokens := strings.Split(rangeHeader, "=")
			if len(equalTokens) != 2 {
				http.Error(w, "Invalid range header", 400)
				return
			}

			dashTokens := strings.Split(equalTokens[1], "-")
			if len(dashTokens) != 2 {
				http.Error(w, "Invalid range header value", 400)
				return
			}

			var err error

			start, err = strconv.ParseInt(dashTokens[0], 10, 64)
			if err != nil {
				http.Error(w, fmt.Sprintf("Invalid range header start: %s", err.Error()), 400)
				return
			}

			if dashTokens[1] != "" {
				end, err = strconv.ParseInt(dashTokens[1], 10, 64)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid range header start: %s", err.Error()), 400)
					return
				}
			}

			contentRangeHeader := fmt.Sprintf("%d-%d/%d", start, end, len(content))
			w.Header().Set("content-range", contentRangeHeader)
			w.WriteHeader(206)
		}

		sr := io.NewSectionReader(bytes.NewReader(content), start, end+1-start)
		_, err := io.Copy(w, sr)
		if err != nil {
			if strings.Contains(err.Error(), "broken pipe") {
				// ignore
			} else if strings.Contains(err.Error(), "forcibly closed by the remote host") {
				// ignore
			} else if strings.Contains(err.Error(), "protocol wrong type for socket") {
				// ignore
			} else {
				t.Logf("storage copy error: %s", err.Error())
				return
			}
		}
	}))

	return server
}
