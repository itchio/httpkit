package httpfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"mime"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	goerrors "errors"

	humanize "github.com/dustin/go-humanize"
	"github.com/itchio/httpkit/neterr"
	"github.com/itchio/httpkit/retrycontext"
	"github.com/pkg/errors"
)

var forbidBacktracking = os.Getenv("HTTPFILE_NO_BACKTRACK") == "1"
var dumpStats = os.Getenv("HTTPFILE_DUMP_STATS") == "1"

// A GetURLFunc returns a URL we can download the resource from.
// It's handy to have this as a function rather than a constant for signed expiring URLs
type GetURLFunc func() (urlString string, err error)

// A NeedsRenewalFunc analyzes an HTTP response and returns true if it needs to be renewed
type NeedsRenewalFunc func(res *http.Response, body []byte) bool

// A LogFunc prints debug message
type LogFunc func(msg string)

// amount we're willing to download and throw away
const maxDiscard int64 = 1 * 1024 * 1024 // 1MB

const maxRenewals = 5

// ErrNotFound is returned when the HTTP server returns 404 - it's not considered a temporary error
var ErrNotFound = goerrors.New("HTTP file not found on server")

var ErrTooManyRenewals = goerrors.New("Giving up, getting too many renewals. Try again later or contact support.")

type hstats struct {
	connectionWait time.Duration
	connections    int
	renews         int

	fetchedBytes int64
	numCacheMiss int64

	cachedBytes int64
	numCacheHit int64
}

var idSeed int64 = 1
var idMutex sync.Mutex

// HTTPFile allows accessing a file served by an HTTP server as if it was local
// (for random-access reading purposes, not writing)
type HTTPFile struct {
	getURL        GetURLFunc
	needsRenewal  NeedsRenewalFunc
	client        *http.Client
	retrySettings *retrycontext.Settings

	Log      LogFunc
	LogLevel int

	name   string
	size   int64
	offset int64 // for io.ReadSeeker

	ReaderStaleThreshold time.Duration

	closed bool

	readers map[string]*httpReader
	lock    sync.Mutex

	currentURL string
	urlMutex   sync.Mutex
	header     http.Header
	requestURL *url.URL

	stats *hstats

	ForbidBacktracking bool
}

type httpReader struct {
	file      *HTTPFile
	id        string
	touchedAt time.Time
	offset    int64
	cache     []byte
	cached    int
	backtrack int
	body      io.ReadCloser
	reader    *bufio.Reader

	header        http.Header
	requestURL    *url.URL
	statusCode    int
	contentLength int64
}

// DefaultReaderStaleThreshold is the duration after which HTTPFile's readers
// are considered stale, and are closed instead of reused. It's set to 10 seconds.
const DefaultReaderStaleThreshold = time.Second * time.Duration(10)

const DefaultLogLevel = 1

func (hr *httpReader) Stale() bool {
	return time.Since(hr.touchedAt) > hr.file.ReaderStaleThreshold
}

func (hr *httpReader) Read(data []byte) (int, error) {
	if hr.backtrack > 0 {
		readLen := len(data)
		if readLen > hr.backtrack {
			readLen = hr.backtrack
		}

		// hr.file.log2("asked to read %d, backtrack is %d, cached is %d", len(data), hr.backtrack, hr.cached)
		cacheStartIndex := len(hr.cache) - hr.backtrack
		// hr.file.log2("copying [%d:%d] to [0:%d]", cacheStartIndex, cacheStartIndex+readLen, readLen)
		copy(data[:readLen], hr.cache[cacheStartIndex:cacheStartIndex+readLen])
		hr.backtrack -= readLen

		hr.file.stats.cachedBytes += int64(readLen)
		hr.file.stats.numCacheHit++

		return readLen, nil
	}

	hr.touchedAt = time.Now()
	readBytes, err := hr.reader.Read(data)
	hr.offset += int64(readBytes)

	hr.file.stats.fetchedBytes += int64(readBytes)
	hr.file.stats.numCacheMiss++

	// offset cache to make room for the new data
	remainingOldCacheSize := len(hr.cache) - readBytes
	// hr.file.log2("moving [%d:] to [:%d]", readBytes, remainingOldCacheSize)
	// hr.file.log2("caching %d bytes into [%d:]", readBytes, remainingOldCacheSize)
	copy(hr.cache[:remainingOldCacheSize], hr.cache[readBytes:])
	copy(hr.cache[remainingOldCacheSize:], data[:readBytes])
	hr.cached += readBytes
	if hr.cached > len(hr.cache) {
		hr.cached = len(hr.cache)
	}

	if err != nil {
		return readBytes, err
	}
	return readBytes, nil
}

func (hr *httpReader) Discard(n int) (int, error) {
	// TODO: don't realloc that buf all the time
	buf := make([]byte, 4096)

	totalDiscarded := 0
	for n > 0 {
		readLen := n
		if readLen > len(buf) {
			readLen = len(buf)
		}

		discarded, err := hr.Read(buf[:readLen])
		totalDiscarded += discarded
		if err != nil {
			return totalDiscarded, err
		}
		n -= discarded
	}
	return totalDiscarded, nil
}

type NeedsRenewalError struct {
	url string
}

func (nre *NeedsRenewalError) Error() string {
	return "url has expired and needs renewal"
}

type ServerErrorCode int64

const (
	ServerErrorCodeUnknown ServerErrorCode = iota
	ServerErrorCodeNoRangeSupport
)

type ServerError struct {
	Host       string
	Message    string
	Code       ServerErrorCode
	StatusCode int
}

func (se *ServerError) Error() string {
	return fmt.Sprintf("server error: for host %s: %s", se.Host, se.Message)
}

func (hr *httpReader) Connect() error {
	hf := hr.file

	if hr.body != nil {
		err := hr.body.Close()
		if err != nil {
			return err
		}

		hr.body = nil
		hr.reader = nil
	}

	tryURL := func(urlStr string) error {
		req, err := http.NewRequest("GET", urlStr, nil)
		if err != nil {
			return err
		}

		byteRange := fmt.Sprintf("bytes=%d-", hr.offset)
		req.Header.Set("Range", byteRange)

		res, err := hf.client.Do(req)
		if err != nil {
			return err
		}

		if res.StatusCode == 200 && hr.offset > 0 {
			defer res.Body.Close()
			return errors.WithStack(&ServerError{Host: req.Host, Message: fmt.Sprintf("HTTP Range header not supported"), Code: ServerErrorCodeNoRangeSupport, StatusCode: res.StatusCode})
		}

		if res.StatusCode/100 != 2 {
			defer res.Body.Close()

			body, err := ioutil.ReadAll(res.Body)
			if err != nil {
				body = []byte("could not read error body")
				err = nil
			}

			if hf.needsRenewal(res, body) {
				return &NeedsRenewalError{url: urlStr}
			}

			return errors.WithStack(&ServerError{Host: req.Host, Message: fmt.Sprintf("HTTP %d: %v", res.StatusCode, string(body)), StatusCode: res.StatusCode})
		}

		hr.reader = bufio.NewReaderSize(res.Body, int(maxDiscard))
		hr.body = res.Body
		hr.header = res.Header
		hr.requestURL = res.Request.URL
		hr.statusCode = res.StatusCode
		hr.contentLength = res.ContentLength
		return nil
	}

	urlStr := hf.getCurrentURL()

	retryCtx := hf.newRetryContext()
	renewalTries := 0

	for retryCtx.ShouldTry() {
		startTime := time.Now()
		err := tryURL(urlStr)
		if err != nil {
			if _, ok := err.(*NeedsRenewalError); ok {
				renewalTries++
				if renewalTries >= maxRenewals {
					return ErrTooManyRenewals
				}

				hf.log("Connect: got renew: %s", err.Error())

				err = func() error {
					renewRetryCtx := hf.newRetryContext()

					for renewRetryCtx.ShouldTry() {
						hf.stats.renews++
						urlStr, err = hf.renewURL()
						if err != nil {
							if hf.shouldRetry(err) {
								hf.log("Connect.renew: got retriable error: %s", err.Error())
								renewRetryCtx.Retry(err)
								continue
							} else {
								hf.log("Connect.renew: got non-retriable error: %s", err.Error())
								return err
							}
						}

						return nil
					}
					return errors.WithMessage(renewRetryCtx.LastError, "httpfile renew")
				}()
				if err != nil {
					return err
				}

				continue
			} else if hf.shouldRetry(err) {
				hf.log("Connect: got retriable error: %s", err.Error())
				retryCtx.Retry(err)
				continue
			} else {
				return err
			}
		}

		totalConnDuration := time.Since(startTime)
		hf.log("Connect: connected in %s!", totalConnDuration)
		hf.stats.connections++
		hf.stats.connectionWait += totalConnDuration
		return nil
	}

	return errors.WithMessage(retryCtx.LastError, "httpfile connect")
}

func (hr *httpReader) Close() error {
	if hr.body != nil {
		err := hr.body.Close()
		hr.body = nil

		if err != nil {
			return err
		}
	}

	return nil
}

var _ io.Seeker = (*HTTPFile)(nil)
var _ io.Reader = (*HTTPFile)(nil)
var _ io.ReaderAt = (*HTTPFile)(nil)
var _ io.Closer = (*HTTPFile)(nil)

// Settings allows passing additional settings to an HTTPFile
type Settings struct {
	Client        *http.Client
	RetrySettings *retrycontext.Settings
	Log           LogFunc
}

// New returns a new HTTPFile. Note that it differs from os.Open in that it does a first request
// to determine the remote file's size. If that fails (after retries), an error will be returned.
func New(getURL GetURLFunc, needsRenewal NeedsRenewalFunc, settings *Settings) (*HTTPFile, error) {
	client := settings.Client
	if client == nil {
		client = http.DefaultClient
	}

	retryCtx := retrycontext.NewDefault()
	if settings.RetrySettings != nil {
		retryCtx.Settings = *settings.RetrySettings
	}

	hf := &HTTPFile{
		getURL:        getURL,
		retrySettings: &retryCtx.Settings,
		needsRenewal:  needsRenewal,
		client:        client,
		name:          "<remote file>",

		readers: make(map[string]*httpReader),
		stats:   &hstats{},

		ReaderStaleThreshold: DefaultReaderStaleThreshold,
		LogLevel:             DefaultLogLevel,

		ForbidBacktracking: forbidBacktracking,
	}

	hf.Log = settings.Log

	urlStr, err := getURL()
	if err != nil {
		// this assumes getURL does its own retrying
		return nil, errors.WithMessage(normalizeError(err), "httpfile.New (getting URL)")
	}
	hf.currentURL = urlStr

	hr, err := hf.borrowReader(0)
	if err != nil {
		return nil, errors.WithMessage(normalizeError(err), "httpfile.New (initial request)")
	}

	hf.requestURL = hr.requestURL

	if hr.statusCode == 206 {
		rangeHeader := hr.header.Get("content-range")
		rangeTokens := strings.Split(rangeHeader, "/")
		totalBytesStr := rangeTokens[len(rangeTokens)-1]
		hf.size, err = strconv.ParseInt(totalBytesStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("Could not parse file size: %s", err.Error())
		}
	} else if hr.statusCode == 200 {
		hf.size = hr.contentLength
	}

	// we have to use requestURL because we want the URL after
	// redirect (for hosts like sourceforge)
	pathTokens := strings.Split(hf.requestURL.Path, "/")
	hf.name = pathTokens[len(pathTokens)-1]

	dispHeader := hr.header.Get("content-disposition")
	if dispHeader != "" {
		_, mimeParams, err := mime.ParseMediaType(dispHeader)
		if err == nil {
			filename := mimeParams["filename"]
			if filename != "" {
				hf.name = filename
			}
		}
	}

	return hf, nil
}

func (hf *HTTPFile) newRetryContext() *retrycontext.Context {
	retryCtx := retrycontext.NewDefault()
	if hf.retrySettings != nil {
		retryCtx.Settings = *hf.retrySettings
	}
	return retryCtx
}

// NumReaders returns the number of connections currently used by the httpfile
// to serve ReadAt calls
func (hf *HTTPFile) NumReaders() int {
	return len(hf.readers)
}

func (hf *HTTPFile) borrowReader(offset int64) (*httpReader, error) {
	if hf.size > 0 && offset >= hf.size {
		return nil, io.EOF
	}

	var bestReader string
	var bestDiff int64 = math.MaxInt64

	var bestBackReader string
	var bestBackDiff int64 = math.MaxInt64

	for _, reader := range hf.readers {
		if reader.Stale() {
			delete(hf.readers, reader.id)

			err := reader.Close()
			if err != nil {
				return nil, err
			}
			continue
		}

		diff := offset - reader.offset
		if diff < 0 && -diff < maxDiscard && -diff <= int64(reader.cached) {
			if -diff < bestBackDiff {
				bestBackReader = reader.id
				bestBackDiff = -diff
			}
		}

		if diff >= 0 && diff < maxDiscard {
			if diff < bestDiff {
				bestReader = reader.id
				bestDiff = diff
			}
		}
	}

	if bestReader != "" {
		// re-use!
		reader := hf.readers[bestReader]
		delete(hf.readers, bestReader)

		// clear backtrack if any
		reader.backtrack = 0

		// discard if needed
		if bestDiff > 0 {
			hf.log2("borrow: for %d, re-using %d by discarding %d bytes", offset, reader.offset, bestDiff)

			// XXX: not int64-clean
			_, err := reader.Discard(int(bestDiff))
			if err != nil {
				if hf.shouldRetry(err) {
					hf.log2("borrow: for %d, discard failed because of retriable error, reconnecting", offset)
					reader.offset = offset
					err = reader.Connect()
					if err != nil {
						return nil, err
					}
				} else {
					return nil, err
				}
			}
		}

		return reader, nil
	}

	if !hf.ForbidBacktracking && bestBackReader != "" {
		// re-use!
		reader := hf.readers[bestBackReader]
		delete(hf.readers, bestBackReader)

		hf.log2("borrow: for %d, re-using %d by backtracking %d bytes", offset, reader.offset, bestBackDiff)

		// backtrack as needed
		reader.backtrack = int(bestBackDiff)
		return reader, nil
	}

	// provision a new reader
	hf.log("Establishing connection for bytes %d-", offset)

	id := generateID()
	reader := &httpReader{
		file:      hf,
		id:        fmt.Sprintf("reader-%d", id),
		touchedAt: time.Now(),
		offset:    offset,
		cache:     make([]byte, int(maxDiscard)),
		backtrack: 0,
	}

	err := reader.Connect()
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func (hf *HTTPFile) returnReader(reader *httpReader) {
	// TODO: enforce max idle readers ?

	reader.touchedAt = time.Now()
	hf.readers[reader.id] = reader
}

func (hf *HTTPFile) getCurrentURL() string {
	hf.urlMutex.Lock()
	defer hf.urlMutex.Unlock()

	return hf.currentURL
}

func (hf *HTTPFile) renewURL() (string, error) {
	hf.urlMutex.Lock()
	defer hf.urlMutex.Unlock()

	urlStr, err := hf.getURL()
	if err != nil {
		return "", err
	}

	hf.currentURL = urlStr
	return hf.currentURL, nil
}

// Stat returns an os.FileInfo for this particular file. Only the Size()
// method is useful, the rest is default values.
func (hf *HTTPFile) Stat() (os.FileInfo, error) {
	return &httpFileInfo{hf}, nil
}

// Seek the read head within the file - it's instant and never returns an
// error, except if whence is one of os.SEEK_SET, os.SEEK_END, or os.SEEK_CUR.
// If an invalid offset is given, it will be truncated to a valid one, between
// [0,size).
func (hf *HTTPFile) Seek(offset int64, whence int) (int64, error) {
	hf.lock.Lock()
	defer hf.lock.Unlock()

	var newOffset int64

	switch whence {
	case os.SEEK_SET:
		newOffset = offset
	case os.SEEK_END:
		newOffset = hf.size + offset
	case os.SEEK_CUR:
		newOffset = hf.offset + offset
	default:
		return hf.offset, fmt.Errorf("invalid whence value %d", whence)
	}

	if newOffset < 0 {
		newOffset = 0
	}

	if newOffset > hf.size {
		newOffset = hf.size
	}

	hf.offset = newOffset
	return hf.offset, nil
}

func (hf *HTTPFile) Read(buf []byte) (int, error) {
	hf.lock.Lock()
	defer hf.lock.Unlock()

	initialOffset := hf.offset
	hf.log2("> Read(%d, %d)", len(buf), initialOffset)
	bytesRead, err := hf.readAt(buf, hf.offset)
	hf.offset += int64(bytesRead)
	hf.log2("< Read(%d, %d) = %d, %+v", len(buf), initialOffset, bytesRead, err != nil)
	return bytesRead, err
}

// ReadAt reads len(buf) byte from the remote file at offset.
// It returns the number of bytes read, and an error. In case of temporary
// network errors or timeouts, it will retry with truncated exponential backoff
// according to RetrySettings
func (hf *HTTPFile) ReadAt(buf []byte, offset int64) (int, error) {
	hf.lock.Lock()
	defer hf.lock.Unlock()

	hf.log2("> ReadAt(%d, %d)", len(buf), offset)
	n, err := hf.readAt(buf, offset)
	hf.log2("< ReadAt(%d, %d) = %d, %+v", len(buf), offset, n, err != nil)
	return n, err
}

func (hf *HTTPFile) readAt(data []byte, offset int64) (int, error) {
	buflen := len(data)
	if buflen == 0 {
		return 0, nil
	}

	reader, err := hf.borrowReader(offset)
	if err != nil {
		return 0, err
	}

	defer hf.returnReader(reader)

	totalBytesRead := 0
	bytesToRead := len(data)

	for totalBytesRead < bytesToRead {
		bytesRead, err := reader.Read(data[totalBytesRead:])
		totalBytesRead += bytesRead

		if err != nil {
			if hf.shouldRetry(err) {
				hf.log("Got %s, retrying", err.Error())
				err = reader.Connect()
				if err != nil {
					return totalBytesRead, err
				}
			} else {
				return totalBytesRead, err
			}
		}
	}

	return totalBytesRead, nil
}

func (hf *HTTPFile) shouldRetry(err error) bool {
	if errors.Cause(err) == io.EOF {
		// don't retry EOF, it's a perfectly expected error
		return false
	}

	if neterr.IsNetworkError(err) {
		hf.log("Retrying: %v", err)
		return true
	}

	if se, ok := errors.Cause(err).(*ServerError); ok {
		switch se.StatusCode {
		case 429: /* Too Many Requests */
			return true
		case 500: /* Internal Server Error */
			return true
		case 502: /* Bad Gateway */
			return true
		case 503: /* Service Unavailable */
			return true
		}
	}

	hf.log("Bailing on error: %v", err)
	return false
}

func isHTTPStatus(err error, statusCode int) bool {
	if se, ok := errors.Cause(err).(*ServerError); ok {
		return se.StatusCode == statusCode
	}
	return false
}

func normalizeError(err error) error {
	if isHTTPStatus(err, 404) {
		return ErrNotFound
	}
	return err
}

func (hf *HTTPFile) closeAllReaders() error {
	for id, reader := range hf.readers {
		err := reader.Close()
		if err != nil {
			return err
		}

		delete(hf.readers, id)
	}

	return nil
}

// Close closes all connections to the distant http server used by this HTTPFile
func (hf *HTTPFile) Close() error {
	hf.lock.Lock()
	defer hf.lock.Unlock()

	if hf.closed {
		return nil
	}

	if dumpStats {
		log.Printf("========= HTTPFile stats ==============")
		log.Printf("= total connections: %d", hf.stats.connections)
		log.Printf("= total renews: %d", hf.stats.renews)
		log.Printf("= time spent connecting: %s", hf.stats.connectionWait)
		size := hf.size
		perc := 0.0
		percCached := 0.0
		if size != 0 {
			perc = float64(hf.stats.fetchedBytes) / float64(size) * 100.0
		}
		allReads := hf.stats.fetchedBytes + hf.stats.cachedBytes
		percCached = float64(hf.stats.cachedBytes) / float64(allReads) * 100.0

		log.Printf("= total bytes fetched: %s / %s (%.2f%%)", humanize.IBytes(uint64(hf.stats.fetchedBytes)), humanize.IBytes(uint64(size)), perc)
		log.Printf("= total bytes served from cache: %s (%.2f%% of all served bytes)", humanize.IBytes(uint64(hf.stats.cachedBytes)), percCached)

		hitRate := float64(hf.stats.numCacheHit) / float64(hf.stats.numCacheHit+hf.stats.numCacheMiss) * 100.0
		log.Printf("= cache hit rate: %.2f%%", hitRate)
		log.Printf("========================================")
	}

	err := hf.closeAllReaders()
	if err != nil {
		return err
	}

	hf.closed = true

	return nil
}

func (hf *HTTPFile) log(format string, args ...interface{}) {
	if hf.Log == nil {
		return
	}

	hf.Log(fmt.Sprintf(format, args...))
}

func (hf *HTTPFile) log2(format string, args ...interface{}) {
	if hf.LogLevel < 2 {
		return
	}

	if hf.Log == nil {
		return
	}

	hf.Log(fmt.Sprintf(format, args...))
}

// GetHeader returns the header the server responded
// with on our initial request. It may contain checksums
// which could be used for integrity checking.
func (hf *HTTPFile) GetHeader() http.Header {
	return hf.header
}

func (hf *HTTPFile) GetRequestURL() *url.URL {
	return hf.requestURL
}

func generateID() int64 {
	idMutex.Lock()
	defer idMutex.Unlock()

	id := idSeed
	idSeed++
	return id
}
