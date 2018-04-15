package httpfile

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
)

// DefaultReaderStaleThreshold is the duration after which HTTPFile's readers
// are considered stale, and are closed instead of reused. It's set to 10 seconds.
const DefaultReaderStaleThreshold = time.Second * time.Duration(10)

type httpReader struct {
	file       *HTTPFile
	id         string
	touchedAt  time.Time
	offset     int64
	cache      []byte
	cached     int
	backtrack  int
	body       io.ReadCloser
	reader     *bufio.Reader
	currentURL string

	header        http.Header
	requestURL    *url.URL
	statusCode    int
	contentLength int64
}

func (hr *httpReader) Stale() bool {
	return time.Since(hr.touchedAt) > hr.file.ReaderStaleThreshold
}

func (hr *httpReader) Read(data []byte) (int, error) {
	if hr.backtrack > 0 {
		// if backtrack > 0, we're reading from the cache:
		//
		//                   offset
		// |...cached data...|...buffered data...|...unread data...
		//      |            |                   |
		//      <-backtrack->|<-----bufsize----->|
		//      ^
		//      position in file
		//
		readLen := len(data)
		if readLen > hr.backtrack {
			// reading from cache (copying ranges of bytes) is
			// completely different than reading from the buffer
			// (actual Read() calls), so if we were asked a large slice
			// we're going to give a short read
			//
			// before:
			//
			// |...cached data...|...buffered data...|...unread data...
			//       |           |
			//       |<--------readLen---------->
			//
			// after:
			//
			// |...cached data...|...buffered data...|...unread data...
			//       |           |
			//       |<-readLen->|
			//
			readLen = hr.backtrack
		}

		// hr.cache has a fixed size but is not necessarily
		// all valid data.
		//
		// |...cached data...|...buffered data...|...unread data...
		//      |            |
		//      cacheStartIndex
		cacheStartIndex := len(hr.cache) - hr.backtrack

		// |...cached data...|...buffered data...|...unread data...
		//      |         |
		//      |<readLen>|
		copy(data[:readLen], hr.cache[cacheStartIndex:cacheStartIndex+readLen])

		// |...cached data...|...buffered data...|...unread data...
		//                |  |
		//                <-->
		//                hr.backtrack (now)
		//
		hr.backtrack -= readLen

		hr.file.stats.cachedBytes += int64(readLen)
		hr.file.stats.numCacheHit++

		return readLen, nil
	}

	// backtrack == 0, so we're reading fresh data
	//
	//                   offset
	// |...cached data...|...buffered data...|...unread data...
	//                   |                   |
	//                   |<-----bufsize----->|
	//                   ^
	//                   position in file
	//
	hr.touchedAt = time.Now()
	readBytes, err := hr.reader.Read(data)
	hr.offset += int64(readBytes)

	hr.file.stats.fetchedBytes += int64(readBytes)
	hr.file.stats.numCacheMiss++

	// make room to cache the new data.
	//
	// before:
	//             |xxxxxxxxxxxxx|xxxx|.......old.......|
	//             |             |    |<---hr.cached--->|
	//             <-thrown away->
	//
	// after:
	//             |xxxx|.......old.......|.....new.....|
	//                  |<---------hr.cached----------->|
	//             |<-remainingOldCacheS->|<-readBytes->|
	//
	remainingOldCacheSize := len(hr.cache) - readBytes

	// before: |xxxxxxxxxxxxx|.........old..........|
	// after:  |xxxx|.........old..........|xxxxxxxx|
	copy(hr.cache[:remainingOldCacheSize], hr.cache[readBytes:])

	// before: |xxxx|.........old..........|xxxxxxxx|
	// after:  |xxxx|.........old..........|..new...|
	copy(hr.cache[remainingOldCacheSize:], data[:readBytes])

	// before: |xxxx|.........old..........|..new...|
	//                       <--------hr.cached----->
	//
	// after:  |xxxx|.........old..........|..new...|
	//              <------------hr.cached---------->
	hr.cached += readBytes

	if hr.cached > len(hr.cache) {
		// before (because cache was full):
		//              |..........................|
		//     <-----------hr.cached--------------->
		// after:
		//              |..........................|
		//              <---------hr.cached-------->
		hr.cached = len(hr.cache)
	}

	if err != nil {
		return readBytes, err
	}
	return readBytes, nil
}

var discardBuf = make([]byte, 4096)

func (hr *httpReader) Discard(n int64) (int, error) {
	// N.B: we don't need to worry about the cache here at all
	// because everything goes through `hr.Read`

	buf := discardBuf
	buflen := int64(len(buf))

	totalDiscarded := 0
	for n > 0 {
		readLen := n
		if readLen > buflen {
			readLen = buflen
		}

		discarded, err := hr.Read(buf[:readLen])
		totalDiscarded += discarded
		if err != nil {
			return totalDiscarded, err
		}
		n -= int64(discarded)
	}
	return totalDiscarded, nil
}

// *not* thread-safe, httpfile handles the locking
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

	retryCtx := hf.newRetryContext()
	renewalTries := 0

	hf.currentURL = hf.getCurrentURL()
	for retryCtx.ShouldTry() {
		startTime := time.Now()
		err := hr.tryConnect()
		if err != nil {
			if _, ok := err.(*NeedsRenewalError); ok {
				renewalTries++
				if renewalTries >= maxRenewals {
					return ErrTooManyRenewals
				}
				hf.log("[%9d-%9d] (Connect) renewing on %v", hr.offset, hr.offset, err)

				err = hr.renewURLWithRetries()
				if err != nil {
					// if we reach this point, we've failed to generate
					// a download URL a bunch of times in a row
					return err
				}
				continue
			} else if hf.shouldRetry(err) {
				hf.log("[%9d-%9d] (Connect) retrying %v", hr.offset, hr.offset, err)
				retryCtx.Retry(err)
				continue
			} else {
				return err
			}
		}

		totalConnDuration := time.Since(startTime)
		hf.log("[%9d-%9d] (Connect) %s", hr.offset, hr.offset, totalConnDuration)
		hf.stats.connections++
		hf.stats.connectionWait += totalConnDuration
		return nil
	}

	return errors.WithMessage(retryCtx.LastError, "httpfile connect")
}

func (hr *httpReader) renewURLWithRetries() error {
	hf := hr.file
	renewRetryCtx := hf.newRetryContext()

	for renewRetryCtx.ShouldTry() {
		var err error
		hf.stats.renews++
		hr.currentURL, err = hf.renewURL()
		if err != nil {
			if hf.shouldRetry(err) {
				hf.log("[%9d-%9d] (Connect) retrying %v", hr.offset, hr.offset, err)
				renewRetryCtx.Retry(err)
				continue
			} else {
				hf.log("[%9d-%9d] (Connect) bailing on %v", hr.offset, hr.offset, err)
				return err
			}
		}

		return nil
	}
	return errors.WithMessage(renewRetryCtx.LastError, "httpfile renew")
}

func (hr *httpReader) tryConnect() error {
	hf := hr.file

	req, err := http.NewRequest("GET", hf.currentURL, nil)
	if err != nil {
		return err
	}

	byteRange := fmt.Sprintf("bytes=%d-", hr.offset)
	req.Header.Set("Range", byteRange)

	res, err := hf.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode == 200 {
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
			return &NeedsRenewalError{url: hf.currentURL}
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
