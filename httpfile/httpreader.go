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

var discardBuf = make([]byte, 4096)

func (hr *httpReader) Discard(n int64) (int, error) {
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
				hf.log("[%9d-%9d] (Connect) renewing on %v", hr.offset, hr.offset, err)

				err = func() error {
					renewRetryCtx := hf.newRetryContext()

					for renewRetryCtx.ShouldTry() {
						hf.stats.renews++
						urlStr, err = hf.renewURL()
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
				}()
				if err != nil {
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
