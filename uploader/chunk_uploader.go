package uploader

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/go-errors/errors"
	"github.com/itchio/httpkit/retrycontext"
	"github.com/itchio/wharf/counter"
	"github.com/itchio/wharf/state"
)

type ProgressListenerFunc func(count int64)

type chunkUploader struct {
	// constructor
	uploadURL  string
	httpClient *http.Client

	// set later
	progressListener ProgressListenerFunc
	consumer         *state.Consumer

	// internal
	offset int64
	total  int64
}

func (cu *chunkUploader) put(buf []byte, last bool) error {
	retryCtx := cu.newRetryContext()

	for retryCtx.ShouldTry() {
		err := cu.tryPut(buf, last)
		if err != nil {
			if ne, ok := err.(*netError); ok {
				retryCtx.Retry(ne.Error())
				continue
			} else if re, ok := err.(*retryError); ok {
				cu.offset += re.committedBytes
				buf = buf[re.committedBytes:]
				retryCtx.Retry("Having troubles uploading some blocks")
				continue
			} else {
				return errors.Wrap(err, 0)
			}
		} else {
			cu.offset += int64(len(buf))
			return nil
		}
	}

	return fmt.Errorf("Too many errors, stopping upload")
}

func (cu *chunkUploader) tryPut(buf []byte, last bool) error {
	buflen := int64(len(buf))
	if !last && buflen%gcsChunkSize != 0 {
		err := fmt.Errorf("internal error: trying to upload non-last buffer of %d bytes (not a multiple of chunk size %d)",
			buflen, gcsChunkSize)
		return errors.Wrap(err, 0)
	}

	cu.debugf("uploading chunk of %d bytes", buflen)

	body := bytes.NewReader(buf)
	countingReader := counter.NewReaderCallback(func(count int64) {
		if cu.progressListener != nil {
			cu.progressListener(cu.offset + count)
		}
	}, body)

	req, err := http.NewRequest("PUT", cu.uploadURL, countingReader)
	if err != nil {
		// does not include HTTP errors, more like golang API usage errors
		return errors.Wrap(err, 0)
	}

	start := cu.offset
	end := start + buflen - 1
	contentRange := fmt.Sprintf("bytes %d-%d/*", cu.offset, end)

	if last {
		// send total size
		totalSize := cu.offset + buflen
		contentRange = fmt.Sprintf("bytes %d-%d/%d", cu.offset, end, totalSize)
	}

	req.Header.Set("content-range", contentRange)
	req.ContentLength = buflen
	cu.debugf("uploading %d-%d, last? %v, content-length set to %d", start, end, last, req.ContentLength)

	startTime := time.Now()

	res, err := cu.httpClient.Do(req)
	if err != nil {
		cu.debugf("while uploading %d-%d: \n%s", start, end, err.Error())
		return &netError{err, GcsUnknown}
	}

	cu.debugf("server replied in %s, with status %s", time.Since(startTime), res.Status)

	status := interpretGcsStatusCode(res.StatusCode)
	if status == GcsUploadComplete && last {
		cu.debugf("upload complete!")
		return nil
	}

	if status == GcsNeedQuery {
		cu.debugf("need to query upload status (HTTP %s)", res.Status)
		statusRes, err := cu.queryStatus()
		if err != nil {
			// this happens after we retry the query a few times
			return err
		}

		if statusRes.StatusCode == 308 {
			cu.debugf("got upload status, trying to resume")
			res = statusRes
			status = GcsResume
		} else {
			status = interpretGcsStatusCode(statusRes.StatusCode)
			err = fmt.Errorf("expected upload status, got HTTP %s (%s) instead", statusRes.Status, status)
			cu.debugf(err.Error())
			return err
		}
	}

	if status == GcsResume {
		expectedOffset := cu.offset + buflen
		rangeHeader := res.Header.Get("Range")
		if rangeHeader == "" {
			cu.debugf("commit failed (null range), retrying")
			return &retryError{committedBytes: 0}
		}

		committedRange, err := parseRangeHeader(rangeHeader)
		if err != nil {
			return err
		}

		cu.debugf("got resume, expectedOffset: %d, committedRange: %s", expectedOffset, committedRange)
		if committedRange.start != 0 {
			return fmt.Errorf("upload failed: beginning not committed somehow (committed range: %s)", committedRange)
		}

		if committedRange.end == expectedOffset {
			cu.debugf("commit succeeded (%d blocks stored)", buflen/gcsChunkSize)
			return nil
		}

		committedBytes := committedRange.end - cu.offset
		if committedBytes < 0 {
			return fmt.Errorf("upload failed: committed negative bytes somehow (committed range: %s, expectedOffset: %d)", committedRange, expectedOffset)
		}

		if committedBytes > 0 {
			cu.debugf("commit partially succeeded (committed %d / %d byte, %d blocks)", committedBytes, buflen, committedBytes/gcsChunkSize)
			return &retryError{committedBytes}
		}

		cu.debugf("commit failed (retrying %d blocks)", buflen/gcsChunkSize)
		return &retryError{committedBytes}
	}

	return fmt.Errorf("got HTTP %d (%s)", res.StatusCode, status)
}

func (cu *chunkUploader) queryStatus() (*http.Response, error) {
	cu.debugf("querying upload status...")

	retryCtx := cu.newRetryContext()
	for retryCtx.ShouldTry() {
		res, err := cu.tryQueryStatus()
		if err != nil {
			cu.debugf("while querying status of upload: %s", err.Error())
			retryCtx.Retry(err.Error())
			continue
		}

		return res, nil
	}

	return nil, fmt.Errorf("gave up on trying to get upload status")
}

func (cu *chunkUploader) tryQueryStatus() (*http.Response, error) {
	req, err := http.NewRequest("PUT", cu.uploadURL, nil)
	if err != nil {
		// does not include HTTP errors, more like golang API usage errors
		return nil, errors.Wrap(err, 0)
	}

	// for resumable uploads of unknown size, the length is unknown,
	// see https://github.com/itchio/butler/issues/71#issuecomment-242938495
	req.Header.Set("content-range", "bytes */*")

	res, err := cu.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, 0)
	}

	status := interpretGcsStatusCode(res.StatusCode)
	if status == GcsResume {
		// got what we wanted (Range header, etc.)
		return res, nil
	}

	return nil, fmt.Errorf("while querying status, got HTTP %s (status %s)", res.Status, status)
}

func (cu *chunkUploader) debugf(msg string, args ...interface{}) {
	if cu.consumer != nil {
		cu.consumer.Debugf(msg, args...)
	}
}

func (cu *chunkUploader) newRetryContext() *retrycontext.Context {
	return retrycontext.New(retrycontext.Settings{
		MaxTries: resumableMaxRetries,
		Consumer: cu.consumer,
	})
}
