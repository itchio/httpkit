package uploader

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itchio/httpkit/timeout"
	"github.com/pkg/errors"
)

type resumableUpload struct {
	maxChunkGroup int
	closed        bool
	err           error
	errMu         sync.RWMutex
	pushedErr     chan struct{}
	splitBuf      *bytes.Buffer
	blocks        chan *rblock
	done          chan struct{}
	chunkUploader *chunkUploader
	id            int
}

// ResumableUpload represents a resumable upload session
// to google cloud storage.
type ResumableUpload interface {
	io.WriteCloser
}

type rblock struct {
	data []byte
	last bool
}

const rblockSize = 256 * 1024

var seed atomic.Int64

var _ ResumableUpload = (*resumableUpload)(nil)

// NewResumableUpload starts a new resumable upload session
// targeting the specified Google Cloud Storage uploadURL. Cancelling ctx
// aborts the upload: Write and Close then return ctx's error.
func NewResumableUpload(ctx context.Context, uploadURL string, opts ...Option) ResumableUpload {
	s := defaultSettings()
	for _, o := range opts {
		o.Apply(s)
	}

	id := int(seed.Add(1) - 1)
	chunkUploader := &chunkUploader{
		ctx:              ctx,
		uploadURL:        uploadURL,
		httpClient:       timeout.NewClient(resumableConnectTimeout, resumableIdleTimeout),
		id:               id,
		consumer:         s.Consumer,
		progressListener: s.ProgressListener,
	}

	ru := &resumableUpload{
		maxChunkGroup: s.MaxChunkGroup,

		err:           nil,
		pushedErr:     make(chan struct{}, 0),
		splitBuf:      new(bytes.Buffer),
		blocks:        make(chan *rblock),
		done:          make(chan struct{}, 0),
		chunkUploader: chunkUploader,
		id:            id,
	}
	ru.splitBuf.Grow(rblockSize)

	go ru.work(ctx)

	return ru
}

// Write implements io.Writer.
func (ru *resumableUpload) Write(buf []byte) (int, error) {
	sb := ru.splitBuf

	written := 0
	for written < len(buf) {
		if err := ru.checkError(); err != nil {
			return written, err
		}
		if ru.closed {
			return written, nil
		}

		availRead := len(buf) - written
		availWrite := sb.Cap() - sb.Len()

		if availWrite == 0 {
			// flush!
			if err := ru.sendBlock(sb.Bytes()); err != nil {
				return written, err
			}
			sb.Reset()
			availWrite = sb.Cap()
		}

		copySize := min(availRead, availWrite)

		// buffer!
		sb.Write(buf[written : written+copySize])
		written += copySize
	}

	return written, nil
}

// Close implements io.Closer.
func (ru *resumableUpload) Close() error {
	if err := ru.checkError(); err != nil {
		return errors.Wrapf(err, "in resumableUpload.Close")
	}

	if ru.closed {
		return nil
	}
	ru.closed = true

	// flush!
	if err := ru.sendBlock(ru.splitBuf.Bytes()); err != nil {
		return errors.Wrapf(err, "in resumableUpload.Close")
	}
	close(ru.blocks)

	// wait for work() to be done
	select {
	case <-ru.done: // muffin
	case <-ru.pushedErr: // muffin
	}

	// return any errors
	return ru.checkError()
}

//===========================================
// internal functions
//===========================================

// sendBlock hands a copy of data to work(), unless the upload has
// already failed, in which case nobody is receiving anymore.
func (ru *resumableUpload) sendBlock(data []byte) error {
	block := &rblock{
		data: append([]byte{}, data...),
	}
	select {
	case ru.blocks <- block:
		return nil
	case <-ru.pushedErr:
		return ru.checkError()
	}
}

func (ru *resumableUpload) work(ctx context.Context) {
	defer close(ru.done)

	// stopped before done closes, so a cancel arriving after a finished
	// upload can't turn its success into an error
	stop := context.AfterFunc(ctx, func() {
		ru.pushError(context.Cause(ctx))
	})
	defer stop()

	sendBuf := new(bytes.Buffer)
	sendBuf.Grow(ru.maxChunkGroup * rblockSize)
	var chunkGroupSize int

	// same as ru.blocks, but `.last` is set properly, no matter
	// what the size is
	annotatedBlocks := make(chan *rblock, ru.maxChunkGroup)
	annotated := make(chan struct{})
	defer func() { <-annotated }()
	go func() {
		defer close(annotated)
		defer close(annotatedBlocks)

		// once the upload has failed nobody drains annotatedBlocks
		queue := func(b *rblock) bool {
			select {
			case annotatedBlocks <- b:
				return true
			case <-ru.pushedErr:
				return false
			}
		}

		var lastBlock *rblock
	annotate:
		for {
			select {
			case b := <-ru.blocks:
				if b == nil {
					// ru.blocks closed!
					break annotate
				}

				if lastBlock != nil && !queue(lastBlock) {
					return
				}
				lastBlock = b
			case <-ru.pushedErr:
				// stop everything
				return
			}
		}

		if lastBlock != nil {
			lastBlock.last = true
			queue(lastBlock)
		}
	}()

aggregate:
	for {
		sendBuf.Reset()
		chunkGroupSize = 0

		{
			// do a block receive for the first vlock
			select {
			case <-ru.pushedErr:
				// nevermind, stop everything
				return
			case block := <-annotatedBlocks:
				if block == nil {
					// done receiving blocks!
					break aggregate
				}

				_, err := sendBuf.Write(block.data)
				if err != nil {
					ru.pushError(errors.WithStack(err))
					return
				}
				chunkGroupSize++

				if block.last {
					// done receiving blocks
					break aggregate
				}
			}
		}

		// see if we can't gather any more blocks
	maximize:
		for chunkGroupSize < ru.maxChunkGroup {
			select {
			case <-ru.pushedErr:
				// nevermind, stop everything
				return
			case block := <-annotatedBlocks:
				if block == nil {
					// done receiving blocks!
					break aggregate
				}

				_, err := sendBuf.Write(block.data)
				if err != nil {
					ru.pushError(errors.WithStack(err))
					return
				}
				chunkGroupSize++

				if block.last {
					// done receiving blocks
					break aggregate
				}
			case <-time.After(100 * time.Millisecond):
				// no more blocks available right now, that's ok
				// let's just send what we got
				break maximize
			}
		}

		// send what we have so far
		ru.debugf("Uploading %d chunks", chunkGroupSize)
		err := ru.chunkUploader.put(sendBuf.Bytes(), false)
		if err != nil {
			ru.pushError(errors.WithStack(err))
			return
		}
	}

	// send the last block
	ru.debugf("Uploading last %d chunks", chunkGroupSize)
	err := ru.chunkUploader.put(sendBuf.Bytes(), true)
	if err != nil {
		ru.pushError(errors.WithStack(err))
		return
	}
}

func (ru *resumableUpload) debugf(msg string, args ...any) {
	if c := ru.chunkUploader.consumer; c != nil {
		fmsg := fmt.Sprintf(msg, args...)
		c.Debugf("[ru-%d] %s", ru.id, fmsg)
	}
}

func (ru *resumableUpload) checkError() error {
	ru.errMu.RLock()
	err := ru.err
	ru.errMu.RUnlock()
	return err
}

func (ru *resumableUpload) pushError(err error) {
	ru.errMu.Lock()
	if ru.err != nil {
		ru.errMu.Unlock()
		return
	}
	ru.err = err
	close(ru.pushedErr)
	ru.errMu.Unlock()
}
