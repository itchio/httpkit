package uploader

import (
	"bytes"
	"io"

	"github.com/go-errors/errors"
	"github.com/itchio/httpkit/timeout"
	"github.com/itchio/wharf/state"
)

type resumableUpload2 struct {
	maxChunkGroup    int
	consumer         *state.Consumer
	progressListener ProgressListenerFunc

	err           error
	splitBuf      bytes.Buffer
	blocks        chan *rblock
	done          chan struct{}
	cancel        chan struct{}
	chunkUploader *chunkUploader
}

type ResumableUpload2 interface {
	io.WriteCloser
	SetConsumer(consumer *state.Consumer)
	SetProgressListener(progressListener ProgressListenerFunc)
}

type rblock struct {
	data []byte
	last bool
}

const rblockSize = 256 * 1024

var _ ResumableUpload2 = (*resumableUpload2)(nil)

func NewResumableUpload2(uploadURL string) ResumableUpload2 {
	// 64 * 256KiB = 16MiB
	const maxChunkGroup = 64

	chunkUploader := &chunkUploader{
		uploadURL:  uploadURL,
		httpClient: timeout.NewClient(resumableConnectTimeout, resumableIdleTimeout),
	}

	ru := &resumableUpload2{
		maxChunkGroup: maxChunkGroup,

		err:           nil,
		blocks:        make(chan *rblock, maxChunkGroup),
		done:          make(chan struct{}),
		cancel:        make(chan struct{}),
		chunkUploader: chunkUploader,
	}
	ru.splitBuf.Grow(rblockSize)

	go ru.work()

	return ru
}

func (ru *resumableUpload2) Write(buf []byte) (int, error) {
	sb := ru.splitBuf

	written := 0
	for written < len(buf) {
		if ru.err != nil {
			close(ru.cancel)
			return 0, ru.err
		}

		availRead := len(buf) - written
		availWrite := sb.Cap() - sb.Len()

		if availWrite == 0 {
			// flush!
			ru.blocks <- &rblock{
				// clone slice
				data: append([]byte{}, sb.Bytes()...),
				last: false,
			}
			sb.Reset()
			availWrite = sb.Cap()
		}

		copySize := availRead
		if copySize > availWrite {
			copySize = availWrite
		}

		// buffer!
		bufferedSize, err := sb.Write(buf[written : written+copySize])
		written += bufferedSize
		if err != nil {
			ru.err = err
			close(ru.cancel)
			return written, ru.err
		}
	}

	return written, nil
}

func (ru *resumableUpload2) Close() error {
	if ru.err != nil {
		close(ru.cancel)
		return ru.err
	}

	// flush!
	ru.blocks <- &rblock{
		data: ru.splitBuf.Bytes(),
		last: true,
	}
	close(ru.blocks)

	// wait for work() to be done
	<-ru.done

	// return any errors
	if ru.err != nil {
		// no need to bother cancelling anymore, work() has returned
		return ru.err
	}
	return nil
}

func (ru *resumableUpload2) SetConsumer(consumer *state.Consumer) {
	ru.consumer = consumer
	ru.chunkUploader.consumer = consumer
}

func (ru *resumableUpload2) SetProgressListener(progressListener ProgressListenerFunc) {
	ru.chunkUploader.progressListener = progressListener
}

//===========================================
// internal functions
//===========================================

func (ru *resumableUpload2) work() {
	defer close(ru.done)

	sendBuf := new(bytes.Buffer)
	sendBuf.Grow(ru.maxChunkGroup * rblockSize)
	var chunkGroupSize int

scan:
	for {
		chunkGroupSize = 0

		if sendBuf.Len() == 0 {
			// do a block receive for the first vlock
			select {
			case <-ru.cancel:
				// nevermind, stop everything
				return
			case block := <-ru.blocks:
				if block == nil {
					// done receiving blocks!
					break scan
				}

				_, err := sendBuf.Write(block.data)
				if err != nil {
					ru.err = errors.Wrap(err, 0)
					return
				}
				chunkGroupSize++

				if block.last {
					break scan
				}
			}
		}

		// see if we can't gather any more blocks
	aggregate:
		for chunkGroupSize < ru.maxChunkGroup {
			select {
			case <-ru.cancel:
				// nevermind, stop everything
				return
			case block := <-ru.blocks:
				if block == nil {
					// done receiving blocks!
					break scan
				}

				_, err := sendBuf.Write(block.data)
				if err != nil {
					ru.err = errors.Wrap(err, 0)
					return
				}
				chunkGroupSize++

				if block.last {
					break scan
				}
			default:
				// no more blocks available right now, that's ok
				break aggregate
			}
		}

		// send non-last block
		ru.debugf("Uploading %d chunks", chunkGroupSize)
		err := ru.chunkUploader.put(sendBuf.Bytes(), true)
		if err != nil {
			ru.err = errors.Wrap(err, 0)
			return
		}

		sendBuf.Reset()
	}

	// send the last block
	ru.debugf("Uploading last %d chunks", chunkGroupSize)
	err := ru.chunkUploader.put(sendBuf.Bytes(), true)
	if err != nil {
		ru.err = errors.Wrap(err, 0)
		return
	}
}

func (ru *resumableUpload2) debugf(msg string, args ...interface{}) {
	if ru.consumer != nil {
		ru.consumer.Debugf(msg, args...)
	}
}
