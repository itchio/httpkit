package uploader

import "github.com/itchio/headway/state"

type settings struct {
	MaxChunkGroup    int
	Consumer         *state.Consumer
	ProgressListener ProgressListenerFunc
}

func defaultSettings() *settings {
	return &settings{
		// 64 * 256KiB = 16MiB
		MaxChunkGroup: 64,
	}
}

type Option interface {
	Apply(s *settings)
}

// ---------

type maxChunkGroupOption struct {
	maxChunkGroup int
}

// WithMaxChunkGroup specifies how many 256KiB chunks can be
// uploaded at a time. Raising this value may increase RAM
// usage, and cost more if we have to retry blocks.
//
// The default value is 64 (16MiB groups)
func WithMaxChunkGroup(maxChunkGroup int) *maxChunkGroupOption {
	return &maxChunkGroupOption{
		maxChunkGroup: maxChunkGroup,
	}
}

func (o *maxChunkGroupOption) Apply(s *settings) {
	s.MaxChunkGroup = o.maxChunkGroup
}

// ---------

type consumerOption struct {
	consumer *state.Consumer
}

// WithConsumer receives the upload's debug log.
func WithConsumer(consumer *state.Consumer) *consumerOption {
	return &consumerOption{consumer: consumer}
}

func (o *consumerOption) Apply(s *settings) {
	s.Consumer = o.consumer
}

// ---------

type progressListenerOption struct {
	progressListener ProgressListenerFunc
}

// WithProgressListener is called with the number of bytes sent so far,
// from the upload's own goroutine.
func WithProgressListener(progressListener ProgressListenerFunc) *progressListenerOption {
	return &progressListenerOption{progressListener: progressListener}
}

func (o *progressListenerOption) Apply(s *settings) {
	s.ProgressListener = o.progressListener
}
