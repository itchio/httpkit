package retrycontext_test

import (
	"errors"
	"testing"

	"github.com/alecthomas/assert"
	"github.com/itchio/httpkit/retrycontext"
)

func Test_Retry(t *testing.T) {
	var markerError = errors.New("marker")
	var failCount int

	run := func() error {
		ctx := retrycontext.NewDefault()
		ctx.Settings.NoSleep = true
		ctx.Settings.MaxTries = 3

		for ctx.ShouldTry() {
			if failCount > 0 {
				failCount -= 1
				ctx.Retry("retrying")
				continue
			}

			return nil
		}

		return markerError
	}

	failCount = 0
	assert.NoError(t, run())

	failCount = 1
	assert.NoError(t, run())

	failCount = 2
	assert.NoError(t, run())

	failCount = 3
	assert.Error(t, run())

	failCount = 4
	assert.Error(t, run())
}