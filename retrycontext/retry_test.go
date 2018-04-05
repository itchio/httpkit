package retrycontext_test

import (
	"testing"

	"github.com/itchio/httpkit/retrycontext"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
				ctx.Retry(errors.Errorf("retrying"))
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
