package retrycontext_test

import (
	"math"
	"testing"
	"time"

	"github.com/itchio/httpkit/retrycontext"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_Retry(t *testing.T) {
	assert := assert.New(t)
	var markerError = errors.New("marker")
	var failCount int
	var totalSleep time.Duration = 0

	run := func() error {
		totalSleep = 0
		ctx := retrycontext.NewDefault()
		ctx.Settings.NoSleep = true
		ctx.Settings.FakeSleep = func(d time.Duration) {
			totalSleep += d
		}
		ctx.Settings.MaxTries = 3

		count := failCount
		for ctx.ShouldTry() {
			if count > 0 {
				count -= 1
				ctx.Retry(errors.Errorf("retrying"))
				continue
			}

			return nil
		}

		return markerError
	}

	failCount = 0
	assert.NoError(run())

	sleepLowerBound := func(failCount int) time.Duration {
		var bound time.Duration
		for i := 0; i < failCount; i++ {
			n := int(math.Pow(2, float64(i)))
			bound += time.Second * time.Duration(n)
		}
		return bound
	}
	sleepUpperBound := func(failCount int) time.Duration {
		// 1s of jitter per retry
		return sleepLowerBound(failCount) + time.Second*time.Duration(failCount)
	}

	checkBounds := func(failCount int, totalSleep time.Duration) {
		t.Helper()
		lower := sleepLowerBound(failCount)
		upper := sleepUpperBound(failCount)
		t.Logf("Expecting %s <= %s <= %s", lower, totalSleep, upper)
		assert.True(totalSleep >= lower)
		assert.True(totalSleep <= upper)
	}

	failCount = 1
	assert.NoError(run())
	checkBounds(failCount, totalSleep)

	failCount = 2
	assert.NoError(run())
	checkBounds(failCount, totalSleep)

	failCount = 3
	assert.EqualError(run(), markerError.Error())
	checkBounds(failCount, totalSleep)

	failCount = 4
	assert.EqualError(run(), markerError.Error())
}
