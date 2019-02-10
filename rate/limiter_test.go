package rate_test

import (
	"sync"
	"testing"
	"time"

	"github.com/itchio/httpkit/rate"
	"github.com/stretchr/testify/assert"
)

func Test_Limiter(t *testing.T) {
	assert := assert.New(t)
	rps := 50
	limiter := rate.NewLimiter(rate.LimiterOpts{
		RequestsPerSecond: rps,
	})

	start := time.Now()
	var wg sync.WaitGroup
	doOne := func() {
		limiter.Wait()
		wg.Done()
	}

	numReqs := 25
	wg.Add(numReqs)
	for i := 0; i < numReqs; i++ {
		go doOne()
	}
	wg.Wait()

	secondsElapsed := time.Since(start).Seconds()
	rpsComputed := int(float64(numReqs) / secondsElapsed)
	t.Logf("Should be doing %d rps, did %d rps", rps, rpsComputed)
	assert.True(rpsComputed <= rps, "should not have been going faster than limit")
}
