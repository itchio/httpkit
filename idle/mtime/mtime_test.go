package mtime

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestInstant(t *testing.T) {
	delta := 1 * time.Millisecond
	i1 := Now()
	i2 := i1.Add(delta)
	assert.True(t, i2 > i1)
	assert.Equal(t, i1, i2.Add(-1*delta))
	assert.Equal(t, delta, i2.Sub(i1))
}

func TestStopwatch(t *testing.T) {
	elapsed := Stopwatch()
	time.Sleep(100 * time.Millisecond)
	e1 := elapsed()
	time.Sleep(100 * time.Millisecond)
	e2 := elapsed()
	assert.True(t, e2 > e1)
	assert.True(t, e1 > 50*time.Millisecond)
	assert.True(t, e2 > 100*time.Millisecond)
}
