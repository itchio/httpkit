package backtracker_test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/itchio/httpkit/httpfile/backtracker"
	"github.com/stretchr/testify/assert"
)

func Test_BackTrackerTiny(t *testing.T) {
	var buf []byte
	for i := 0; i < 16; i++ {
		buf = append(buf, byte(i))
	}

	bt := backtracker.New(0, bytes.NewReader(buf), 2)

	tinybuf := make([]byte, 1)
	readOne := func(expected byte) {
		t.Helper()
		n, err := bt.Read(tinybuf)
		assert.NoError(t, err)
		assert.EqualValues(t, 1, n)
		assert.EqualValues(t, expected, tinybuf[0], "read value %d", expected)
	}

	backtrack := func(n int64) {
		t.Helper()
		assert.NoError(t, bt.Backtrack(n), "can backtrack %d", n)
	}

	hasOffset := func(n int64) {
		t.Helper()
		assert.EqualValues(t, n, bt.Offset(), "has offset %d", n)
	}

	hasCache := func(n int64) {
		t.Helper()
		assert.EqualValues(t, n, bt.Cached(), "has %d bytes cached", n)
	}

	readOne(0)
	hasOffset(1)
	hasCache(1)

	readOne(1)
	hasOffset(2)
	hasCache(2)

	backtrack(1)
	hasOffset(2)
	readOne(1)

	backtrack(2)
	hasOffset(2)
	readOne(0)
	readOne(1)

	fivebuf := make([]byte, 5)
	backtrack(2)
	_, err := io.ReadFull(bt, fivebuf)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0, 1, 2, 3, 4}, fivebuf)

	hasOffset(5)
	hasCache(2)

	backtrack(2)
	hasOffset(5)
	hasCache(2)

	readOne(3)
	readOne(4)

	err = bt.Discard(5)
	assert.NoError(t, err)

	hasOffset(10)

	finalbuf, err := ioutil.ReadAll(bt)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{10, 11, 12, 13, 14, 15}, finalbuf)
}

func Test_BacktrackerNoCache(t *testing.T) {
	var buf []byte
	for i := 0; i < 16; i++ {
		buf = append(buf, byte(i))
	}

	bt := backtracker.New(0, bytes.NewReader(buf), 0)

	buf2, err := ioutil.ReadAll(bt)
	assert.NoError(t, err)
	assert.EqualValues(t, buf, buf2)
}

func Test_BacktrackerRidiculousCache(t *testing.T) {
	var buf []byte
	for i := 0; i < 64; i++ {
		buf = append(buf, byte(i))
	}

	bt := backtracker.New(17, bytes.NewReader(buf), 1*1024*1024)

	bb := make([]byte, 7)
	off := 0
	for {
		_, err := io.ReadFull(bt, bb)

		if err == io.ErrUnexpectedEOF {
			break
		} else {
			assert.NoError(t, err)
			assert.EqualValues(t, buf[off:off+len(bb)], bb)
		}

		err = bt.Backtrack(1)
		assert.NoError(t, err)
		off += len(bb) - 1
	}
}

func Test_BackTrackerLarge(t *testing.T) {
	K := 1024
	MB := K * K

	buf := make([]byte, 4*MB)
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, err := prng.Read(buf)
	assert.NoError(t, err)

	bt := backtracker.New(0, bytes.NewReader(buf), int64(127*K))

	err = bt.Discard(int64(512 * K))
	assert.NoError(t, err)

	b := make([]byte, 32)
	_, err = io.ReadFull(bt, b)
	assert.NoError(t, err)
	assert.EqualValues(t, buf[512*K:512*K+len(b)], b)

	off := 512*K + len(b)

	{
		woff := 16*K + 4649
		err = bt.Backtrack(int64(woff))
		_, err = io.ReadFull(bt, b)
		assert.NoError(t, err)
		assert.EqualValues(t, buf[off-woff:off-woff+len(b)], b)
	}

	{
		woff := 32*K + 3517
		err = bt.Backtrack(int64(woff))
		_, err = io.ReadFull(bt, b)
		assert.NoError(t, err)
		assert.EqualValues(t, buf[off-woff:off-woff+len(b)], b)
	}

	err = bt.Backtrack(int64(128 * K))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "can't backtrack")

	err = bt.Backtrack(0)
	assert.NoError(t, err)
	assert.EqualValues(t, off, bt.Offset())

	{
		woff := 2*MB + 6991

		err = bt.Discard(int64(woff))
		assert.NoError(t, err)

		off += woff
		assert.EqualValues(t, off, bt.Offset())

		_, err = io.ReadFull(bt, b)
		assert.NoError(t, err)
		assert.EqualValues(t, buf[off:off+len(b)], b)
	}

	err = bt.Discard(int64(8 * MB))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "EOF")
}

func Test_BacktrackerOffset(t *testing.T) {
	bt := backtracker.New(4, bytes.NewReader([]byte{4, 5, 6, 7}), 2)
	assert.EqualValues(t, 4, bt.Offset())

	buf, err := ioutil.ReadAll(bt)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{4, 5, 6, 7}, buf)
}
