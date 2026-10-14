package timeout

import (
	"net"
	"os"
	"sync"
	"time"
)

// bandwidth caps the combined read and write rate of every connection
// this package dials. Connections share one token bucket, so the cap
// applies to the process as a whole rather than per connection.
var bandwidth = &throttle{}

// SetBandwidth caps reads and writes to bytesPerSecond across all
// connections. Anything below 1 lifts the cap. Transfers already
// waiting for bandwidth pick up the new rate within replanInterval.
func SetBandwidth(bytesPerSecond int64) {
	bandwidth.setRate(bytesPerSecond)
}

const (
	maxThrottledChunk = 64 * 1024
	// replanInterval bounds how long a waiting transfer sleeps before it
	// looks again, so a changed rate, a changed deadline, or bandwidth
	// given back by another connection is picked up soon.
	replanInterval = 100 * time.Millisecond
)

type throttle struct {
	mu     sync.Mutex
	rate   int64
	tokens float64
	last   time.Time
	// gen changes with every rate change; a reservation made under an
	// older bucket is neither honored nor refunded against the new one.
	gen uint64
}

type reservation struct {
	n   int
	gen uint64
}

func (t *throttle) setRate(bytesPerSecond int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if bytesPerSecond < 1 {
		bytesPerSecond = 0
	}
	t.rate = bytesPerSecond
	t.tokens = 0
	t.last = time.Now()
	t.gen++
}

// reserve picks how much of want a single transfer may move under the
// current rate, takes that from the bucket (running it into debt if it
// must), and returns the amount with how long the caller has to wait
// for the debt to clear.
func (t *throttle) reserve(want int) (reservation, time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rate == 0 {
		return reservation{n: want, gen: t.gen}, 0
	}
	n := max(1, min(want, int(min(t.rate/4, maxThrottledChunk))))

	now := time.Now()
	t.tokens += now.Sub(t.last).Seconds() * float64(t.rate)
	t.last = now
	t.tokens = min(t.tokens, float64(t.rate))
	t.tokens -= float64(n)
	if t.tokens >= 0 {
		return reservation{n: n, gen: t.gen}, 0
	}
	return reservation{n: n, gen: t.gen}, time.Duration(-t.tokens / float64(t.rate) * float64(time.Second))
}

// current reports whether r was made against the bucket as it is now.
func (t *throttle) current(r reservation) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return r.gen == t.gen
}

func (t *throttle) refund(r reservation, n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.rate == 0 || r.gen != t.gen {
		return
	}
	t.tokens += float64(n)
}

type throttledConn struct {
	net.Conn
	t         *throttle
	closed    chan struct{}
	closeOnce sync.Once

	mu            sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

func (t *throttle) conn(c net.Conn) *throttledConn {
	return &throttledConn{Conn: c, t: t, closed: make(chan struct{})}
}

func (c *throttledConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return c.Conn.Read(b)
	}
	r, err := c.wait(len(b), c.currentReadDeadline)
	if err != nil {
		return 0, err
	}
	got, err := c.Conn.Read(b[:r.n])
	if got < r.n {
		c.t.refund(r, r.n-got)
	}
	return got, err
}

func (c *throttledConn) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return c.Conn.Write(b)
	}
	written := 0
	for written < len(b) {
		r, err := c.wait(len(b)-written, c.currentWriteDeadline)
		if err != nil {
			return written, err
		}
		m, err := c.Conn.Write(b[written : written+r.n])
		written += m
		if m < r.n {
			c.t.refund(r, r.n-m)
		}
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// wait blocks until some of want bytes may be moved and returns the
// reservation for them. It gives up when the connection is closed or
// the deadline passes, and re-plans every replanInterval so the
// reservation tracks the bucket.
func (c *throttledConn) wait(want int, deadline func() time.Time) (reservation, error) {
	for {
		r, delay := c.t.reserve(want)
		if delay == 0 {
			return r, nil
		}

		slice := min(delay, replanInterval)
		if dl := deadline(); !dl.IsZero() {
			left := time.Until(dl)
			if left <= 0 {
				c.t.refund(r, r.n)
				return reservation{}, os.ErrDeadlineExceeded
			}
			slice = min(slice, left)
		}

		timer := time.NewTimer(slice)
		select {
		case <-timer.C:
			if slice == delay && c.t.current(r) {
				return r, nil
			}
			c.t.refund(r, r.n)
		case <-c.closed:
			timer.Stop()
			c.t.refund(r, r.n)
			return reservation{}, net.ErrClosed
		}
	}
}

func (c *throttledConn) currentReadDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readDeadline
}

func (c *throttledConn) currentWriteDeadline() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeDeadline
}

func (c *throttledConn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.writeDeadline = t
	c.mu.Unlock()
	return c.Conn.SetDeadline(t)
}

func (c *throttledConn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	c.readDeadline = t
	c.mu.Unlock()
	return c.Conn.SetReadDeadline(t)
}

func (c *throttledConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	c.writeDeadline = t
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(t)
}

func (c *throttledConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Conn.Close()
}
