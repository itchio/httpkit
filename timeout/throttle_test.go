package timeout

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// slowPair returns a connection throttled so hard that any read or
// write of a few bytes has to wait.
func slowPair(t *testing.T) (*throttledConn, net.Conn) {
	th := &throttle{}
	th.setRate(1)
	client, server := net.Pipe()
	t.Cleanup(func() { server.Close() })
	c := th.conn(client)
	t.Cleanup(func() { c.Close() })
	return c, server
}

func TestThrottledReadUnblocksOnClose(t *testing.T) {
	c, server := slowPair(t)
	go server.Write(make([]byte, 16))

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 16))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	c.Close()

	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not return after close")
	}
}

func TestThrottledWriteUnblocksOnClose(t *testing.T) {
	c, server := slowPair(t)
	go func() {
		buf := make([]byte, 16)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()

	done := make(chan error, 1)
	go func() {
		_, err := c.Write(make([]byte, 16))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	c.Close()

	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("expected net.ErrClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write did not return after close")
	}
}

func TestThrottleUnlimitedPassesWholeReads(t *testing.T) {
	th := &throttle{}
	client, server := net.Pipe()
	defer server.Close()
	c := th.conn(client)
	defer c.Close()

	go server.Write(make([]byte, maxThrottledChunk*2))

	buf := make([]byte, maxThrottledChunk*2)
	n, err := c.Read(buf)
	if err != nil || n != len(buf) {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestThrottlePacesReads(t *testing.T) {
	th := &throttle{}
	th.setRate(64 * 1024)
	client, server := net.Pipe()
	defer server.Close()
	c := th.conn(client)
	defer c.Close()

	const total = 96 * 1024
	go func() {
		buf := make([]byte, 4096)
		for sent := 0; sent < total; {
			n, err := server.Write(buf)
			if err != nil {
				return
			}
			sent += n
		}
	}()

	start := time.Now()
	buf := make([]byte, 32*1024)
	for got := 0; got < total; {
		n, err := c.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got += n
	}
	// the bucket starts empty, so 96 KiB at 64 KiB/s is about 1.5s
	if took := time.Since(start); took < time.Second {
		t.Fatalf("96 KiB at 64 KiB/s finished in %s", took)
	}
}

func TestThrottlePacesWrites(t *testing.T) {
	th := &throttle{}
	th.setRate(64 * 1024)
	client, server := net.Pipe()
	defer server.Close()
	c := th.conn(client)
	defer c.Close()

	const total = 96 * 1024
	go func() {
		buf := make([]byte, 4096)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()

	start := time.Now()
	n, err := c.Write(make([]byte, total))
	if err != nil || n != total {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if took := time.Since(start); took < time.Second {
		t.Fatalf("96 KiB at 64 KiB/s finished in %s", took)
	}
}

func TestThrottledReadHonorsReadDeadline(t *testing.T) {
	c, server := slowPair(t)
	go server.Write(make([]byte, 16))

	c.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
	start := time.Now()
	_, err := c.Read(make([]byte, 16))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	var nerr net.Error
	if !errors.As(err, &nerr) || !nerr.Timeout() {
		t.Fatalf("deadline error should be a net.Error timeout: %v", err)
	}
	// the reservation alone would have slept 16s
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("read waited %s past a 10ms deadline", took)
	}
}

func TestThrottledReadNoticesLaterDeadline(t *testing.T) {
	c, server := slowPair(t)
	go server.Write(make([]byte, 16))

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 16))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	c.SetReadDeadline(time.Now())
	select {
	case err := <-done:
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("expected deadline error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read did not notice the new deadline")
	}
}

func TestLiftingTheCapReleasesPendingReads(t *testing.T) {
	c, server := slowPair(t)
	go server.Write(make([]byte, 16))

	done := make(chan error, 1)
	go func() {
		_, err := c.Read(make([]byte, 16))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	c.t.setRate(0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read stayed throttled after the cap was lifted")
	}
}

func TestClosingOneConnFreesBandwidthForAnother(t *testing.T) {
	th := &throttle{}
	th.setRate(16)
	open := func() (*throttledConn, net.Conn) {
		client, server := net.Pipe()
		t.Cleanup(func() { server.Close() })
		c := th.conn(client)
		t.Cleanup(func() { c.Close() })
		return c, server
	}
	hog, hogServer := open()
	go hogServer.Write(make([]byte, 64))
	other, otherServer := open()
	go otherServer.Write(make([]byte, 4))

	// the hog reserves four seconds of bandwidth, the other queues behind it
	go hog.Read(make([]byte, 64))
	time.Sleep(50 * time.Millisecond)
	done := make(chan error, 1)
	go func() {
		_, err := other.Read(make([]byte, 4))
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	hog.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read kept waiting on bandwidth a closed connection gave back")
	}
}

func TestThrottledZeroLengthRead(t *testing.T) {
	c, server := slowPair(t)
	go server.Write(make([]byte, 1))

	n, err := c.Read(nil)
	if n != 0 || err != nil {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestStaleReservationsDoNotCreditNewBucket(t *testing.T) {
	th := &throttle{}
	th.setRate(1)
	r, _ := th.reserve(64)
	th.setRate(1024)
	th.refund(r, 64)
	if th.tokens != 0 {
		t.Fatalf("new bucket holds %v tokens from the old rate", th.tokens)
	}
	if th.current(r) {
		t.Fatal("a reservation from before the rate change must not be honored")
	}
}
