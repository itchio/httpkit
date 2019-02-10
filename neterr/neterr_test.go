package neterr_test

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/itchio/httpkit/htfs"
	"github.com/itchio/httpkit/retrycontext"
	"github.com/itchio/httpkit/timeout"

	"github.com/itchio/httpkit/neterr"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_TcpDial(t *testing.T) {
	assert := assert.New(t)
	var err error
	assert.False(neterr.IsNetworkError(err))

	_, err = net.DialTimeout("tcp", "localhost:1", 100*time.Millisecond)
	t.Logf("%v", err)
	assert.True(neterr.IsNetworkError(err))
	assert.True(neterr.IsNetworkError(errors.WithStack(err)))

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	get := func(url string) (*http.Response, error) {
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return nil, err
		}

		return client.Do(req)
	}

	_, err = get("http://localhost:1/hi")
	t.Logf("%v", err)
	assert.True(neterr.IsNetworkError(err))
	assert.True(neterr.IsNetworkError(errors.WithStack(err)))

	_, err = get("http://no.example.org")
	t.Logf("%v", err)
	assert.True(neterr.IsNetworkError(err))
	assert.True(neterr.IsNetworkError(errors.WithStack(err)))

	req, err := http.NewRequest("GET", "http://example.org/", nil)
	assert.NoError(err)

	client = &http.Client{
		Timeout: 200 * time.Millisecond,
		Transport: &http.Transport{
			Dial: func(network string, addr string) (net.Conn, error) {
				for {
					time.Sleep(1 * time.Second)
				}
			},
		},
	}
	_, err = client.Do(req)
	t.Logf("%v", err)
	assert.True(neterr.IsNetworkError(err))
	assert.True(neterr.IsNetworkError(errors.WithStack(err)))
}

func Test_File(t *testing.T) {
	assert := assert.New(t)
	_, err := htfs.Open(func() (string, error) {
		return "http://no.example.org", nil
	}, func(res *http.Response, body []byte) bool {
		return false
	}, &htfs.Settings{
		Client: timeout.NewClient(time.Second, time.Second),
		RetrySettings: &retrycontext.Settings{
			MaxTries: 2,
		},
	})
	t.Logf("%+v", err)
	assert.True(neterr.IsNetworkError(err))
}

func Test_UnexpectedEof(t *testing.T) {
	assert := assert.New(t)
	l, err := net.Listen("tcp", "localhost:0")
	assert.NoError(err)

	go func() {
		conn, err := l.Accept()
		assert.NoError(err)

		_, err = conn.Write([]byte{1, 2, 3})
		assert.NoError(err)

		err = conn.Close()

		assert.NoError(err)
	}()

	conn, err := net.DialTimeout("tcp", l.Addr().String(), 100*time.Millisecond)
	assert.NoError(err)

	buf := make([]byte, 4)
	_, err = io.ReadFull(conn, buf)
	t.Logf("%v", err)
	assert.True(neterr.IsNetworkError(err))
}
