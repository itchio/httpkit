package timeout_test

import (
	"crypto/tls"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/itchio/httpkit/timeout"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/http2"
)

func Test_HTTP2(t *testing.T) {
	assert := assert.New(t)

	// Create an HTTPS test server with HTTP/2 enabled
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify the server received the request over HTTP/2
		assert.Equal("HTTP/2.0", r.Proto, "server should receive HTTP/2 request")

		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.Write([]byte("ok"))
	}))

	// Enable HTTP/2 on the server
	http2.ConfigureServer(server.Config, nil)
	server.TLS = &tls.Config{
		NextProtos: []string{"h2", "http/1.1"},
	}
	server.StartTLS()
	defer server.Close()

	// Create a client with HTTP/2 support that trusts the test server's self-signed cert
	client := timeout.NewDefaultClient()
	transport := client.Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2", "http/1.1"},
	}
	http2.ConfigureTransport(transport)
	client.Transport = transport

	res, err := client.Get(server.URL)
	assert.NoError(err)
	assert.EqualValues(200, res.StatusCode)
	defer res.Body.Close()

	// Verify client received response over HTTP/2
	assert.Equal("HTTP/2.0", res.Proto, "client should receive HTTP/2 response")

	body, err := io.ReadAll(res.Body)
	assert.NoError(err)
	assert.Equal("ok", string(body))
}
