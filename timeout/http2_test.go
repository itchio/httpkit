package timeout_test

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/itchio/httpkit/timeout"
	"github.com/stretchr/testify/assert"
)

func Test_HTTP2(t *testing.T) {
	assert := assert.New(t)
	c := timeout.NewDefaultClient()
	res, err := c.Get("https://http2.pro/api/v1")
	assert.NoError(err)
	assert.EqualValues(200, res.StatusCode)
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	assert.NoError(err)

	type ApiRes struct {
		HTTP2    int    `json:"http2"`
		Protocol string `json:"protocol"`
		// ignore other fields
	}

	var apiRes ApiRes
	err = json.Unmarshal(body, &apiRes)
	assert.NoError(err)

	assert.EqualValues(1, apiRes.HTTP2)
	assert.EqualValues("HTTP/2.0", apiRes.Protocol)
}
