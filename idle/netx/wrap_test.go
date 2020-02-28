package netx

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

type connWrap struct {
	net.Conn
	val     int
	wrapped net.Conn
}

func (c *connWrap) Wrapped() net.Conn {
	return c.wrapped
}

func TestWalkWrapped(t *testing.T) {
	c := &connWrap{val: 4}
	c = &connWrap{val: 3, wrapped: c}
	c = &connWrap{val: 2, wrapped: c}
	c = &connWrap{val: 1, wrapped: c}
	var result []int
	WalkWrapped(c, func(conn net.Conn) bool {
		switch t := conn.(type) {
		case *connWrap:
			result = append(result, t.val)
			return t.val < 3
		}
		return true
	})
	assert.EqualValues(t, []int{1, 2, 3}, result)
}

func TestWalkWrappedFirst(t *testing.T) {
	var c net.Conn
	gotFirst := false
	WalkWrapped(c, func(conn net.Conn) bool {
		if conn == nil {
			gotFirst = true
		}
		return true
	})
	assert.True(t, gotFirst)
}
