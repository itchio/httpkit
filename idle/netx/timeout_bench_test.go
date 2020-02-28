package netx

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"strings"
	"testing"
)

const (
	ioTimeout       = "i/o timeout"
	ioTimeoutLength = 11
)

type timeouterror struct {
}

func (t *timeouterror) Error() string {
	return ioTimeout
}

func (t *timeouterror) Timeout() bool {
	return true
}

func (t *timeouterror) Temporary() bool {
	return false
}

type timeouterror2 struct {
}

func (t *timeouterror2) Error() string {
	return "unusual message"
}

func (t *timeouterror2) Timeout() bool {
	return true
}

func (t *timeouterror2) Temporary() bool {
	return false
}

// This is the slowest
func BenchmarkTimeoutUsingInterfaceCast(b *testing.B) {
	var err error = &timeouterror{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err.(net.Error).Timeout() {
		}
	}
}

// This is very slow too
func BenchmarkTimeoutUsingReflect(b *testing.B) {
	var err error = &timeouterror{}
	netErrType := reflect.TypeOf(err)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if reflect.TypeOf(err) == netErrType {
		}
	}
}

// This is surprisingly slow
func BenchmarkTimeoutUsingBytesEquals(b *testing.B) {
	var err error = &timeouterror{}
	ioTimeoutBytes := []byte(ioTimeout)
	iotl := len(ioTimeoutBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es := []byte(err.Error())
		esl := len(es)
		if esl >= iotl && bytes.Equal(es[esl-ioTimeoutLength:], ioTimeoutBytes) {
		}
	}
}

// This is also surprisingly slow
func BenchmarkTimeoutUsingHandBuiltCompare(b *testing.B) {
	var err error = &timeouterror{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es := err.Error()
		if hasSuffix(es, ioTimeout, ioTimeoutLength) {
		}
	}
}

// Surprisingly slow
func BenchmarkTimeoutUsingInterfaceEqualCheck(b *testing.B) {
	var err error = &timeouterror{}
	err2 := err
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err == err2 {
		}
	}
}

// This is faster
func BenchmarkTimeoutUsingSuffix(b *testing.B) {
	var err error = &timeouterror{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es := err.Error()
		if strings.HasSuffix(es, ioTimeout) {
		}
	}
}

// This is even faster
func BenchmarkTimeoutUsingSliceCompare(b *testing.B) {
	var err error = &timeouterror{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es := err.Error()
		esl := len(es)
		if esl >= ioTimeoutLength && es[esl-ioTimeoutLength:] == ioTimeout {
		}
	}
}

// This is very fast
func BenchmarkTimeoutUsingConcreteCast(b *testing.B) {
	var err error = &timeouterror{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok := err.(*timeouterror)
		if ok {
		}
	}
}

// This is the fastest
func BenchmarkTimeoutUsingConcreteEqualCheck(b *testing.B) {
	err := &timeouterror{}
	err2 := err
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err == err2 {
		}
	}
}

func hasSuffix(a string, b string, l int) bool {
	delta := len(a) - l
	if delta < 0 {
		return false
	}
	for i := 0; i < l; i++ {
		if a[i+delta] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkMixedFastPath3x(b *testing.B) {
	err1 := &timeouterror{}
	err2 := &timeouterror2{}
	err3 := context.Canceled

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutFP(err1)
		isTimeoutFP(err2)
		isTimeoutFP(err3)
	}
}

func BenchmarkFastPath1(b *testing.B) {
	err1 := &timeouterror{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutFP(err1)
	}
}

func BenchmarkFastPath2(b *testing.B) {
	err1 := &timeouterror2{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutFP(err1)
	}
}

func BenchmarkFastPath3(b *testing.B) {
	err1 := context.Canceled

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutFP(err1)
	}
}

func BenchmarkMixedSlowPath3x(b *testing.B) {
	err1 := &timeouterror{}
	err2 := &timeouterror2{}
	err3 := context.Canceled

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutSP(err1)
		isTimeoutSP(err2)
		isTimeoutSP(err3)
	}
}

func BenchmarkSlowPath1(b *testing.B) {
	err1 := &timeouterror{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutSP(err1)
	}
}

func BenchmarkSlowPath2(b *testing.B) {
	err1 := &timeouterror2{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutSP(err1)
	}
}

func BenchmarkSlowPath3(b *testing.B) {
	err1 := context.Canceled

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isTimeoutSP(err1)
	}
}

func isTimeoutFP(err error) bool {
	es := err.Error()
	esl := len(es)
	if esl >= ioTimeoutLength && es[esl-ioTimeoutLength:] == ioTimeout {
		return true
	}
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return true
	}
	return false
}

func isTimeoutSP(err error) bool {
	if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
		return true
	}
	return false
}
