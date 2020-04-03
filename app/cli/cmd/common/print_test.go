package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDuration(t *testing.T) {
	t1 := time.Unix(1577836800, 0)
	t2 := time.Unix(1577845810, 0)

	s := duration(&t1, &t2)
	assert.Equal(t, "2h 30m 10s", s)
}
