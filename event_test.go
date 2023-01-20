package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEvent(t *testing.T) {
	assert := assert.New(t)

	e := NewEvent("cloud/say", []byte("Hello World"))
	now := time.Now()

	assert.Equal("Hello World", string(e.Payload))
	assert.True(now.Sub(e.Time()) < 1*time.Second)
}
