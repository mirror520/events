package events

import (
	"bytes"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewEvent(t *testing.T) {
	assert := assert.New(t)

	e := NewEvent("cloud/say", []byte("Hello World"))
	now := time.Now()

	assert.Equal("Hello World", string(e.Payload))
	assert.True(now.Sub(e.Time()) < 1*time.Second)
}

func TestULIDTimestamp(t *testing.T) {
	assert := assert.New(t)

	var prefix []byte
	{
		var id ulid.ULID
		if err := id.SetTime(ulid.Now()); err != nil {
			assert.Fail(err.Error())
			return
		}

		prefix = id[0:6]
	}

	id := ulid.Make()

	assert.True(bytes.HasPrefix(id.Bytes(), prefix))
}
