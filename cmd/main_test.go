package main

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/mirror520/events"
	"github.com/mirror520/events/infra/kv"
)

func TestGetGenesis(t *testing.T) {
	assert := assert.New(t)

	home, _ := os.UserHomeDir()

	db, err := kv.NewBadgerDatabase(home + "/.events")
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	var event events.Event
	if err := db.Get(context.Background(), "genesis", &event); err != nil {
		assert.Fail(err.Error())
		return
	}

	assert.Equal("hello/world", event.Topic)
	assert.Equal("Hello World", string(event.Payload))
}
