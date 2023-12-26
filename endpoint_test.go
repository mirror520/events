package events

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshalStoreRequest(t *testing.T) {
	assert := assert.New(t)

	jsonStr := []byte(`{
		"id": "01HJJD04ZSE4T4SN6T7SVYBPNV",
		"topic": "hello.world",
		"payload": "Hello World"
	}`)

	var req StoreRequest
	assert.Zero(req.ID.Time())

	if err := json.Unmarshal(jsonStr, &req); err != nil {
		assert.Fail(err.Error())
		return
	}

	assert.Equal("01HJJD04ZSE4T4SN6T7SVYBPNV", req.ID.String())
}
