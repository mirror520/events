package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	assert := assert.New(t)

	cfg, err := LoadConfig("../")
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	_, ok := cfg.Transports["mqtt-test"]
	assert.True(ok)
}
