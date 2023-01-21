package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLoadConfig(t *testing.T) {
	assert := assert.New(t)

	err := LoadConfig("../")
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	_, ok := DataSource("mqtt-test")
	assert.True(ok)
}
