package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tdewolff/minify/v2"
	"github.com/tdewolff/minify/v2/json"
)

func TestMinify(t *testing.T) {
	assert := assert.New(t)

	m := minify.New()
	m.AddFunc("application/json", json.Minify)

	input := `{
		"from": "14c5f88a",
		"to": "bb75a980",
		"value": 10,
		"timestamp": "2023-01-22T23:35:00.000+08:00"
	}`

	output, err := m.Bytes("application/json", []byte(input))
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	assert.Equal(
		`{"from":"14c5f88a","to":"bb75a980","value":10,"timestamp":"2023-01-22T23:35:00.000+08:00"}`,
		string(output),
	)
}
