package events

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestNewEvent(t *testing.T) {
	assert := assert.New(t)

	payload, err := NewPayloadFromBytes([]byte(`"Hello World"`))
	if err != nil {
		assert.Fail(err.Error())
		return
	}

	e := NewEvent("cloud/say", payload)
	now := time.Now()

	assert.Equal(Any, e.Payload.Type)
	assert.Equal("Hello World", e.Payload.Data)
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

		prefix = id[:6]
	}

	id := ulid.Make()

	assert.True(bytes.HasPrefix(id.Bytes(), prefix))
}

func TestPayloadUnmarshalJSON(t *testing.T) {
	assert := assert.New(t)

	// json data: object
	{
		input := []byte(`{"hello": "Hello World"}`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		raw, ok := payload.JSON()
		assert.True(ok)
		assert.Equal(JSON, payload.Type)
		assert.Equal(input, []byte(raw))
	}

	// json data: array
	{
		input := []byte(`[1, 2, 3, 4, 5]`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		raw, ok := payload.JSON()
		assert.True(ok)
		assert.Equal(JSON, payload.Type)
		assert.Equal(input, []byte(raw))
	}

	// float64 data
	{
		input := []byte(`3.14`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(Any, payload.Type)
		assert.Equal(3.14, payload.Data)
	}

	// string data
	{
		input := []byte(`"Hello World"`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(Any, payload.Type)
		assert.Equal("Hello World", payload.Data)
	}

	// boolean data
	{
		input := []byte(`true`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(Any, payload.Type)
		assert.Equal(true, payload.Data)
	}

	// null data
	{
		input := []byte(`null`)

		var payload Payload
		if err := json.Unmarshal(input, &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(Any, payload.Type)
		assert.Nil(payload.Data)
	}

	// bytes data
	{
		input := 3.14

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, input); err != nil {
			assert.Fail(err.Error())
			return
		}

		encodedInput := base64.StdEncoding.EncodeToString(buf.Bytes())
		jsonStr := fmt.Sprintf(`{ "$binary": "%s" }`, encodedInput)

		var payload Payload
		if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(Bytes, payload.Type)
		assert.Equal(buf.Bytes(), payload.Data)
	}
}

func TestPayloadMarshalJSON(t *testing.T) {
	assert := assert.New(t)

	// json data: object
	{
		input := []byte(`{"hello":"Hello World"}`)
		payload := NewPayloadFromJSON(input)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(input, data)
	}

	// json data: array
	{
		input := []byte(`[1,2,3,4,5]`)
		payload := NewPayloadFromJSON(input)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal(input, data)
	}

	// float64 data
	{
		input := 3.14
		payload := NewPayload(input)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal([]byte(`3.14`), data)
	}

	// string data
	{
		input := "Hello World"
		payload := NewPayload(input)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal([]byte(`"Hello World"`), data)
	}

	// boolean data
	{
		input := true
		payload := NewPayload(input)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal([]byte(`true`), data)
	}

	// null data
	{
		payload := NewPayload(nil)

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		assert.Equal([]byte(`null`), data)
	}

	// bytes data
	{
		input := 3.14

		buf := new(bytes.Buffer)
		if err := binary.Write(buf, binary.BigEndian, input); err != nil {
			assert.Fail(err.Error())
			return
		}

		encodedInput := base64.StdEncoding.EncodeToString(buf.Bytes())

		payload, err := NewPayloadFromBytes(buf.Bytes(), true)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		data, err := json.Marshal(&payload)
		if err != nil {
			assert.Fail(err.Error())
			return
		}

		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			assert.Fail(err.Error())
			return
		}

		binData, ok := raw["$binary"].(string)
		assert.True(ok)
		assert.Equal(encodedInput, string(binData))
	}
}
