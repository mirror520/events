package mongo

import (
	"encoding/json"
	"errors"
	"reflect"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"

	"github.com/mirror520/events"
)

var (
	tULID    = reflect.TypeOf(ulid.ULID{})
	tPayload = reflect.TypeOf(events.Payload{})
)

// ref: https://gist.github.com/SupaHam/3afe982dc75039356723600ccc91ff77

type ULIDCodec interface {
	bsoncodec.ValueEncoder
	bsoncodec.ValueDecoder
}

func NewULIDCodec() ULIDCodec {
	return &ulidCodec{}
}

type ulidCodec struct{}

func (c *ulidCodec) EncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tULID {
		return bsoncodec.ValueEncoderError{
			Name:     "ULIDCodec",
			Types:    []reflect.Type{tULID},
			Received: val,
		}
	}

	id := val.Interface().(ulid.ULID)
	return vw.WriteBinaryWithSubtype(id.Bytes(), bson.TypeBinaryUUID)
}

func (c *ulidCodec) DecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tULID {
		return bsoncodec.ValueDecoderError{
			Name:     "ULIDCodec",
			Types:    []reflect.Type{tULID},
			Received: val,
		}
	}

	switch vr.Type() {
	case bson.TypeBinary:
		data, subtype, err := vr.ReadBinary()
		if err != nil {
			return err
		}

		if subtype != bson.TypeBinaryUUID {
			return errors.New("invalid subtype")
		}

		id := ulid.ULID(data)
		val.Set(reflect.ValueOf(id))

	case bson.TypeNull:
		return vr.ReadNull()

	case bson.TypeUndefined:
		return vr.ReadUndefined()

	default:
		return errors.New("invalid type")
	}

	return nil
}

type PayloadCodec interface {
	bsoncodec.ValueEncoder
	bsoncodec.ValueDecoder
}

func NewPayloadCodec() PayloadCodec {
	return &payloadCodec{}
}

type payloadCodec struct{}

func (c *payloadCodec) EncodeValue(ec bsoncodec.EncodeContext, vw bsonrw.ValueWriter, val reflect.Value) error {
	if !val.IsValid() || val.Type() != tPayload {
		return bsoncodec.ValueEncoderError{
			Name:     "PayloadCodec",
			Types:    []reflect.Type{tPayload},
			Received: val,
		}
	}

	payload := val.Interface().(events.Payload)
	switch val := payload.Data.(type) {
	case json.RawMessage:
		var raw any
		if err := json.Unmarshal(val, &raw); err != nil {
			return err
		}

		enc, err := bson.NewEncoder(vw)
		if err != nil {
			return err
		}

		return enc.Encode(raw)

	case []byte:
		if payload.Type != events.Bytes {
			return errors.New("invalid subtype")
		}

		return vw.WriteBinaryWithSubtype(val, bson.TypeBinaryGeneric)

	case float64:
		return vw.WriteDouble(val)

	case bool:
		return vw.WriteBoolean(val)

	case string:
		return vw.WriteString(val)

	case nil:
		return vw.WriteNull()

	default:
		enc, err := bson.NewEncoder(vw)
		if err != nil {
			return err
		}

		return enc.Encode(val)
	}
}

func (c *payloadCodec) DecodeValue(dc bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tPayload {
		return bsoncodec.ValueDecoderError{
			Name:     "PayloadCodec",
			Types:    []reflect.Type{tPayload},
			Received: val,
		}
	}

	switch vr.Type() {
	case bson.TypeEmbeddedDocument:
		dec, err := bson.NewDecoder(vr)
		if err != nil {
			return err
		}

		var raw bson.Raw
		if err := dec.Decode(&raw); err != nil {
			return err
		}

		bs, err := bson.MarshalExtJSON(raw, false, false)
		if err != nil {
			return err
		}

		payload := events.NewPayloadFromJSON(bs)
		val.Set(reflect.ValueOf(payload))

	case bson.TypeArray:
		dec, err := bson.NewDecoder(vr)
		if err != nil {
			return err
		}

		var arr []any
		if err := dec.Decode(&arr); err != nil {
			return err
		}

		bs, err := json.Marshal(&arr)
		if err != nil {
			return err
		}

		payload := events.NewPayloadFromJSON(bs)
		val.Set(reflect.ValueOf(payload))

	case bson.TypeBinary:
		bs, subtype, err := vr.ReadBinary()
		if err != nil {
			return err
		}

		if subtype != bson.TypeBinaryGeneric {
			return errors.New("invalid subtype")
		}

		payload, err := events.NewPayloadFromBytes(bs, true)
		if err != nil {
			return err
		}

		val.Set(reflect.ValueOf(payload))

	case bson.TypeDouble:
		data, err := vr.ReadDouble()
		if err != nil {
			return err
		}

		payload := events.NewPayload(data)
		val.Set(reflect.ValueOf(payload))

	case bson.TypeBoolean:
		data, err := vr.ReadBoolean()
		if err != nil {
			return err
		}

		payload := events.NewPayload(data)
		val.Set(reflect.ValueOf(payload))

	case bson.TypeString:
		data, err := vr.ReadString()
		if err != nil {
			return err
		}

		payload := events.NewPayload(data)
		val.Set(reflect.ValueOf(payload))

	case bson.TypeNull:
		return vr.ReadNull()

	default:
		dec, err := bson.NewDecoder(vr)
		if err != nil {
			return err
		}

		var data any
		if err := dec.Decode(&data); err != nil {
			return err
		}

		payload := events.NewPayload(data)
		val.Set(reflect.ValueOf(payload))
	}

	return nil
}
