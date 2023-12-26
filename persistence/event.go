package persistence

import (
	"errors"

	"github.com/mirror520/events"
	"github.com/mirror520/events/persistence/badger"
	"github.com/mirror520/events/persistence/influxdb"
	"github.com/mirror520/events/persistence/inmem"
)

func NewEventRepository(cfg events.Persistence) (events.Repository, error) {
	switch cfg.Driver {
	case events.InMem:
		return inmem.NewEventRepository(cfg)

	case events.BadgerDB:
		return badger.NewEventRepository(cfg)

	case events.InfluxDB:
		return influxdb.NewEventRepository(cfg)

	default:
		return nil, errors.New("driver not supported")
	}
}
