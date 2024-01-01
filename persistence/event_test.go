package persistence

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"

	"github.com/mirror520/events"
	"github.com/mirror520/events/persistence/badger"
	"github.com/mirror520/events/persistence/influxdb"
	"github.com/mirror520/events/persistence/inmem"
	"github.com/mirror520/events/persistence/mongo"
)

type persistenceTestSuite struct {
	suite.Suite
	dataset []*events.Event
}

func (suite *persistenceTestSuite) SetupSuite() {
	now := time.Now()

	ids := make([]ulid.ULID, 5)

	for i := 0; i < 5; i++ {
		ms := now.Add(time.Duration(-(5 - i)) * time.Minute).UnixMilli()

		id := ulid.Make()
		id.SetTime(uint64(ms))

		ids[i] = id
	}

	suite.dataset = []*events.Event{
		events.NewEvent("hello.world", events.Payload{
			Data: json.RawMessage(`{"msg":"Hello World"}`),
			Type: events.JSON,
		}, ids[0]),
		events.NewEvent("hello.world", events.Payload{Data: nil}, ids[1]),
		events.NewEvent("hello.world", events.Payload{Data: true}, ids[2]),
		events.NewEvent("hello.world", events.Payload{Data: 3.14}, ids[3]),
		events.NewEvent("hello.world", events.Payload{Data: "Hello World"}, ids[4]),
	}
}

func (suite *persistenceTestSuite) TestInMemPersistence() {
	cfg := events.Persistence{
		Driver: events.InMem,
	}

	repo, _ := inmem.NewEventRepository(cfg)
	defer repo.Close()

	var errs error
	for _, e := range suite.dataset {
		err := repo.Store(e)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	if errs != nil {
		suite.Fail(errs.Error())
		return
	}

	it, _ := repo.Iterator(context.TODO(), time.Time{})
	defer it.Close(nil)

	es, err := it.Fetch(5)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.Len(es, 5)
	for i, e := range suite.dataset {
		suite.Equal(e.Payload.Data, es[i].Payload.Data)
	}
}

func (suite *persistenceTestSuite) TestBadgerPersistence() {
	cfg := events.Persistence{
		Driver: events.BadgerDB,
		DSN:    "file::memory",
	}

	repo, err := badger.NewEventRepository(cfg)
	if err != nil {
		suite.T().Skip(err.Error())
		return
	}
	defer repo.Close()

	var errs error
	for _, e := range suite.dataset {
		err := repo.Store(e)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	if errs != nil {
		suite.Fail(errs.Error())
		return
	}

	it, _ := repo.Iterator(context.TODO(), time.Time{})
	defer it.Close(nil)

	time.Sleep(1000 * time.Millisecond)

	es, err := it.Fetch(5)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.Len(es, 5)
	for i, e := range suite.dataset {
		suite.Equal(e.Payload.Data, es[i].Payload.Data)
	}
}

func (suite *persistenceTestSuite) TestInfluxDBPersistence() {
	cfg := events.Persistence{
		Driver: events.InfluxDB,
		DSN:    "http://localhost:8086?db=tests&duration=1s",
	}

	repo, err := influxdb.NewEventRepository(cfg)
	if err != nil {
		suite.T().Skip(err.Error())
		return
	}
	defer repo.Close()

	defer func(repo events.Repository) {
		influxdb, ok := repo.(influxdb.EventRepository)
		if ok {
			influxdb.Exec(`DROP DATABASE "tests"`)
		}
	}(repo)

	var errs error
	for _, e := range suite.dataset {
		err := repo.Store(e)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	if errs != nil {
		suite.Fail(errs.Error())
		return
	}

	time.Sleep(3 * time.Second)

	it, _ := repo.Iterator(context.TODO(), time.Time{})
	defer it.Close(nil)

	es, err := it.Fetch(5)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.Len(es, 5)
	for i, e := range suite.dataset {
		suite.Equal(e.Payload.Data, es[i].Payload.Data)
	}
}

func (suite *persistenceTestSuite) TestMongoDBPersistence() {
	cfg := events.Persistence{
		Driver: events.MongoDB,
		DSN:    "mongodb://localhost:27017?db=tests&duration=1s",
	}

	repo, err := mongo.NewEventRepository(cfg)
	if err != nil {
		suite.T().Skip(err.Error())
		return
	}
	defer repo.Close()

	defer func(repo events.Repository) {
		mongodb, ok := repo.(mongo.EventRepository)
		if ok {
			mongodb.DropDatabase("tests")
		}
	}(repo)

	var errs error
	for _, e := range suite.dataset {
		err := repo.Store(e)
		if err != nil {
			errs = errors.Join(errs, err)
		}
	}

	if errs != nil {
		suite.Fail(errs.Error())
		return
	}

	it, _ := repo.Iterator(context.TODO(), time.Time{})
	defer it.Close(nil)

	time.Sleep(3 * time.Second)

	es, err := it.Fetch(5)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.Len(es, 5)
	for i, e := range suite.dataset {
		suite.Equal(e.Payload.Data, es[i].Payload.Data)
	}
}

func TestPersistenceTestSuite(t *testing.T) {
	suite.Run(t, new(persistenceTestSuite))
}
