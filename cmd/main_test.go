package main

import (
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"

	"github.com/mirror520/events"
	"github.com/mirror520/events/persistence"
)

type eventsTestSuite struct {
	suite.Suite
	now time.Time

	repo events.Repository
	svc  events.Service
}

func (suite *eventsTestSuite) SetupSuite() {
	cfg := events.Persistence{
		Driver: events.InMem,
	}

	repo, err := persistence.NewEventRepository(cfg)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	now := time.Now()
	{
		var id ulid.ULID
		id.SetTime(ulid.Timestamp(now.Add(-10 * time.Second)))
		repo.Store(&events.Event{
			ID:      id,
			Topic:   "hello/1/world",
			Payload: []byte("Test 1"),
		})
	}
	{
		var id ulid.ULID
		id.SetTime(ulid.Timestamp(now.Add(-5 * time.Second)))
		repo.Store(&events.Event{
			ID:      id,
			Topic:   "hello/2/world",
			Payload: []byte("Test 2"),
		})
	}

	svc := events.NewService(repo)
	svc.Up()

	suite.now = now
	suite.repo = repo
	suite.svc = svc
}

func (suite *eventsTestSuite) TestStore() {
	topic := "hello/world"
	payload := []byte(`{
		"message": "Hello World",
		"timestamp": "2023-01-22T23:35:00.000+08:00"
	}`)

	err := suite.svc.Store(topic, payload)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	it, err := suite.svc.Iterator(topic, time.Time{})
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	events, err := it.Fetch(3)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.Len(events, 3)
	suite.Equal(topic, events[2].Topic)
	suite.Equal(payload, events[2].Payload)
}

func (suite *eventsTestSuite) TearDownAllSuite() {
	suite.svc.Down()
	suite.repo.Close()
}

func TestEventsTestSuite(t *testing.T) {
	suite.Run(t, new(eventsTestSuite))
}
