package events

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/suite"

	"github.com/mirror520/events/model/event"
	"github.com/mirror520/events/persistent/inmem"
	"github.com/mirror520/events/pubsub"
)

type eventsTestSuite struct {
	suite.Suite
	now time.Time

	pubSub pubsub.PubSub
	repo   event.Repository
	svc    Service
	sync.Mutex
}

func (suite *eventsTestSuite) SetupSuite() {
	pubSub, err := pubsub.NewChannelPubSub()
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	repo := inmem.NewEventRepository()

	now := time.Now()
	{
		var id ulid.ULID
		id.SetTime(ulid.Timestamp(now.Add(-10 * time.Second)))
		repo.Store(&event.Event{
			ID:      id,
			Topic:   "hello/1/world",
			Payload: []byte("Test 1"),
		})
	}
	{
		var id ulid.ULID
		id.SetTime(ulid.Timestamp(now.Add(-5 * time.Second)))
		repo.Store(&event.Event{
			ID:      id,
			Topic:   "hello/2/world",
			Payload: []byte("Test 2"),
		})
	}

	svc := NewService(repo, pubSub)
	svc.Up()

	suite.now = now
	suite.pubSub = pubSub
	suite.repo = repo
	suite.svc = svc
}

func (suite *eventsTestSuite) TestStore() {
	suite.Lock()
	defer suite.Unlock()

	input := []byte(`{
		"from": "14c5f88a",
		"to": "bb75a980",
		"value": 10,
		"timestamp": "2023-01-22T23:35:00.000+08:00"
	}`)

	_, err := suite.svc.Store("hello/world", input)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	ch := make(chan *event.Event)
	errCh := suite.repo.Iterator(context.Background(), ch, time.Time{})

	ok := false
	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		for {
			select {
			case e := <-ch:
				if bytes.Equal(e.Payload, input) {
					ok = true
				}

			case err := <-errCh:
				if err != nil {
					suite.Fail(err.Error())
				}
				return
			}
		}
	}(wg)

	wg.Wait()

	suite.True(ok)
}

func (suite *eventsTestSuite) TestReplay() {
	suite.Lock()
	defer suite.Unlock()

	ch := make(chan *event.Event)
	suite.pubSub.Subscribe("hello/+/world", func(msg pubsub.Message) {
		ch <- event.NewEvent(msg.Topic(), msg.Payload())
	})

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		e := <-ch
		suite.Equal("hello/1/world", e.Topic)
		suite.Equal("Test 1", string(e.Payload))

		e = <-ch
		suite.Equal("hello/2/world", e.Topic)
		suite.Equal("Test 2", string(e.Payload))

		wg.Done()
	}(wg)

	err := suite.svc.Replay(time.Time{})
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	wg.Wait()
}

func (suite *eventsTestSuite) TestReplayWithFiltering() {
	suite.Lock()
	defer suite.Unlock()

	ch := make(chan *event.Event)
	suite.pubSub.Subscribe("hello/+/world", func(msg pubsub.Message) {
		ch <- event.NewEvent(msg.Topic(), msg.Payload())
	})

	wg := new(sync.WaitGroup)
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		e := <-ch
		suite.Equal("hello/2/world", e.Topic)
		suite.Equal("Test 2", string(e.Payload))

		wg.Done()
	}(wg)

	err := suite.svc.Replay(time.Time{}, "hello/2/world")
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	wg.Wait()
}

func (suite *eventsTestSuite) TearDownAllSuite() {
	suite.svc.Down()
	suite.repo.Close()
	suite.pubSub.Close()
}

func TestEventsTestSuite(t *testing.T) {
	// TODO: mutually exclusive issue
	suite.Run(t, new(eventsTestSuite))
}
