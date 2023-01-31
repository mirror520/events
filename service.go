package events

import (
	"context"
	"errors"
	"time"

	"github.com/mirror520/events/model/event"
	"github.com/mirror520/events/pubsub"
)

var (
	ErrReplaying    = errors.New("replaying")
	ErrEmptyPayload = errors.New("empty payload")
	ErrBusying      = errors.New("busying")
)

type Service interface {
	Up()
	Down()
	Store(topic string, payload []byte) (string, error)
	Replay(from time.Time, topic ...string) error
	StopReplay() error
}

type ServiceMiddleware func(Service) Service

type service struct {
	events       event.Repository
	destinations []pubsub.PubSub

	ctx          context.Context
	cancel       context.CancelFunc
	cancelReplay context.CancelFunc
}

func NewService(events event.Repository, destinations []pubsub.PubSub) Service {
	return &service{
		events:       events,
		destinations: destinations,
	}
}

func (svc *service) Up() {
	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel
}

func (svc *service) Down() {
	svc.cancel()
}

func (svc *service) Store(topic string, payload []byte) (string, error) {
	if svc.cancelReplay != nil {
		return "", ErrReplaying
	}

	if len(payload) == 0 {
		return "", ErrEmptyPayload
	}

	e := event.NewEvent(topic, payload)
	err := svc.events.Store(e)
	if err != nil {
		return "", err
	}

	return e.ID.String(), nil
}

func (svc *service) Replay(from time.Time, topic ...string) error {
	if svc.cancelReplay != nil {
		return ErrBusying
	}

	topics := make(map[string]struct{})
	for _, t := range topic {
		topics[t] = struct{}{}
	}

	ch := make(chan *event.Event)
	ctx, cancel := context.WithCancel(svc.ctx)
	svc.cancelReplay = cancel

	errCh := svc.events.Iterator(ctx, ch, from)

	go func(ctx context.Context, ch <-chan *event.Event, errCh <-chan error) {
		for {
			select {
			case <-ctx.Done():
				return

			case e := <-ch:
				if len(topics) > 0 {
					if _, ok := topics[e.Topic]; !ok {
						continue // filtering
					}
				}

				for _, pubSub := range svc.destinations {
					pubSub.Publish(e.Topic, e.Payload)
				}

			case <-errCh:
				time.Sleep(5 * time.Second)
				svc.StopReplay()
				return
			}
		}
	}(ctx, ch, errCh)

	return nil
}

func (svc *service) StopReplay() error {
	if svc.cancelReplay != nil {
		svc.cancelReplay()
	}

	svc.cancelReplay = nil
	return nil
}
