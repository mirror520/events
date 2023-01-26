package events

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mirror520/events/model/event"
	"github.com/mirror520/events/pubsub"
)

type Service interface {
	Up()
	Down()
	Store(topic string, payload []byte) error
	Replay(from time.Time, topic ...string) error
	StopReplay() error
}

type service struct {
	events       event.Repository
	destinations []pubsub.PubSub

	log          *zap.Logger
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
	svc.log = zap.L().With(
		zap.String("service", "events"),
	)
	log := svc.log.With(zap.String("action", "up"))

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel

	log.Info("done")
}

func (svc *service) Down() {
	svc.cancel()
	svc.log.Info("done", zap.String("action", "down"))
}

func (svc *service) Store(topic string, payload []byte) error {
	log := svc.log.With(
		zap.String("action", "store"),
	)

	e := event.NewEvent(topic, payload)
	err := svc.events.Store(e)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("event stored")
	return nil
}

func (svc *service) Replay(from time.Time, topic ...string) error {
	if svc.cancelReplay != nil {
		svc.cancelReplay()
		svc.cancelReplay = nil
	}

	ch := make(chan *event.Event)
	ctx, cancel := context.WithCancel(svc.ctx)
	svc.cancelReplay = cancel

	go func(ctx context.Context, ch <-chan *event.Event) {
		for {
			select {
			case <-ctx.Done():
				return

			case e := <-ch:
				for _, pubSub := range svc.destinations {
					pubSub.Publish(e.Topic, e.Payload)
				}
			}
		}
	}(ctx, ch)

	go svc.events.Iterator(ctx, ch, from)

	return nil
}

func (svc *service) StopReplay() error {
	if svc.cancelReplay != nil {
		svc.cancelReplay()
	}

	svc.cancelReplay = nil
	return nil
}
