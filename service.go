package events

import (
	"context"

	"go.uber.org/zap"

	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/mirror520/events/model"
	"github.com/mirror520/events/model/event"
	"github.com/mirror520/events/pubsub"
)

type Service interface {
	Up()
	Down()
	EventStore(e event.Event) error
}

type service struct {
	events  event.Repository
	sources map[string]*model.Source

	log    *zap.Logger
	cancel context.CancelFunc
}

func NewService(events event.Repository, sources map[string]*model.Source) Service {
	svc := new(service)
	svc.events = events
	svc.sources = sources
	return svc
}

func (svc *service) Up() {
	svc.log = zap.L().With(
		zap.String("service", "events"),
	)
	log := svc.log.With(zap.String("action", "up"))

	ctx, cancel := context.WithCancel(context.Background())
	svc.cancel = cancel

	for name, source := range svc.sources {
		log := log.With(zap.String("source", name))

		pubsub, err := pubsub.Factory(source)
		if err != nil {
			log.Error(err.Error())
			continue
		}

		for _, topic := range source.MqttConfig.Topics {
			messages, err := pubsub.Subscribe(ctx, topic)
			if err != nil {
				log.Error(err.Error(), zap.String("topic", topic))
				continue
			}

			go svc.process(ctx, topic, messages)
		}
	}

	log.Info("done")
}

func (svc *service) Down() {
	svc.cancel()
	svc.log.Info("done", zap.String("action", "down"))
}

// TODO: move to transport
func (svc *service) process(ctx context.Context, topic string, messages <-chan *message.Message) {
	log := svc.log.With(
		zap.String("action", "process"),
		zap.String("topic", topic),
	)

	for {
		select {
		case <-ctx.Done():
			log.Info("done")
			return

		case msg := <-messages:
			if msg == nil {
				continue
			}
			log.Debug("event recv")

			e := event.NewEvent(topic, msg.Payload)
			err := svc.EventStore(e)
			if err != nil {
				log.Error(err.Error())
			}

			msg.Ack()
		}
	}
}

func (svc *service) EventStore(e event.Event) error {
	err := svc.events.Store(e)
	if err != nil {
		return err
	}

	return nil
}
