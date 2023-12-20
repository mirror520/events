package events

import (
	"time"

	"go.uber.org/zap"
)

func LoggingMiddleware(log *zap.Logger) ServiceMiddleware {
	return func(next Service) Service {
		return &loggingMiddleware{
			log.With(zap.String("service", "events")),
			next,
		}
	}
}

type loggingMiddleware struct {
	log  *zap.Logger
	next Service
}

func (mw *loggingMiddleware) Up() {
	mw.next.Up()
}

func (mw *loggingMiddleware) Down() {
	mw.next.Down()
}

func (mw *loggingMiddleware) Store(topic string, payload []byte) error {
	log := mw.log.With(
		zap.String("action", "store"),
		zap.String("topic", topic),
	)

	err := mw.next.Store(topic, payload)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("event stored")
	return nil
}

func (mw *loggingMiddleware) Iterator(topic string, since time.Time) (Iterator, error) {
	log := mw.log.With(
		zap.String("action", "iterator"),
		zap.String("topic", topic),
		zap.Time("since", since),
	)

	it, err := mw.next.Iterator(topic, since)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	log.Info("iterator created")
	return it, nil
}

func (mw *loggingMiddleware) FetchFromIterator(batch int, id string) ([]*Event, error) {
	log := mw.log.With(
		zap.String("action", "fetch"),
		zap.String("iterator", id),
		zap.Int("batch", batch),
	)

	events, err := mw.next.FetchFromIterator(batch, id)
	if err != nil {
		log.Error(err.Error())
		return nil, err
	}

	log.Info("event fetched", zap.Int("size", len(events)))
	return events, nil
}

func (mw *loggingMiddleware) CloseIterator(id string) error {
	log := mw.log.With(
		zap.String("action", "close_iterator"),
		zap.String("iterator", id),
	)

	err := mw.next.CloseIterator(id)
	if err != nil {
		log.Error(err.Error())
		return err
	}

	log.Info("iterator closed")
	return nil
}
