package events

import (
	"errors"
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

func (mw *loggingMiddleware) Store(topic string, payload []byte) (string, error) {
	log := mw.log.With(
		zap.String("action", "store"),
		zap.String("topic", topic),
	)

	id, err := mw.next.Store(topic, payload)
	if err != nil {
		if !errors.Is(err, ErrReplaying) {
			log.Error(err.Error())
		}

		return "", err
	}

	log.Info("event stored", zap.String("id", id))
	return id, nil
}

func (mw *loggingMiddleware) Replay(from time.Time, topic ...string) error {
	log := mw.log.With(
		zap.String("action", "replay"),
		zap.Time("from", from),
	)

	err := mw.next.Replay(from, topic...)
	if err != nil {
		log.Error(err.Error())
	}

	log.Info("replaying")
	return nil
}

func (mw *loggingMiddleware) StopReplay() error {
	return mw.next.StopReplay()
}
