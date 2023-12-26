package events

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
)

var (
	ErrEmptyPayload     = errors.New("empty payload")
	ErrIteratorNotFound = errors.New("iterator not found")
	ErrInvalidType      = errors.New("invalid type")
)

type Service interface {
	Up()
	Down()
	Store(topic string, payload json.RawMessage, ids ...ulid.ULID) error
	NewIterator(topic string, since time.Time) (string, error)
	Iterator(id string) (Iterator, error)

	// Iterator
	FetchFromIterator(batch int, id string) ([]*Event, error)
	CloseIterator(id string) error
}

type ServiceMiddleware func(Service) Service

type service struct {
	log       *zap.Logger
	events    Repository
	iterators sync.Map

	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(events Repository) Service {
	return &service{
		events: events,
	}
}

func (svc *service) Up() {
	svc.log = zap.L().With(
		zap.String("service", "events"),
	)

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel

	svc.log.Info("done", zap.String("action", "up"))
}

func (svc *service) Down() {
	svc.cancel()
	svc.log.Info("done", zap.String("action", "down"))
}

func (svc *service) Store(topic string, payload json.RawMessage, ids ...ulid.ULID) error {
	if len(payload) == 0 {
		return ErrEmptyPayload
	}

	e := NewEvent(topic, payload, ids...)
	err := svc.events.Store(e)
	if err != nil {
		return err
	}

	return nil
}

func (svc *service) NewIterator(topic string, since time.Time) (string, error) {
	// TODO: options
	it, err := svc.events.Iterator(svc.ctx, since)
	if err != nil {
		return "", err
	}

	go svc.doneHandler(it)

	svc.iterators.Store(it.ID(), it)
	return it.ID(), nil
}

func (svc *service) Iterator(id string) (Iterator, error) {
	val, ok := svc.iterators.Load(id)
	if !ok {
		return nil, errors.New("iterator not found")
	}

	it, ok := val.(Iterator)
	if !ok {
		return nil, errors.New("invalid type")
	}

	return it, nil
}

func (svc *service) doneHandler(it Iterator) {
	log := svc.log.With(
		zap.String("iterator", it.ID()),
		zap.String("handler", "iterator_done"),
	)

	<-it.Done()
	if err := it.Err(); err != nil {
		if !errors.Is(err, context.Canceled) {
			log.Error(err.Error())
		}
	}

	svc.iterators.Delete(it.ID())
	log.Info("done")
}

func (svc *service) FetchFromIterator(batch int, id string) ([]*Event, error) {
	val, ok := svc.iterators.Load(id)
	if !ok {
		return nil, ErrIteratorNotFound
	}

	it, ok := val.(Iterator)
	if !ok {
		return nil, ErrInvalidType
	}

	return it.Fetch(batch)
}

func (svc *service) CloseIterator(id string) error {
	val, ok := svc.iterators.LoadAndDelete(id)
	if !ok {
		return ErrIteratorNotFound
	}

	it, ok := val.(Iterator)
	if !ok {
		return ErrInvalidType
	}

	it.Close(nil)
	return nil
}
