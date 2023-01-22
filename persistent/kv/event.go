package kv

import (
	"github.com/dgraph-io/badger/v3"

	"github.com/mirror520/events/model/event"
)

type eventRepository struct {
	db *badger.DB
}

func NewEventRepository() event.Repository {
	repo := new(eventRepository)
	repo.db = DB()
	return repo
}

func (repo *eventRepository) Store(e event.Event) error {
	return repo.db.Update(func(txn *badger.Txn) error {
		return txn.Set(e.ID.Bytes(), e.Payload)
	})
}
