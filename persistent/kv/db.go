package kv

import (
	"log"
	"os"
	"sync"

	"github.com/dgraph-io/badger/v3"
)

var (
	instance *badger.DB // pattern: Signleton
	once     sync.Once
)

func DB() *badger.DB {
	once.Do(loadDatabase)
	return instance
}

func loadDatabase() {
	path, ok := os.LookupEnv("EVENTS_WORK_DIR")
	if !ok {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err.Error())
		}
		path = homeDir + "/.events"
	}

	opts := badger.DefaultOptions(path).
		WithDir(path + "/meta").
		WithValueDir(path + "/data")

	db, err := badger.Open(opts)
	if err != nil {
		panic(err.Error())
	}

	instance = db
}

func Close() error {
	if instance == nil {
		return nil
	}

	return instance.Close()
}
