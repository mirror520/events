package main

import (
	"context"
	"log"
	"os"

	"github.com/mirror520/events"
	"github.com/mirror520/events/kv"
)

func main() {
	workDir, ok := os.LookupEnv("EVENTS_WORK_DIR")
	if !ok {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err.Error())
		}
		workDir = homeDir
	}

	db, err := kv.NewBadgerDatabase(workDir)
	if err != nil {
		log.Fatal(err.Error())
	}

	event := events.Event{
		Topic:   "hello/world",
		Payload: []byte("Hello World"),
	}

	ctx := context.Background()
	if err := db.Set(ctx, "genesis", event); err != nil {
		log.Fatal(err.Error())
	}
}
