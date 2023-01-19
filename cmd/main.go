package main

import (
	"context"
	"log"
	"os"

	"github.com/mirror520/events"
	"github.com/mirror520/events/infra/kv"
)

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err.Error())
	}

	db, err := kv.NewBadgerDatabase(home + "/.events")
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
