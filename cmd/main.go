package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/mirror520/events"
	"github.com/mirror520/events/kv"
)

var homeDir string

func init() {
	homeDir, _ = os.UserHomeDir()
}

func main() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "workdir",
				Aliases: []string{"dir"},
				Usage:   "Work directory",
				EnvVars: []string{"EVENTS_WORK_DIR"},
				Value:   homeDir + "/.events",
			},
		},
		Action: run,
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func run(cli *cli.Context) error {
	db, err := kv.NewBadgerDatabase(cli.String("workdir"))
	if err != nil {
		return err
	}

	event := events.Event{
		Topic:   "hello/world",
		Payload: []byte("Hello World"),
	}

	ctx := context.Background()
	if err := db.Set(ctx, "genesis", event); err != nil {
		return err
	}

	return nil
}
