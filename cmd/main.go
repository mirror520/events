package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/mirror520/events"
	"github.com/mirror520/events/conf"
	"github.com/mirror520/events/persistent/kv"
)

func main() {
	path, ok := os.LookupEnv("EVENTS_WORK_DIR")
	if !ok {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err.Error())
		}
		path = homeDir + "/.events"
	}

	log, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err.Error())
	}
	defer log.Sync()

	zap.ReplaceGlobals(log)

	cfg, err := conf.LoadConfig(path)
	if err != nil {
		log.Fatal(err.Error())
	}

	repo := kv.NewEventRepository()
	svc := events.NewService(repo, cfg.Sources)
	svc.Up()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sign := <-quit
	log.Info(sign.String())

	svc.Down()
	kv.Close()
}
