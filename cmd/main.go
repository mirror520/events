package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
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

	log = log.With(
		zap.String("action", "main"),
	)

	cfg, err := conf.LoadConfig(path)
	if err != nil {
		log.Fatal(err.Error())
	}

	r := gin.Default()
	r.Use(cors.Default())

	repo := kv.NewEventRepository()
	svc := events.NewService(repo, cfg.Sources)
	svc.Up()
	{
		endpoint := events.EventStoreEndpoint(svc)
		handler := events.EventStoreHandler(endpoint)
		r.PUT("/events", handler)
	}

	go r.Run(":8080")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sign := <-quit
	log.Info(sign.String())

	svc.Down()
	kv.Close()

	log.Info("done")
}
