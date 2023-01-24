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
	"github.com/mirror520/events/pubsub"
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

	pubSubs := make(map[string]pubsub.PubSub)
	for name, transport := range cfg.Transports {
		pubSub, err := pubsub.Factory(transport)
		if err != nil {
			log.Error(err.Error(),
				zap.String("transport", name),
			)
			continue
		}

		pubSubs[name] = pubSub
	}

	destinations := make([]pubsub.PubSub, 0)
	for _, destination := range cfg.Destinations {
		pubSub, ok := pubSubs[destination.Transport]
		if !ok {
			log.Error("transport not found",
				zap.String("destination", destination.Transport),
			)
			continue
		}

		destinations = append(destinations, pubSub)
	}

	r := gin.Default()
	r.Use(cors.Default())

	repo := kv.NewEventRepository()
	svc := events.NewService(repo, destinations)
	svc.Up()
	{
		endpoint := events.StoreEndpoint(svc)
		endpoint = events.MinifyMiddleware("json")(endpoint)
		r.PUT("/events", events.HTTPStoreHandler(endpoint))

		for _, source := range cfg.Sources {
			log := log.With(zap.String("source", source.Transport))

			pubSub, ok := pubSubs[source.Transport]
			if !ok {
				log.Error("transport not found")
				continue
			}

			for _, topic := range source.Topics {
				err := pubSub.Subscribe(topic, events.MQTTStoreHandler(endpoint))
				if err != nil {
					log.Error(err.Error(), zap.String("topic", topic))
				}
			}
		}
	}

	{
		endpoint := events.PlaybackEndpoint(svc)
		r.GET("/playback", events.HTTPPlaybackHandler(endpoint))
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
