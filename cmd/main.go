package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/gin-gonic/gin"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"github.com/mirror520/events"
	"github.com/mirror520/events/persistence"
	"github.com/mirror520/events/transport/http"
)

func main() {
	app := &cli.App{
		Name:  "events",
		Usage: "The events is a versatile solution for storing, managing, and iterating through events.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "path",
				Usage:   "Specifies the working directory",
				EnvVars: []string{"EVENTS_PATH"},
			},
			&cli.IntFlag{
				Name:    "port",
				Usage:   "Specifies the HTTP service port",
				Value:   8080,
				EnvVars: []string{"EVENTS_HTTP_PORT"},
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func run(cli *cli.Context) error {
	path := cli.String("path")
	if path == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return err
		}

		path = homeDir + "/.events"
	}

	log, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	defer log.Sync()

	zap.ReplaceGlobals(log)

	log = log.With(
		zap.String("action", "main"),
	)

	f, err := os.Open(path + "/config.yaml")
	if err != nil {
		return err
	}
	defer f.Close()

	var cfg *events.Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return err
	}

	cfg.SetPath(path)

	repo, err := persistence.NewEventRepository(cfg.Persistence)
	if err != nil {
		return err
	}

	svc := events.NewService(repo)
	svc = events.LoggingMiddleware(zap.L())(svc)
	svc.Up()

	r := gin.Default()
	apiV1 := r.Group("/v1")

	// PUT /events
	{
		endpoint := events.StoreEndpoint(svc)
		endpoint = events.MinifyMiddleware(events.JSON)(endpoint)
		apiV1.PUT("/events", http.StoreHandler(endpoint))
	}

	// POST /events/iterators
	{
		endpoint := events.NewIteratorEndpoint(svc)
		apiV1.POST("/events/iterators", http.NewIteratorHandler(endpoint))
	}

	// GET /events/iterators/:id?batch=100
	{
		endpoint := events.FetchFromIterator(svc)
		apiV1.GET("/events/iterators/:id", http.FetchFromIteratorHandler(endpoint))
	}

	// DELETE /events/iterators/:id
	{
		endpoint := events.CloseIterator(svc)
		apiV1.DELETE("/events/iterators/:id", http.CloseIteratorHandler(endpoint))
	}

	go r.Run(":" + strconv.Itoa(cli.Int("port")))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sign := <-quit
	log.Info(sign.String())

	svc.Down()
	repo.Close()

	log.Info("done")
	return nil
}
