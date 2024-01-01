package influxdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	influx "github.com/influxdata/influxdb1-client/v2"

	"github.com/mirror520/events"
)

type EventRepository interface {
	events.Repository
	Exec(command string) error
}

type eventRepository struct {
	log    *zap.Logger
	cfg    *Config
	client influx.Client
	points []*influx.Point
	cancel context.CancelFunc
	sync.Mutex
}

func NewEventRepository(cfg events.Persistence) (events.Repository, error) {
	conf, err := parseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}

	client, err := influx.NewHTTPClient(conf.HTTPConfig)
	if err != nil {
		return nil, err
	}

	command := influx.NewQuery(`CREATE DATABASE `+conf.Database, "", "")
	client.Query(command)

	ctx, cancel := context.WithCancel(context.Background())

	repo := &eventRepository{
		log: zap.L().With(
			zap.String("persistence", "influxdb"),
		),
		cfg:    conf,
		client: client,
		points: make([]*influx.Point, 0),
		cancel: cancel,
	}

	go repo.batchWriteHandler(ctx)

	return repo, nil
}

func (repo *eventRepository) batchWriteHandler(ctx context.Context) {
	log := repo.log.With(
		zap.String("action", "batch_write"),
	)

	ticker := time.NewTicker(repo.cfg.Duration)
	for {
		select {
		case <-ctx.Done():
			repo.Lock()
			if size := len(repo.points); size > 0 {
				log := log.With(zap.Int("points", size))

				bp, err := influx.NewBatchPoints(repo.cfg.BatchPointsConfig)
				if err != nil {
					log.Error(err.Error())
				} else {
					bp.AddPoints(repo.points)

					if err := repo.client.Write(bp); err != nil {
						log.Error(err.Error())
					} else {
						log.Info("points written")
					}
				}
			}

			repo.points = make([]*influx.Point, 0)
			repo.Unlock()

			log.Info("done")
			return

		case <-ticker.C:
			repo.Lock()
			if size := len(repo.points); size > 0 {
				log := log.With(zap.Int("points", size))

				bp, err := influx.NewBatchPoints(repo.cfg.BatchPointsConfig)
				if err != nil {
					log.Error(err.Error())
				} else {
					bp.AddPoints(repo.points)

					if err := repo.client.Write(bp); err != nil {
						log.Error(err.Error())
					} else {
						log.Info("points written")
					}
				}
			}

			repo.points = make([]*influx.Point, 0)
			repo.Unlock()
		}
	}
}

func (repo *eventRepository) Store(e *events.Event) error {
	tags := map[string]string{
		"topic": e.Topic,
	}

	data, err := json.Marshal(e.Payload.Data)
	if err != nil {
		return err
	}

	fields := map[string]any{
		"id":      e.ID.String(),
		"payload": string(data),
	}

	ts := time.UnixMilli(int64(e.ID.Time()))

	point, err := influx.NewPoint(repo.cfg.Measurement, tags, fields, ts)
	if err != nil {
		return err
	}

	repo.Lock()
	repo.points = append(repo.points, point)
	repo.Unlock()
	return nil
}

func (repo *eventRepository) Iterator(ctx context.Context, since time.Time) (events.Iterator, error) {
	ms := ulid.Timestamp(since)

	var last ulid.ULID
	last.SetTime(ms)

	ctx, cancel := context.WithCancelCause(ctx)

	return &iterator{
		id:      "influx-" + ulid.Make().String(),
		last:    last,
		fetchFn: repo.fetch,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (repo *eventRepository) fetch(batch int, last ulid.ULID) ([]*events.Event, error) {
	ms := last.Time()

	query := fmt.Sprintf(`SELECT id, topic, payload FROM %s WHERE time > %dms LIMIT %d`,
		repo.cfg.Measurement, ms, batch)

	q := influx.NewQuery(query, repo.cfg.Database, "")

	resp, err := repo.client.Query(q)
	if err != nil {
		return nil, err
	}

	if err := resp.Error(); err != nil {
		return nil, err
	}

	series := resp.Results[0].Series
	if len(series) == 0 || len(series[0].Values) == 0 {
		return nil, errors.New("event empty")
	}

	row := series[0]

	es := make([]*events.Event, len(row.Values))
	for i, value := range row.Values {
		idStr := value[1].(string)
		topic := value[2].(string)
		payloadStr := value[3].(string)

		id, err := ulid.Parse(idStr)
		if err != nil {
			return nil, err
		}

		payload, err := events.NewPayloadFromBytes([]byte(payloadStr))
		if err != nil {
			return nil, err
		}

		e := &events.Event{
			ID:      id,
			Topic:   topic,
			Payload: payload,
		}

		es[i] = e
	}

	return es, nil
}

func (repo *eventRepository) Close() error {
	if repo.cancel != nil {
		repo.cancel()
		repo.cancel = nil

		time.Sleep(3 * time.Second)
	}

	return repo.client.Close()
}

func (repo *eventRepository) Exec(command string) error {
	q := influx.NewQuery(command, "", "")
	resp, err := repo.client.Query(q)
	if err != nil {
		return err
	}

	if err := resp.Error(); err != nil {
		return err
	}

	return nil
}

type fetch func(batch int, last ulid.ULID) ([]*events.Event, error)

type iterator struct {
	id      string
	last    ulid.ULID
	fetchFn fetch

	ctx    context.Context
	cancel context.CancelCauseFunc
}

func (it *iterator) ID() string {
	return it.id
}

func (it *iterator) Fetch(batch int) ([]*events.Event, error) {
	events, err := it.fetchFn(batch, it.last)
	if err != nil {
		return nil, err
	}

	it.last = events[len(events)-1].ID

	return events, nil
}

func (it *iterator) Close(err error) {
	if it.cancel != nil {
		it.cancel(err)
	}

	it.cancel = nil
}

func (it *iterator) Done() <-chan struct{} {
	return it.ctx.Done()
}

func (it *iterator) Err() error {
	return it.ctx.Err()
}

type Config struct {
	influx.HTTPConfig
	influx.BatchPointsConfig
	Measurement string
	Duration    time.Duration
}

func parseConfig(dsn string) (*Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if u.Host == "" {
		return nil, errors.New("invalid address")
	}

	q := u.Query()

	if !q.Has("db") {
		return nil, errors.New("invalid database")
	}

	measurement := "events"
	if q.Has("measurement") {
		measurement = q.Get("measurement")
	}

	duration := 10 * time.Second
	if q.Has("duration") {
		durationStr := q.Get("duration")
		dur, err := time.ParseDuration(durationStr)
		if err != nil {
			return nil, err
		}

		duration = dur
	}

	return &Config{
		HTTPConfig: influx.HTTPConfig{
			Addr:     u.Scheme + "://" + u.Host,
			Username: q.Get("u"),
			Password: q.Get("p"),
		},
		BatchPointsConfig: influx.BatchPointsConfig{
			Database:        q.Get("db"),
			Precision:       q.Get("precision"),
			RetentionPolicy: q.Get("rp"),
		},
		Measurement: measurement,
		Duration:    duration,
	}, nil
}
