package influxdb

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"

	influx "github.com/influxdata/influxdb1-client/v2"
	"github.com/oklog/ulid/v2"

	"github.com/mirror520/events"
)

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

	go repo.batchWrite(ctx)

	return repo, nil
}

func (repo *eventRepository) batchWrite(ctx context.Context) {
	log := repo.log.With(
		zap.String("action", "batchWrite"),
	)

	ticker := time.NewTicker(10 * time.Second)
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

	fields := map[string]any{
		"id":      e.ID.String(),
		"payload": string(e.Payload),
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

	return &iterator{
		id:      "influx-" + ulid.Make().String(),
		last:    last,
		fetchCh: repo.fetch,
		done:    make(chan struct{}),
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
		payload := value[3].(string)

		id, err := ulid.Parse(idStr)
		if err != nil {
			return nil, err
		}

		e := &events.Event{
			ID:      id,
			Topic:   topic,
			Payload: []byte(payload),
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

type fetch func(batch int, last ulid.ULID) ([]*events.Event, error)

type iterator struct {
	id      string
	last    ulid.ULID
	fetchCh fetch

	done chan struct{}
	err  error
}

func (it *iterator) ID() string {
	return it.id
}

func (it *iterator) Fetch(batch int) ([]*events.Event, error) {
	events, err := it.fetchCh(batch, it.last)
	if err != nil {
		return nil, err
	}

	it.last = events[len(events)-1].ID

	return events, nil
}

func (it *iterator) Close(err error) {
	it.err = err
	it.done <- struct{}{}
}

func (it *iterator) Done() <-chan struct{} {
	return it.done
}

func (it *iterator) Err() error {
	return it.err
}

type Config struct {
	influx.HTTPConfig
	influx.BatchPointsConfig
	Measurement string
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
	}, nil
}
