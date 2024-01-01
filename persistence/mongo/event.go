package mongo

import (
	"context"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.uber.org/zap"

	"github.com/mirror520/events"
)

type EventRepository interface {
	events.Repository
	DropDatabase(name string) error
}

type eventRepository struct {
	log    *zap.Logger
	cfg    *Config
	db     *mongo.Database
	docs   []any
	ctx    context.Context
	cancel context.CancelFunc
	sync.Mutex
}

func NewEventRepository(cfg events.Persistence) (events.Repository, error) {
	ctx, cancel := context.WithCancel(context.Background())

	conf, err := parseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}

	repo := &eventRepository{
		log: zap.L().With(
			zap.String("persistence", "mongo"),
		),
		cfg:    conf,
		docs:   make([]any, 0),
		ctx:    ctx,
		cancel: cancel,
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(conf.URI))
	if err != nil {
		return nil, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	db := client.Database(conf.Database)

	tso := options.TimeSeries().SetTimeField("_time")
	opts := options.CreateCollection().SetTimeSeriesOptions(tso)

	if err := db.CreateCollection(ctx, conf.Collection, opts); err != nil {
		cmdErr, ok := err.(mongo.CommandError)
		if !ok || cmdErr.Code != 48 {
			return nil, err
		}
	}

	repo.db = db

	go repo.batchWriteHandler(repo.ctx)

	return repo, nil
}

func (repo *eventRepository) batchWriteHandler(ctx context.Context) {
	log := repo.log.With(
		zap.String("action", "batch_write"),
	)

	coll := repo.db.Collection("events")

	ticker := time.NewTicker(repo.cfg.Duration)
	for {
		select {
		case <-ctx.Done():
			repo.Lock()
			if size := len(repo.docs); size > 0 {
				log := log.With(zap.Int("points", size))

				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, err := coll.InsertMany(ctx, repo.docs)
				if err != nil {
					log.Error(err.Error())
				} else {
					log.Info("points written")
				}

				cancel()
				repo.docs = make([]any, 0)
			}
			repo.Unlock()

			log.Info("done")
			return

		case <-ticker.C:
			repo.Lock()
			if size := len(repo.docs); size > 0 {
				log := log.With(zap.Int("points", size))

				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, err := coll.InsertMany(ctx, repo.docs)
				if err != nil {
					log.Error(err.Error())
				} else {
					log.Info("points written")
				}

				cancel()
				repo.docs = make([]any, 0)
			}
			repo.Unlock()
		}
	}
}

func (repo *eventRepository) Store(e *events.Event) error {
	repo.Lock()

	doc := NewEvent(e)
	repo.docs = append(repo.docs, doc)

	repo.Unlock()
	return nil
}

func (repo *eventRepository) Iterator(ctx context.Context, since time.Time) (events.Iterator, error) {
	var (
		prefetchSize = 10
		ch           = make(chan *events.Event, prefetchSize*2) // buffer + prefetch
		errCh        = make(chan error)
	)

	ctx, cancel := context.WithCancelCause(ctx)
	go func(ctx context.Context, ch chan<- *events.Event, errCh chan<- error) {
		var last ulid.ULID
		last.SetTime(ulid.Timestamp(since))

		coll := repo.db.Collection(repo.cfg.Collection)

		ticker := time.NewTicker(500 * time.Millisecond)
		for {
			select {
			case <-ctx.Done():
				errCh <- nil
				return

			case <-ticker.C:
				ms := last.Time()
				ts := time.UnixMilli(int64(ms))

				filter := bson.D{
					{
						Key: "_time",
						Value: bson.D{
							{
								Key:   "$gt",
								Value: ts,
							},
						},
					},
				}

				cursor, err := coll.Find(ctx, filter)
				if err == nil {
					for cursor.Next(ctx) {
						var result *Event
						err = cursor.Decode(&result)
						if err != nil {
							break
						} else {
							e := result.Event()

							ch <- e
							last = e.ID
						}
					}

					cursor.Close(ctx)
				}

				if err != nil {
					errCh <- err
					return
				}
			}
		}
	}(ctx, ch, errCh)

	it := &iterator{
		id:      "mongo-" + ulid.Make().String(),
		timeout: 200 * time.Millisecond,
		ch:      ch,
		errCh:   errCh,
		ctx:     ctx,
		cancel:  cancel,
	}

	go it.handle(ctx, errCh)

	return it, nil
}

func (repo *eventRepository) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return repo.db.Client().Disconnect(ctx)
}

func (repo *eventRepository) DropDatabase(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return repo.db.Drop(ctx)
}

type iterator struct {
	id      string
	timeout time.Duration

	ch    <-chan *events.Event
	errCh <-chan error

	ctx    context.Context
	cancel context.CancelCauseFunc
}

func (it *iterator) ID() string {
	return it.id
}

func (it *iterator) Fetch(batch int) ([]*events.Event, error) {
	es := make([]*events.Event, 0)
	after := time.After(it.timeout)
	for {
		select {
		case e := <-it.ch:
			es = append(es, e)
			if len(es) == batch {
				return es, nil
			}

		case <-after:
			if len(es) == 0 {
				return nil, events.ErrTimeout
			}

			return es, nil
		}
	}
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

func (it iterator) handle(ctx context.Context, errCh <-chan error) {
	for {
		select {
		case <-ctx.Done():
			return

		case err := <-errCh:
			it.Close(err)
			return
		}
	}
}
