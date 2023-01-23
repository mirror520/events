package events

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/mirror520/events/model"
	"github.com/mirror520/events/model/event"
)

type Service interface {
	Up()
	Down()
	Store(e *event.Event) error
	Playback(topics []string, from time.Time) error
	StopPlayback() error
}

type service struct {
	sources map[string]*model.Source
	events  event.Repository
	task    *playbackTask // signle task, avoid confict

	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

func NewService(events event.Repository, sources map[string]*model.Source) Service {
	return &service{
		sources: sources,
		events:  events,
	}
}

func (svc *service) Up() {
	svc.log = zap.L().With(
		zap.String("service", "events"),
	)
	log := svc.log.With(zap.String("action", "up"))

	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx = ctx
	svc.cancel = cancel

	log.Info("done")
}

func (svc *service) Down() {
	svc.cancel()
	svc.log.Info("done", zap.String("action", "down"))
}

func (svc *service) Store(e *event.Event) error {
	err := svc.events.Store(e)
	if err != nil {
		return err
	}

	return nil
}

func (svc *service) Playback(topics []string, from time.Time) error {
	if svc.task != nil {
		return errors.New("busying")
	}

	var topicMap map[string]struct{}
	if len(topics) > 0 {
		topicMap = make(map[string]struct{})
		for _, topic := range topics {
			topicMap[topic] = struct{}{}
		}
	}

	ctx, cancel := context.WithCancel(svc.ctx)
	task := &playbackTask{
		from:   from,
		topics: topicMap,
		log: svc.log.With(
			zap.String("task", "playback"),
		),
		ctx:    ctx,
		cancel: cancel,
		events: svc.events,
	}

	go task.Start()

	svc.task = task
	return nil
}

func (svc *service) StopPlayback() error {
	if svc.task == nil {
		return errors.New("task not found")
	}

	svc.task.Stop()
	svc.task = nil
	return nil
}

type playbackTask struct {
	from   time.Time
	topics map[string]struct{}

	log    *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
	events event.Repository
}

func (task *playbackTask) Start() {
	log := task.log.With(
		zap.String("action", "start"),
	)

	eventCh := make(chan *event.Event)
	go func(ctx context.Context, ch <-chan *event.Event) {
		for {
			select {
			case <-ctx.Done():
				return

			case <-ch:
				// TODO: publish to destinations
			}
		}
	}(task.ctx, eventCh)

	err := task.events.Iterator(eventCh, task.from)
	if err != nil {
		log.Error(err.Error())
		return
	}

	log.Info("done")
}

func (task *playbackTask) Stop() {
	task.cancel()
	task.log.Info("done",
		zap.String("action", "stop"),
	)
}
