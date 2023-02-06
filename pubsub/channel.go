package pubsub

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"sync"
)

type ChannelPubSub interface {
	PubSub
}

type channelPubSub struct {
	pattern     *regexp.Regexp
	subscribers []*subscriber
	ctx         context.Context
	cancel      context.CancelFunc
	sync.RWMutex
}

func NewChannelPubSub() (ChannelPubSub, error) {
	pubSub := new(channelPubSub)

	pattern, err := regexp.Compile(`[\w+#/]+`)
	if err != nil {
		return nil, err
	}
	pubSub.pattern = pattern

	pubSub.subscribers = make([]*subscriber, 0)

	ctx, cancel := context.WithCancel(context.Background())
	pubSub.ctx = ctx
	pubSub.cancel = cancel

	return pubSub, nil
}

func (pubSub *channelPubSub) Publish(topic string, payload []byte) error {
	pubSub.RLock()
	defer pubSub.RUnlock()

	for _, subscribe := range pubSub.subscribers {
		if ok := subscribe.pattern.MatchString(topic); !ok {
			continue
		}

		subscribe.ch <- NewMessage(topic, payload)
	}

	return nil
}

func (pubSub *channelPubSub) Subscribe(topic string, callback MessageHandler) error {
	if ok := pubSub.pattern.MatchString(topic); !ok {
		return errors.New("invalid topic")
	}

	expr := topic
	expr = strings.Replace(expr, "+", "\\w+", -1)
	expr = strings.Replace(expr, "#", ".+", -1)

	pattern, err := regexp.Compile(expr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(pubSub.ctx)
	sub := &subscriber{
		topic:    topic,
		pattern:  pattern,
		ch:       make(chan Message),
		callback: callback,
		cancel:   cancel,
	}

	go sub.receive(ctx)

	pubSub.Lock()
	pubSub.subscribers = append(pubSub.subscribers, sub)
	pubSub.Unlock()

	return nil
}

func (pubSub *channelPubSub) Close() error {
	pubSub.cancel()
	return nil
}

type subscriber struct {
	topic    string
	pattern  *regexp.Regexp
	ch       chan Message
	callback MessageHandler
	cancel   context.CancelFunc
}

func (sub *subscriber) receive(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case msg := <-sub.ch:
			sub.callback(msg)
		}
	}
}
