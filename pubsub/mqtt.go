package pubsub

import (
	"context"
	"strconv"

	"github.com/ThreeDotsLabs/watermill/message"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MqttPubSub interface {
	message.Publisher
	message.Subscriber
}

type mqttPubSub struct {
	client mqtt.Client
	qos    byte
}

type MqttConfig struct {
	Broker   string
	Username string
	Password string
	QoS      byte
}

func NewMqttPubSub(cfg MqttConfig) (MqttPubSub, error) {
	opts := mqtt.NewClientOptions().
		SetClientID("").
		AddBroker(cfg.Broker).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password)

	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()

	if err := token.Error(); err != nil {
		return nil, err
	}

	return &mqttPubSub{client, cfg.QoS}, nil
}

func (pubsub *mqttPubSub) Publish(topic string, messages ...*message.Message) error {
	for _, message := range messages {
		token := pubsub.client.Publish(topic, pubsub.qos, false, message)
		token.Wait()

		if err := token.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (pubsub *mqttPubSub) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	msg := make(chan *message.Message)

	token := pubsub.client.Subscribe(topic, pubsub.qos, func(c mqtt.Client, m mqtt.Message) {
		msg <- &message.Message{
			UUID:    strconv.Itoa(int(m.MessageID())),
			Payload: m.Payload(),
		}
	})

	token.Wait()
	if err := token.Error(); err != nil {
		return nil, err
	}

	return msg, nil
}

func (pubsub *mqttPubSub) Close() error {
	pubsub.client.Disconnect(200)
	return nil
}
