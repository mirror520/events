package pubsub

import (
	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/mirror520/events/model"
)

type MqttPubSub interface {
	PubSub
}

type mqttPubSub struct {
	cfg    *model.MqttConfig
	client mqtt.Client
}

func NewMqttPubSub(cfg *model.MqttConfig) (MqttPubSub, error) {
	opts := mqtt.NewClientOptions().
		SetClientID(cfg.ClientID).
		AddBroker(cfg.Broker).
		SetUsername(cfg.Username).
		SetPassword(cfg.Password)

	client := mqtt.NewClient(opts)

	token := client.Connect()
	token.Wait()

	if err := token.Error(); err != nil {
		return nil, err
	}

	return &mqttPubSub{cfg, client}, nil
}

func (pubSub *mqttPubSub) Publish(topic string, payload []byte) error {
	token := pubSub.client.Publish(topic, pubSub.cfg.QoS, false, payload)

	token.Wait()
	err := token.Error()
	if err != nil {
		return err
	}

	return nil
}

func (pubSub *mqttPubSub) Subscribe(topic string, callback MessageHandler) error {
	token := pubSub.client.Subscribe(
		topic,
		pubSub.cfg.QoS,
		func(client mqtt.Client, msg mqtt.Message) {
			callback(msg)
		},
	)

	token.Wait()
	if err := token.Error(); err != nil {
		return err
	}

	return nil
}

func (pubSub *mqttPubSub) Close() error {
	pubSub.client.Disconnect(200)
	return nil
}
