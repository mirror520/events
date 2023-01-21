package pubsub

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/suite"

	"github.com/mirror520/events/conf"
)

type mqttTestSuite struct {
	suite.Suite
	pubSub MqttPubSub
}

func (suite *mqttTestSuite) SetupSuite() {
	err := conf.LoadConfig("../")
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	source, ok := conf.DataSource("mqtt-test")
	if !ok {
		suite.Fail("source not found")
		return
	}

	source.MqttConfig.ClientID = "test-" + time.Now().String()
	source.MqttConfig.QoS = 1

	pubSub, err := NewMqttPubSub(source.MqttConfig)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.pubSub = pubSub
}

func (suite *mqttTestSuite) TestPubSub() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	messages, err := suite.pubSub.Subscribe(ctx, "hello/world")
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	go func(ctx context.Context, messages <-chan *message.Message) {
		for {
			select {
			case <-ctx.Done():
				return

			case msg := <-messages:
				if msg == nil {
					continue
				}

				payload := string(msg.Payload)
				suite.Equal("Hello World", payload)
			}
		}
	}(ctx, messages)

	msg := message.NewMessage(watermill.NewUUID(), []byte("Hello World"))
	if err := suite.pubSub.Publish("hello/world", msg); err != nil {
		suite.Fail(err.Error())
		return
	}
}

func TestMqttTestSuite(t *testing.T) {
	suite.Run(t, new(mqttTestSuite))
}
