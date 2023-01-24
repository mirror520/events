package pubsub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/mirror520/events/conf"
)

type mqttTestSuite struct {
	suite.Suite
	pubSub MqttPubSub
}

func (suite *mqttTestSuite) SetupSuite() {
	cfg, err := conf.LoadConfig("../")
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	transport, ok := cfg.Transports["mqtt-test"]
	if !ok {
		suite.Fail("transport not found")
		return
	}

	transport.MqttConfig.ClientID = "test-" + time.Now().String()
	transport.MqttConfig.QoS = 1

	pubSub, err := NewMqttPubSub(transport.MqttConfig)
	if err != nil {
		suite.Fail(err.Error())
		return
	}

	suite.pubSub = pubSub
}

func (suite *mqttTestSuite) TestPubSub() {
	if err := suite.pubSub.Subscribe("hello/world", func(msg Message) {
		payload := string(msg.Payload())
		suite.Equal("Hello World", payload)
	}); err != nil {
		suite.Fail(err.Error())
		return
	}

	if err := suite.pubSub.Publish("hello/world", []byte("Hello World")); err != nil {
		suite.Fail(err.Error())
		return
	}
}

func TestMqttTestSuite(t *testing.T) {
	suite.Run(t, new(mqttTestSuite))
}
