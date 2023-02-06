package model

import (
	"gopkg.in/yaml.v3"
)

type Config struct {
	Transports   map[string]*Transport
	Sources      []*Source
	Destinations []*Destination
}

func (cfg *Config) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Transports   map[string]*Transport
		Sources      []*Source
		Destinations []*Destination
	}

	if err := node.Decode(&raw); err != nil {
		return err
	}

	for _, transport := range raw.Transports {
		if transport.MqttConfig != nil {
			transport.Type = MQTT
		}
	}

	cfg.Transports = raw.Transports
	cfg.Sources = raw.Sources
	cfg.Destinations = raw.Destinations

	return nil
}

type TransportType int

const (
	MQTT TransportType = iota
	CHANNEL
)

type Transport struct {
	Type       TransportType
	MqttConfig *MqttConfig `yaml:"mqtt"`
}

type MqttConfig struct {
	ClientID string `yaml:"-"`
	Broker   string `yaml:"broker"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	QoS      byte   `yaml:"-"`
}

type Source struct {
	Transport string
	Topics    []string
}

type Destination struct {
	Transport string
}
