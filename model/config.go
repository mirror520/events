package model

import "gopkg.in/yaml.v3"

type Config struct {
	Sources map[string]*Source
}

func (cfg *Config) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Sources map[string]*Source
	}

	if err := node.Decode(&raw); err != nil {
		return err
	}

	for _, source := range raw.Sources {
		if source.MqttConfig != nil {
			source.Type = MQTT_SOURCE
		}
	}

	cfg.Sources = raw.Sources

	return nil
}

type SourceType int

const (
	MQTT_SOURCE SourceType = iota
)

type Source struct {
	Type       SourceType
	MqttConfig *MqttConfig `yaml:"mqtt"`
}

type MqttConfig struct {
	ClientID string   `yaml:"-"`
	Broker   string   `yaml:"broker"`
	Username string   `yaml:"username"`
	Password string   `yaml:"password"`
	Topics   []string `yaml:"topics"`
	QoS      byte     `yaml:"-"`
}
