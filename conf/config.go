package conf

import (
	"os"

	"gopkg.in/yaml.v3"

	"github.com/mirror520/events/model"
)

var cfg *model.Config

func LoadConfig(path string) error {
	f, err := os.Open(path + "/config.yaml")
	if err != nil {
		f, err = os.Open(path + "/config.example.yaml")
		if err != nil {
			return err
		}
	}
	defer f.Close()

	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return err
	}

	return nil
}

func DataSource(name string) (*model.Source, bool) {
	if cfg == nil {
		return nil, false
	}

	source, ok := cfg.Sources[name]
	return source, ok
}
