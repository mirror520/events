package events

type Config struct {
	Persistence Persistence `yaml:"persistence"`
	Path        string      `yaml:"-"`
}

func (cfg *Config) SetPath(path string) {
	cfg.Path = path

	if cfg.Persistence.Driver == BadgerDB && cfg.Persistence.DSN == "" {
		cfg.Persistence.DSN = path + "/data"
	}
}

type Persistence struct {
	Driver StorageDriver `yaml:"driver"`
	DSN    string        `yaml:"dsn"`
}

type StorageDriver string

const (
	// InMemory

	InMem StorageDriver = "inmem"

	// KVDB

	BadgerDB StorageDriver = "badger"
	Redis    StorageDriver = "redis"

	// TSDB

	InfluxDB StorageDriver = "influxdb"

	// RDB

	MySQL      StorageDriver = "mysql"
	SQLServer  StorageDriver = "sqlserver"
	PostgreSQL StorageDriver = "postgres"
	SQLite     StorageDriver = "sqlite"
)
