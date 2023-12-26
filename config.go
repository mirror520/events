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

	// Key-value Database

	BadgerDB StorageDriver = "badger"
	Redis    StorageDriver = "redis"

	// Time Series Database

	InfluxDB   StorageDriver = "influxdb"
	InfluxDBv2 StorageDriver = "influxdb2"

	// Relational DBMS

	MySQL      StorageDriver = "mysql"
	SQLServer  StorageDriver = "sqlserver"
	PostgreSQL StorageDriver = "postgres"
	SQLite     StorageDriver = "sqlite"

	// Document Database
	MongoDB StorageDriver = "mongo"
)
