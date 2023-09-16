package database

import (
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/sirupsen/logrus"
)

// DBConfig ...
type DBConfig struct {
	Driver      string         `yaml:"driver"`
	Host        string         `yaml:"host"`
	Port        int            `yaml:"port"`
	DB          string         `yaml:"db"`
	User        string         `yaml:"user"`
	Password    string         `yaml:"password"`
	Location    *time.Location `yaml:"location"`
	MaxConn     int            `yaml:"max_conn"`
	IdleConn    int            `yaml:"idle_conn"`
	Timeout     time.Duration  `yaml:"timeout"`
	DialRetry   int            `yaml:"dial_retry"`
	DialTimeout time.Duration  `yaml:"dial_timeout"`
	//Logger      DatabaseLogger `yaml:"logger"`
}

// New ...
func (d *DBConfig) New() (*sql.DB, error) {
	switch d.Driver {
	case "mysql":
		return newMySQLConnection(d.mysqlDSN(), d.DialRetry, d.MaxConn, d.IdleConn, d.DialTimeout, d.Timeout)
	case "postgres":
		return newPostgresSQLConnection(d.postgresqlDSN(), d.DialRetry, d.MaxConn, d.IdleConn, d.DialTimeout, d.Timeout)
	default:
		return nil, errors.New("invalid database driver")
	}
}

// newMySQLConnection create connection to a MySQL/MariaDB server with passed arguments
// and returns DB struct.
func newMySQLConnection(
	baseDSN string,
	retry int,
	maxOpenConn int,
	maxIdleConn int,
	retryTimeout time.Duration,
	timeout time.Duration) (*sql.DB, error) {
	var db *sql.DB
	var err error
	counter := 0
	var id int

	db, err = sql.Open("mysql", baseDSN)
	if err != nil {
		return nil, fmt.Errorf("cannot open database %s: %s", baseDSN, err)
	}
	db.SetMaxOpenConns(maxOpenConn)
	db.SetMaxIdleConns(maxIdleConn)
	db.SetConnMaxLifetime(timeout)

	if retryTimeout == 0 {
		retryTimeout = time.Second
	}

	counter = 0
	for {
		<-time.NewTicker(retryTimeout).C
		counter++
		err := db.QueryRow("SELECT connection_id()").Scan(&id)
		if err == nil {
			break
		}

		logrus.Errorf("Cannot connect to database %s: %s", baseDSN, err)
		if counter >= retry {
			return nil, fmt.Errorf("cannot connect to database %s after %d retries: %s", baseDSN, counter, err)
		}
	}

	logrus.Info("Connected to mysql database: ", baseDSN)
	return db, nil
}

// newPostgresSQLConnection create connection to a Postgres server with passed arguments
// and returns DB struct.
func newPostgresSQLConnection(
	baseDSN string,
	retry int,
	maxOpenConn int,
	maxIdleConn int,
	retryTimeout time.Duration,
	timeout time.Duration) (*sql.DB, error) {
	var db *sql.DB
	var err error
	counter := 0
	var id int

	db, err = sql.Open("postgres", baseDSN)
	if err != nil {
		return nil, fmt.Errorf("cannot open database %s: %s", baseDSN, err)
	}
	db.SetMaxOpenConns(maxOpenConn)
	db.SetMaxIdleConns(maxIdleConn)
	db.SetConnMaxLifetime(timeout)

	counter = 0
	for {
		<-time.NewTicker(retryTimeout).C
		counter++
		err := db.QueryRow("SELECT pg_backend_pid()").Scan(&id)
		if err == nil {
			break
		}

		logrus.Errorf("Cannot connect to database %s: %s", baseDSN, err)
		if counter >= retry {
			return nil, fmt.Errorf("cannot connect to database %s after %d retries: %s", baseDSN, counter, err)
		}
	}

	logrus.Info("Connected to postgres database: ", baseDSN)

	return db, nil
}

func (d *DBConfig) mysqlDSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&collation=utf8mb4_general_ci&loc=%s", d.User, d.Password, d.Host, d.Port, d.DB, url.QueryEscape(d.Location.String()))
}

func (d *DBConfig) postgresqlDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", d.User, url.QueryEscape(d.Password), d.Host, d.Port, d.DB)
}
