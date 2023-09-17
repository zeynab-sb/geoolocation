package geoolocation

import (
	"database/sql"
	"errors"
	"github.com/zeynab-sb/geoolocation/database"
	"github.com/zeynab-sb/geoolocation/repository"
	"path/filepath"
	"time"
)

// Geo implements import csv method and provide a repository to access to model.
type Geo struct {
	db *sql.DB

	// It can be either mysql or postgres
	driver string

	// Access to model layer
	Repository repository.LocationRepository
}

// New - instantiate Geo with database config
func New(config *database.DBConfig) (*Geo, error) {
	db, err := config.New()
	if err != nil {
		return nil, err
	}

	repo := repository.NewLocationRepository(db)

	return &Geo{db: db, driver: config.Driver, Repository: repo}, nil
}

// Result is returned in ImportCSV
type Result struct {
	// The number of rows in the correct format and inserted in DB.
	acceptedRows int64

	// The number of corrupted rows.
	discardedRows int64

	// The whole amount of time that it took to import CSV in seconds
	timeTaken float64
}

func (g *Geo) ImportCSV(path string, concurrency int) (*Result, error) {
	if filepath.Ext(path) != ".csv" {
		return nil, errors.New("invalid file extension")
	}

	start := time.Now()

	data := make(chan csvData, concurrency)
	signal := make(chan bool)
	importer := csvImporter{
		path:        path,
		concurrency: concurrency,
		driver:      g.driver,
		db:          g.db,
		data:        data,
		signal:      signal,
	}

	if err := importer.setUpSanitizer(); err != nil {
		return nil, err
	}

	totalRows, err := importer.read()
	if err != nil {
		return nil, err
	}

	insertedRows, err := importer.load()
	if err != nil {
		return nil, err
	}

	importer.clean()

	finished := time.Now()

	return &Result{
		acceptedRows:  insertedRows,
		discardedRows: totalRows - insertedRows,
		timeTaken:     finished.Sub(start).Seconds(),
	}, nil
}
