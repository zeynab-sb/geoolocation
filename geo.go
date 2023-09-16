package geoolocation

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"github.com/zeynab-sb/geoolocation/database"
	"github.com/zeynab-sb/geoolocation/repository"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

// ipRegex contains ipv4 pattern xxxx.xxxx.xxxx.xxxx.
// countryCodeRegex contains code pattern that is two capital letter .
// sqlPatternRegex contains some sql commands.
var ipRegex, countryCodeRegex, sqlPatternRegex *regexp.Regexp

func init() {
	ipRegex = regexp.MustCompile(`^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$`)
	countryCodeRegex = regexp.MustCompile(`[A-Z]{2}`)
	sqlPatternRegex = regexp.MustCompile(`(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|UNION|AND|OR|DROP|EXEC(UTE)?|ALTER|CREATE|TRUNCATE)\b`)
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

// ImportCSV ...
func (g *Geo) ImportCSV(path string, concurrency int) (*Result, error) {
	start := time.Now()
	if filepath.Ext(path) != ".csv" {
		return nil, errors.New("invalid file extension")
	}

	sanitizedPath := fmt.Sprintf("%s_sanitized.csv", strings.TrimSuffix(filepath.Base(path), ".csv"))
	totalRows, err := sanitizeCSV(path, sanitizedPath, concurrency)
	if err != nil {
		return nil, err
	}

	insertedRows, err := loadData(g.db, sanitizedPath, g.driver)
	if err != nil {
		return nil, err
	}

	err = os.Remove(sanitizedPath)
	if err != nil {
		return nil, err
	}

	finished := time.Now()
	result := &Result{
		acceptedRows:  insertedRows,
		discardedRows: totalRows - insertedRows,
		timeTaken:     finished.Sub(start).Seconds(),
	}

	return result, nil
}

func sanitizeCSV(currentPath string, sanitizedPath string, concurrency int) (int64, error) {
	var wg sync.WaitGroup
	wg.Add(concurrency)
	ch := make(chan []string, concurrency)

	currentFile, err := os.Open(currentPath)
	if err != nil {
		return 0, err
	}
	defer currentFile.Close()

	validHeader := [7]string{"ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value"}
	reader := csv.NewReader(currentFile)
	header, err := reader.Read()
	if err != nil {
		return 0, err
	}

	if len(header) != len(validHeader) {
		return 0, errors.New("invalid headers in csv file")
	}

	for i := 0; i < 7; i++ {
		if header[i] != validHeader[i] {
			return 0, errors.New("invalid headers in csv file")
		}
	}

	sanitizedFile, err := os.Create(sanitizedPath)
	if err != nil {
		return 0, err
	}
	defer sanitizedFile.Close()

	writer := csv.NewWriter(sanitizedFile)
	defer writer.Flush()

	var m sync.Mutex
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for r := range ch {
				if len(r) < 6 {
					continue
				}

				if err := validateIP(r[0]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[0])
					continue
				}

				if err := validateCountryCode(r[1]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[1])
					continue
				}

				if err := validateName(r[2]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[2])
					continue
				}

				r[2] = normalizeName(r[2])

				if err := validateName(r[3]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[3])
					continue
				}

				r[3] = normalizeName(r[3])

				if err := validateLat(r[4]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[4])
					continue
				}

				if err := validateLng(r[5]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[5])
					continue
				}

				//TODO: validate mystery value
				if err := validateIP(r[0]); err != nil {
					logrus.Warnf("row rejected: %v, value: %s", err, r[6])
					continue
				}

				m.Lock()
				if err := writer.Write(r); err != nil {
					fmt.Println(err)
				}
				m.Unlock()
			}
		}()
	}

	var totalRows int64
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		totalRows++
		if err != nil {
			//TODO: error handling
			log.Fatal(err)
		}

		ch <- record
	}

	close(ch)
	wg.Wait()
	return totalRows, nil
}

func loadData(db *sql.DB, path string, driver string) (int64, error) {
	switch driver {
	case "mysql":
		mysql.RegisterLocalFile(path)
		r, err := db.Exec("LOAD DATA LOCAL INFILE '" + path + "' INTO TABLE locations FIELDS TERMINATED BY \",\" LINES TERMINATED BY \"\\n\" (ip_address,country_code,country,city,latitude,longitude,mystery_value);")
		if err != nil {
			return 0, err
		}

		insertedRows, err := r.RowsAffected()
		if err != nil {
			return 0, err
		}

		return insertedRows, nil
	//TODO: add load command with postgres
	case "postgres":
		return 0, nil
	default:
		return 0, errors.New("invalid database driver")
	}
}

// validateIP ...
func validateIP(ip string) error {
	if ipRegex.MatchString(ip) {
		return nil
	}
	return errors.New("invalid ip")
}

// validateLat ...
func validateLat(lat string) error {
	f, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		return errors.New("invalid latitude")
	}

	if -90 <= f && f <= 90 {
		return nil
	}

	return errors.New("invalid latitude")
}

// validateLng ...
func validateLng(lat string) error {
	f, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		return errors.New("invalid latitude")
	}

	if -180 <= f && f <= 180 {
		return nil
	}

	return errors.New("invalid latitude")
}

// validateCountryCode ...
func validateCountryCode(code string) error {
	if countryCodeRegex.MatchString(code) {
		return nil
	}

	return errors.New("invalid country code")
}

// validateName ...
func validateName(name string) error {
	if sqlPatternRegex.MatchString(name) {
		return errors.New("invalid name")
	}

	return nil
}

// normalizeName ...
func normalizeName(name string) string {
	if strings.Contains(name, "\"") {
		return fmt.Sprintf("\"%s\"", name)
	}

	if strings.Contains(name, "'") {
		return fmt.Sprintf("'%s'", name)
	}

	return name
}
