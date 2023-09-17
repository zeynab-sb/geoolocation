package geoolocation

import (
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

type csvImporter struct {
	path          string
	sanitizedPath string
	concurrency   int
	driver        string
	db            *sql.DB
	data          chan csvData
	signal        chan bool
}

var csvHeader []string

// countryCodeRegex contains code pattern that is two capital letter .
// sqlPatternRegex contains some sql commands.
var countryCodeRegex, sqlPatternRegex *regexp.Regexp

func init() {
	csvHeader = []string{"ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value"}
	countryCodeRegex = regexp.MustCompile(`[A-Z]{2}`)
	sqlPatternRegex = regexp.MustCompile(`(?i)\b(?:SELECT|INSERT|UPDATE|DELETE|UNION|OR|DROP|EXEC(UTE)?|ALTER|CREATE|TRUNCATE)\b`)
}

func (i *csvImporter) setUpSanitizer() error {
	i.sanitizedPath = fmt.Sprintf("%s_sanitized.csv", strings.TrimSuffix(filepath.Base(i.path), ".csv"))
	sanitizedFile, err := os.Create(i.sanitizedPath)
	if err != nil {
		return err
	}

	go func(file *os.File) {
		defer file.Close()

		writer := csv.NewWriter(file)
		defer writer.Flush()

		var wg sync.WaitGroup
		wg.Add(i.concurrency)

		var m sync.Mutex
		for j := 0; j < i.concurrency; j++ {
			go func() {
				defer wg.Done()
				for d := range i.data {
					err := d.sanitize()
					if err != nil {
						logrus.Warnf("data rejected: %v, value: %s", err, d)
						continue
					}

					m.Lock()
					if err := writer.Write([]string{d.ipAddress, d.countryCode, d.country, d.city, d.latitude, d.longitude, d.mysteryValue}); err != nil {
						logrus.Errorf("error writing a record: %s :%v", d, err)
					}
					m.Unlock()
				}
			}()
		}

		wg.Wait()
		i.signal <- true
	}(sanitizedFile)

	return nil
}

func (i *csvImporter) read() (int64, error) {
	defer close(i.data)

	file, err := os.Open(i.path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return 0, errors.New("error reading csv header")
	}

	if len(header) != len(csvHeader) {
		return 0, errors.New("invalid csv header")
	}

	for j := range csvHeader {
		if header[j] != csvHeader[j] {
			return 0, errors.New("invalid csv header")
		}
	}

	var totalRows int64
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}

		totalRows++
		if err != nil {
			logrus.Errorf("error reading a record: %s :%v", record, err)
			continue
		}

		d := csvData{
			ipAddress:    record[0],
			countryCode:  record[1],
			country:      record[2],
			city:         record[3],
			latitude:     record[4],
			longitude:    record[5],
			mysteryValue: record[6],
		}

		i.data <- d
	}

	return totalRows, nil
}

func (i *csvImporter) load() (int64, error) {
	<-i.signal

	switch i.driver {
	case "mysql":
		mysql.RegisterLocalFile(i.sanitizedPath)
		r, err := i.db.Exec("LOAD DATA LOCAL INFILE '" + i.sanitizedPath + "' INTO TABLE locations FIELDS TERMINATED BY \",\" LINES TERMINATED BY \"\\n\" (ip_address,country_code,country,city,latitude,longitude,mystery_value);")
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

func (i *csvImporter) clean() {
	err := os.Remove(i.sanitizedPath)
	if err != nil {
		logrus.Errorf("error removing sanitized file: %v", err)
	}
}

type csvData struct {
	ipAddress    string
	countryCode  string
	country      string
	city         string
	latitude     string
	longitude    string
	mysteryValue string
}

func (d *csvData) sanitize() error {
	if net.ParseIP(d.ipAddress) == nil {
		return errors.New("invalid ip")
	}

	if !countryCodeRegex.MatchString(d.countryCode) {
		return errors.New("invalid country code")
	}

	if sqlPatternRegex.MatchString(d.country) {
		return errors.New("invalid country")
	}

	if strings.Contains(d.country, "'") {
		d.country = fmt.Sprintf("'%s'", d.country)
	}

	if sqlPatternRegex.MatchString(d.city) {
		return errors.New("invalid city")
	}

	if strings.Contains(d.city, "'") {
		d.city = fmt.Sprintf("'%s'", d.city)
	}

	fLat, err := strconv.ParseFloat(d.latitude, 64)
	if err != nil {
		return errors.New("invalid latitude")
	}

	if !(-90 <= fLat && fLat <= 90) {
		return errors.New("invalid latitude")
	}

	fLng, err := strconv.ParseFloat(d.longitude, 64)
	if err != nil {
		return errors.New("invalid longitude")
	}

	if !(-180 <= fLng && fLng <= 180) {
		return errors.New("invalid longitude")
	}

	if _, err := strconv.ParseInt(d.mysteryValue, 10, 64); err != nil {
		return errors.New("invalid mystery value")
	}

	return nil
}
