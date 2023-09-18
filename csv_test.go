package geoolocation

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/zeynab-sb/geoolocation/database"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

type CSVTestSuite struct {
	suite.Suite
	sqlMock   sqlmock.Sqlmock
	db        *sql.DB
	patch     *gomonkey.Patches
	logBuffer bytes.Buffer
}

func (suite *CSVTestSuite) SetupSuite() {
	mockDB, sqlMock, err := sqlmock.New()
	if err != nil {
		log.Fatal("error in new connection", err)
	}

	suite.sqlMock = sqlMock
	suite.db = mockDB

	mockCtrl := gomock.NewController(suite.T())
	defer mockCtrl.Finish()

	suite.patch = gomonkey.NewPatches()

	suite.patch.ApplyFunc(time.Now, func() time.Time {
		return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	})
}

func (suite *CSVTestSuite) SetupTest() {
	suite.patch.ApplyFunc(time.Now, func() time.Time {
		return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	})
	suite.logBuffer.Truncate(0)
}

func (suite *CSVTestSuite) TearDownSuit() {
	suite.patch.Reset()

	_ = suite.db.Close()
}

func (suite *CSVTestSuite) TearDownTest() {
	suite.patch.Reset()
}

func (suite *CSVTestSuite) newImporter(path string, concurrency int) *csvImporter {
	data := make(chan csvData, 1)
	signal := make(chan bool)
	return &csvImporter{
		path:        path,
		concurrency: concurrency,
		driver:      &database.MySQLDriver{DB: suite.db},
		db:          suite.db,
		data:        data,
		signal:      signal,
	}
}

func (suite *CSVTestSuite) TestCSV_setUpSanitizer_CreateSanitizedFile_Failure() {
	require := suite.Require()
	expectedError := "error creating file"

	suite.patch.ApplyFuncReturn(os.OpenFile, nil, errors.New("error creating file"))

	i := suite.newImporter("data.csv", 1)
	err := i.setUpSanitizer()
	require.EqualError(err, expectedError)
}

func (suite *CSVTestSuite) TestCSV_setUpSanitizer_Success() {
	require := suite.Require()
	expectedRow := []string{"127.0.0.1", "AC", "Test", "Test", "-35.437661078966926", "-134.6494137784682", "2147483647"}
	expectedLogMsg := "time=\"2020-01-01T00:00:00Z\" level=warning msg=\"data rejected: invalid ip, value: {127.0.0 AC Test Test -35.437661078966926 -134.6494137784682 2147483647}\"\n"

	data := map[string]csvData{
		"correct_data": {
			ipAddress:    "127.0.0.1",
			countryCode:  "AC",
			country:      "Test",
			city:         "Test",
			latitude:     "-35.437661078966926",
			longitude:    "-134.6494137784682",
			mysteryValue: "2147483647",
		},
		"invalid_data": {
			ipAddress:    "127.0.0",
			countryCode:  "AC",
			country:      "Test",
			city:         "Test",
			latitude:     "-35.437661078966926",
			longitude:    "-134.6494137784682",
			mysteryValue: "2147483647",
		},
	}

	suite.logBuffer.Truncate(0)
	logrus.SetOutput(&suite.logBuffer)

	importer := suite.newImporter("data.csv", 1)
	err := importer.setUpSanitizer()

	for _, d := range data {
		importer.data <- d
	}

	close(importer.data)

	<-importer.signal
	require.NoError(err)

	file, err := os.Open("../data_sanitized.csv")
	require.NoError(err)

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	require.NoError(err)

	err = os.Remove("../data_sanitized.csv")
	require.NoError(err)

	require.Equal(1, len(records))
	require.Equal(expectedRow, records[0])
	require.Contains(suite.logBuffer.String(), expectedLogMsg)
}

func createCSV(data [][]string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	writer := csv.NewWriter(file)
	err = writer.WriteAll(data)
	if err != nil {
		return err
	}

	writer.Flush()
	err = file.Close()
	if err != nil {
		return err
	}

	return nil
}

func deleteCSV(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}

	return nil
}

func (suite *CSVTestSuite) TestCSV_read_OpenFile_Failure() {
	require := suite.Require()
	expectedError := "error opening file"

	suite.patch.ApplyFuncReturn(os.OpenFile, nil, errors.New("error opening file"))

	i := suite.newImporter("data.csv", 1)
	_, err := i.read()
	require.EqualError(err, expectedError)
}

func (suite *CSVTestSuite) TestCSV_read_ReadingHeader_Failure() {
	require := suite.Require()
	expectedError := "error reading csv header"

	err := createCSV([][]string{
		{"ip_address", "city"},
		{"127.0.0.1", "test"},
	}, "data1.csv")
	require.NoError(err)

	var r csv.Reader
	suite.patch.ApplyMethodReturn(&r, "Read", nil, errors.New("error"))

	i := suite.newImporter("data1.csv", 1)
	_, err = i.read()
	require.EqualError(err, expectedError)

	err = deleteCSV("data1.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_read_InvalidHeaderLength_Failure() {
	require := suite.Require()
	expectedError := "invalid csv header"

	err := createCSV([][]string{
		{"ip_address", "city"},
		{"127.0.0.1", "test"},
	}, "data2.csv")
	require.NoError(err)

	i := suite.newImporter("data2.csv", 1)
	_, err = i.read()
	require.EqualError(err, expectedError)

	err = deleteCSV("data2.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_read_InvalidHeader_Failure() {
	require := suite.Require()
	expectedError := "invalid csv header"

	err := createCSV([][]string{
		{"col1", "col2", "col3", "col4", "col5", "col6", "col7"},
		{"test", "test", "test", "test", "test", "test", "test"},
	}, "data3.csv")
	require.NoError(err)

	i := suite.newImporter("data3.csv", 1)
	_, err = i.read()
	require.EqualError(err, expectedError)

	err = deleteCSV("data3.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_read_Success() {
	require := suite.Require()
	expectedRows := int64(3)
	expectedData := []csvData{
		{
			ipAddress:    "127.0.0.1",
			countryCode:  "TA",
			country:      "test",
			city:         "test",
			latitude:     "48.92021642445653",
			longitude:    "14.900399560492929",
			mysteryValue: "2147483647",
		},
		{
			ipAddress:    "127.0.0.2",
			countryCode:  "TB",
			country:      "test",
			city:         "test",
			latitude:     "48.92021642545653",
			longitude:    "14.900399560892929",
			mysteryValue: "2147493647",
		},
	}
	expectedLogMsg := "time=\"2020-01-01T00:00:00Z\" level=error msg=\"error reading a record: [test test test test test test] :record on line 4: wrong number of fields\"\n"

	err := createCSV([][]string{
		{"ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value"},
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"},
		{"test", "test", "test", "test", "test", "test"},
	}, "data4.csv")
	require.NoError(err)

	i := suite.newImporter("data4.csv", 1)

	var wg sync.WaitGroup
	wg.Add(1)
	var receivedData []csvData
	go func(i *csvImporter) {
		defer wg.Done()
		for d := range i.data {
			receivedData = append(receivedData, d)
		}
	}(i)

	suite.logBuffer.Truncate(0)
	logrus.SetOutput(&suite.logBuffer)

	total, err := i.read()
	require.NoError(err)

	wg.Wait()
	require.Equal(expectedRows, total)
	require.Equal(expectedData, receivedData)
	require.Contains(suite.logBuffer.String(), expectedLogMsg)

	err = deleteCSV("data4.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_load_MySQL_DatabaseErr_Failure() {
	require := suite.Require()
	expectedError := "database error"

	err := createCSV([][]string{
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"},
	}, "../data5.csv")
	require.NoError(err)

	i := suite.newImporter("../data5.csv", 1)
	i.sanitizedPath = "../data5.csv"

	suite.sqlMock.ExpectExec("LOAD DATA LOCAL INFILE '../data5.csv' IGNORE INTO TABLE locations (.+)").
		WillReturnError(errors.New("database error"))

	go func() {
		i.signal <- true
	}()

	_, err = i.load()
	require.EqualError(err, expectedError)

	err = deleteCSV("../data5.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_load_MySQL_Success() {
	require := suite.Require()
	expectedRows := int64(2)

	err := createCSV([][]string{
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"},
	}, "../data6.csv")
	require.NoError(err)

	i := suite.newImporter("data6.csv", 1)
	i.sanitizedPath = "../data6.csv"

	suite.sqlMock.ExpectExec("LOAD DATA LOCAL INFILE '../data6.csv' IGNORE INTO TABLE locations (.+)").
		WillReturnResult(sqlmock.NewResult(2, 2))

	go func() {
		i.signal <- true
	}()

	inserted, err := i.load()
	require.NoError(err)
	require.Equal(expectedRows, inserted)

	err = deleteCSV("../data6.csv")
	require.NoError(err)
}

func (suite *CSVTestSuite) TestCSV_clean_Failure() {
	require := suite.Require()
	expectedLogMsg := "time=\"2020-01-01T00:00:00Z\" level=error msg=\"error removing sanitized file: error\"\n"

	i := suite.newImporter("data7.csv", 1)
	i.sanitizedPath = "data7.csv"

	suite.logBuffer.Truncate(0)
	logrus.SetOutput(&suite.logBuffer)

	suite.patch.ApplyFuncReturn(os.Remove, errors.New("error"))
	i.clean()
	require.Contains(suite.logBuffer.String(), expectedLogMsg)
}

func (suite *CSVTestSuite) TestCSV_clean_Success() {
	require := suite.Require()

	err := createCSV([][]string{
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"},
	}, "data8.csv")
	require.NoError(err)

	i := suite.newImporter("data8.csv", 1)
	i.sanitizedPath = "data8.csv"

	suite.logBuffer.Truncate(0)
	logrus.SetOutput(&suite.logBuffer)

	i.clean()

	_, err = os.Open("data8.csv")
	require.EqualError(err, errors.New("open data8.csv: no such file or directory").Error())
}

func (suite *CSVTestSuite) TestCSV_sanitize() {
	require := suite.Require()

	tests := []struct {
		desc            string
		csvData         csvData
		expectedError   error
		expectedCSVData csvData
	}{
		{
			"Valid CSV Data",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			nil,
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid ip",
			csvData{
				ipAddress:    "127.0.",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid ip"),
			csvData{
				ipAddress:    "127.0.",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid country code",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "B",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid country code"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "B",
				country:      "test",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid country",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "select",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid country"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "select",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Country contains '",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "te'st",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			nil,
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "'te'st'",
				city:         "test",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid city",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "select",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid city"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "select",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"City contains '",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "te'st",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			nil,
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "'te'st'",
				latitude:     "48.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid lat not float",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "4gh",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid latitude"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "4gh",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid lat",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "148.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid latitude"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "148.92021642445653",
				longitude:    "14.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid lng not float",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.900399560492929",
				longitude:    "4gh",
				mysteryValue: "2147483647",
			},
			errors.New("invalid longitude"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.900399560492929",
				longitude:    "4gh",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid lng",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.92021642445653",
				longitude:    "240.900399560492929",
				mysteryValue: "2147483647",
			},
			errors.New("invalid longitude"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.92021642445653",
				longitude:    "240.900399560492929",
				mysteryValue: "2147483647",
			},
		},
		{
			"Invalid mystery value",
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.92021642445653",
				longitude:    "40.900399560492929",
				mysteryValue: "21474ff83647",
			},
			errors.New("invalid mystery value"),
			csvData{
				ipAddress:    "127.0.0.1",
				countryCode:  "AB",
				country:      "test",
				city:         "test",
				latitude:     "14.92021642445653",
				longitude:    "40.900399560492929",
				mysteryValue: "21474ff83647",
			},
		},
	}

	for _, t := range tests {
		suite.Run(t.desc, func() {
			err := t.csvData.sanitize()
			require.Equal(err, t.expectedError)
			require.Equal(t.csvData, t.expectedCSVData)
		})
	}
}

func TestCSV(t *testing.T) {
	suite.Run(t, new(CSVTestSuite))
}
