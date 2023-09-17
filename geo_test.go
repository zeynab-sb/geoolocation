package geoolocation

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/zeynab-sb/geoolocation/database"
	"log"
	"os"
	"testing"
)

type GeoTestSuite struct {
	suite.Suite
	db      *sql.DB
	sqlMock sqlmock.Sqlmock
	patch   *gomonkey.Patches
	geo     Geo
}

func (suite *GeoTestSuite) SetupSuite() {
	mockDB, sqlMock, err := sqlmock.New()
	if err != nil {
		log.Fatal("error in new connection", err)
	}

	suite.db = mockDB
	suite.sqlMock = sqlMock

	mockCtrl := gomock.NewController(suite.T())
	defer mockCtrl.Finish()

	suite.patch = gomonkey.NewPatches()
	suite.geo = Geo{
		db:         mockDB,
		driver:     &database.MySQLDriver{DB: mockDB},
		Repository: nil,
	}
}

func (suite *GeoTestSuite) TearDownSuit() {
	suite.patch.Reset()

	_ = suite.db.Close()
}

func (suite *GeoTestSuite) TearDownTest() {
	suite.patch.Reset()
}

func (suite *GeoTestSuite) TestGeo_ImportCSV_InvalidExtension_Failure() {
	require := suite.Require()
	expectedError := "invalid file extension"

	_, err := suite.geo.ImportCSV("data.txt", 1)
	require.EqualError(err, expectedError)
}

func (suite *GeoTestSuite) TestGeo_ImportCSV_SetupSanitizer_Failure() {
	require := suite.Require()
	expectedError := "error creating file"

	// setupSanitizer will return error while creating file
	suite.patch.ApplyFuncReturn(os.OpenFile, nil, errors.New("error creating file"))

	_, err := suite.geo.ImportCSV("data.csv", 1)
	require.EqualError(err, expectedError)
}

func (suite *GeoTestSuite) TestGeo_ImportCSV_read_Failure() {
	require := suite.Require()
	expectedError := "invalid csv header"

	// read will return error because of invalid headers
	err := createCSV([][]string{
		{"col1", "col2", "col3", "col4", "col5", "col6", "col7"},
		{"test", "test", "test", "test", "test", "test", "test"},
	}, "data9.csv")
	require.NoError(err)

	_, err = suite.geo.ImportCSV("data9.csv", 1)
	require.EqualError(err, expectedError)

	err = deleteCSV("data9.csv")
	require.NoError(err)

	err = deleteCSV("data9_sanitized.csv")
	require.NoError(err)
}

func (suite *GeoTestSuite) TestGeo_ImportCSV_load_Failure() {
	require := suite.Require()
	expectedError := "database error"

	err := createCSV([][]string{
		{"ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value"},
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"}},
		"data10.csv")
	require.NoError(err)

	// load will return error by database
	suite.sqlMock.ExpectExec("LOAD DATA LOCAL INFILE 'data10_sanitized.csv' INTO TABLE locations (.+)").
		WillReturnError(errors.New("database error"))

	_, err = suite.geo.ImportCSV("data10.csv", 1)
	require.EqualError(err, expectedError)

	err = deleteCSV("data10.csv")
	require.NoError(err)

	err = deleteCSV("data10_sanitized.csv")
	require.NoError(err)
}

func (suite *GeoTestSuite) TestGeo_ImportCSV_Success() {
	require := suite.Require()
	acceptedRows := int64(2)
	discardedRows := int64(1)

	err := createCSV([][]string{
		{"ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value"},
		{"127.0.0.1", "TA", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647"},
		{"127.0.0.2", "TB", "test", "test", "48.92021642545653", "14.900399560892929", "2147493647"},
		{"test", "test", "test", "test", "test", "test", "test"}},
		"data11.csv")
	require.NoError(err)

	suite.sqlMock.ExpectExec("LOAD DATA LOCAL INFILE 'data11_sanitized.csv' INTO TABLE locations (.+)").
		WillReturnResult(sqlmock.NewResult(2, 2))

	result, err := suite.geo.ImportCSV("data11.csv", 1)
	require.NoError(err)
	require.Equal(acceptedRows, result.acceptedRows)
	require.Equal(discardedRows, result.discardedRows)

	err = deleteCSV("data11.csv")
	require.NoError(err)
}

func TestGeo(t *testing.T) {
	suite.Run(t, new(GeoTestSuite))
}
