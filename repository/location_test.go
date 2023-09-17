package repository

import (
	"database/sql"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"log"
	"testing"
	"time"
)

type LocationTestSuite struct {
	suite.Suite
	db      *sql.DB
	sqlMock sqlmock.Sqlmock
	patch   *gomonkey.Patches
	repo    locationRepository
}

func (suite *LocationTestSuite) SetupSuite() {
	mockDB, sqlMock, err := sqlmock.New()
	if err != nil {
		log.Fatal("error in new connection", err)
	}

	suite.db = mockDB
	suite.sqlMock = sqlMock

	mockCtrl := gomock.NewController(suite.T())
	defer mockCtrl.Finish()

	suite.patch = gomonkey.NewPatches()
	suite.repo = locationRepository{db: mockDB}
}

func (suite *LocationTestSuite) TearDownSuit() {
	suite.patch.Reset()

	_ = suite.db.Close()
}

func (suite *LocationTestSuite) TearDownTest() {
	suite.patch.Reset()
}

func (suite *LocationTestSuite) TestLocation_GetLocationByIP_Failure() {
	require := suite.Require()
	expectedErr := "database error"

	suite.sqlMock.ExpectQuery("^SELECT (.+) FROM locations WHERE ip_address = (.+)").
		WithArgs("127.0.0.1").
		WillReturnError(errors.New("database error"))

	_, err := suite.repo.GetLocationByIP("127.0.0.1")
	require.EqualError(err, expectedErr)
}

func (suite *LocationTestSuite) TestLocation_GetLocationByIP_Success() {
	require := suite.Require()
	suite.patch.ApplyFunc(time.Now, func() time.Time {
		return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	})
	
	expectedLoc := &Location{
		ID:           1,
		IPAddress:    "127.0.0.1",
		CountryCode:  "AB",
		Country:      "test",
		City:         "test",
		Lat:          48.92021642445653,
		Lng:          14.900399560492929,
		MysteryValue: 2147483647,
		UpdatedAt:    time.Now(),
		CreatedAt:    time.Now(),
	}

	rows := sqlmock.NewRows([]string{"id", "ip_address", "country_code", "country", "city", "latitude", "longitude", "mystery_value", "created_at", "updated_at"}).
		AddRow(1, "127.0.0.1", "AB", "test", "test", "48.92021642445653", "14.900399560492929", "2147483647", time.Now(), time.Now())
	suite.sqlMock.ExpectQuery("^SELECT (.+) FROM locations WHERE ip_address = (.+)").
		WithArgs("127.0.0.1").
		WillReturnRows(rows)

	res, err := suite.repo.GetLocationByIP("127.0.0.1")
	require.NoError(err)
	require.Equal(expectedLoc, res)
}

func TestLocation(t *testing.T) {
	suite.Run(t, new(LocationTestSuite))
}
