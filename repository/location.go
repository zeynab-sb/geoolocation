package repository

import (
	"database/sql"
	"time"
)

type LocationRepository interface {
	GetLocationByIP(ip string) (*Location, error)
}

// Location is a model in DB
type Location struct {
	ID           uint      `db:"id"`
	IPAddress    string    `db:"ip_address"`
	CountryCode  string    `db:"country_code"`
	Country      string    `db:"country"`
	City         string    `db:"city"`
	Lat          float64   `db:"latitude"`
	Lng          float64   `db:"longitude"`
	MysteryValue int       `db:"mystery_value"`
	UpdatedAt    time.Time `db:"updated_at"`
	CreatedAt    time.Time `db:"created_at"`
}

type locationRepository struct {
	db *sql.DB
}

func NewLocationRepository(db *sql.DB) LocationRepository {
	repo := new(locationRepository)
	repo.db = db

	return repo
}

// GetLocationByIP retrieve location info by ip.
func (r *locationRepository) GetLocationByIP(ip string) (*Location, error) {
	var location Location
	err := r.db.QueryRow("SELECT * FROM locations WHERE ip_address = ?", ip).Scan(&location.ID,
		&location.IPAddress, &location.CountryCode, &location.Country, &location.City, &location.Lat,
		&location.Lng, &location.MysteryValue, &location.CreatedAt, &location.UpdatedAt)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return &location, nil
}
