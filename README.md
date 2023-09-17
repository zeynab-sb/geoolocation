
# geolocation

This library providing interface to import csv file and repository to access to model layer.

## Optimizations

There are several ways to read a CSV file, sanitize it, and put it into the database. One way is to read data row by row from CSV and add each row to the database. This way is simple, but in a big CSV file, it puts a lot of load on our database, which could be more efficient. Another standard method is to read all the data in one place and then sanitize each and bulk insert data in the database. This way, it controls the load to the database, but it needs lots of RAM to be more efficient.

In this library, the data is being read row by row from CSV, and we sanitize each row and write it in a new CSV file at the end; with the help of the load and copy command that some databases provide us, we import the sanitized file to the database. In this way, if we have a situation to run parallel, we can do sanitization parallel and then write to the CSV file async and, after that, load data to the database.


## Installation

```
 go get github.com/zeynab-sb/geoolocation
```

## Examples

Accepted CSV header and data

``` csv
ip_address,country_code,country,city,latitude,longitude,mystery_value
200.106.141.15,SI,Nepal,DuBuquemouth,-84.87503094689836,7.206435933364332,7823011346
```

We import CSV data in the model below. If you already have the locations schema in your database, it is excellent, and if you haven't, you can easily make schema by this library.

```golang
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
```
If you want to prevent duplicate row this constraint should be added. It will be added in CreateSchema.

```sql
    CONSTRAINT uc_location UNIQUE (ip_address,country_code,country,city,latitude,longitude,mystery_value)

```

Building Schema

``` golang
package main

import (
	"fmt"
	"github.com/zeynab-sb/geoolocation"
)

func main() {
    // the config of the database should be send
	d := &database.DBConfig{
		Driver:      "mysql",
		Host:        "127.0.0.1",
		Port:        3306,
		DB:          "database",
		User:        "user",
		Password:    "password",
		Location:    nil,
		MaxConn:     0,
		IdleConn:    0,
		Timeout:     0,
		DialRetry:   0,
		DialTimeout: 0,
	}

	geo, err := geoolocation.New(d)
	if err != nil {
		fmt.Println(err)
	}

	err := geo.CreateSchema()
	if err != nil {
		fmt.Println(err)
	}
}

```

ImportCSV

``` golang
package main

import (
	"fmt"
	"github.com/zeynab-sb/geoolocation"
)

func main() {
    // the config of the database should be send
	d := &database.DBConfig{
		Driver:      "mysql",
		Host:        "127.0.0.1",
		Port:        3306,
		DB:          "database",
		User:        "user",
		Password:    "password",
		Location:    nil,
		MaxConn:     0,
		IdleConn:    0,
		Timeout:     0,
		DialRetry:   0,
		DialTimeout: 0,
	}

	geo, err := geoolocation.New(d)
	if err != nil {
		fmt.Println(err)
	}


	// In the ImportCSV function, you should send the path and the number of 
	// concurrent  processes. If you have just one hardware thread, don't send 
	// this param more than one because the process that is in the concurrent part 
	// is CPU bound, and it just increases the result time.

	result, err := geo.ImportCSV("data.csv", runtime.NumCPU())
	if err != nil {
		fmt.Println(err)
	}
}

```

Using Repository

``` golang
package main

import (
	"fmt"
	"github.com/zeynab-sb/geoolocation"
)

func main() {
    // the config of the database should be send
	d := &database.DBConfig{
		Driver:      "mysql",
		Host:        "127.0.0.1",
		Port:        3306,
		DB:          "database",
		User:        "user",
		Password:    "password",
		Location:    nil,
		MaxConn:     0,
		IdleConn:    0,
		Timeout:     0,
		DialRetry:   0,
		DialTimeout: 0,
	}

	geo, err := geoolocation.New(d)
	if err != nil {
		fmt.Println(err)
	}

    loc, err := geo.Repository.GetLocationByIP("127.0.0.1")
	result, err := geo.ImportCSV("data.csv", runtime.NumCPU())
	if err != nil {
		fmt.Println(err)
	}
}

```


## Running Tests

To run tests, run the following command

```bash
  go test -v ./...
```

