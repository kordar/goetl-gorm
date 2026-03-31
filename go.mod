module github.com/kordar/goetl-gorm

go 1.22

replace github.com/kordar/goetl => ../goetl

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/kordar/goetl v0.0.2
	gorm.io/gorm v1.25.12
)

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/spf13/cast v1.10.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)
