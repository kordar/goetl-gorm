module github.com/kordar/goetl-gorm

go 1.22

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/kordar/goetl v0.0.1
	gorm.io/gorm v1.25.12
)

replace github.com/kordar/goetl => ../goetl

require (
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/kordar/gologger v0.0.8
	golang.org/x/text v0.14.0 // indirect
)
