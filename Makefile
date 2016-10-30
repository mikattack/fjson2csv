PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

.PHONY: all test build install clean

all: build

build: fjson2csv

fjson2csv: *.go cmd/fjson2csv/*.go
	go build ./cmd/fjson2csv

fjson2csv-data: *.go cmd/fjson2csv-data/*.go
	go build ./cmd/fjson2csv-data

test:
	go test $(shell go list ./... | grep -v "/vendor/")

install: all
	install -d $(BINDIR)
	install fjson2csv $(BINDIR)

clean:
	rm -f json2csv
