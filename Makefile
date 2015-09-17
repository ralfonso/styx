export GO15VENDOREXPERIMENT=1
all: build

deps:
	go get github.com/Masterminds/glide
	glide install

build: deps
	go install

test:
	go test -race

run: build
	styx
