GO=go
BINARY=rafty
# go source files, ignore vendor directory
SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

info:
	@echo "---------------------------------------"
	@echo " build   generate a build              "
	@echo " fmt     format using go fmt           "
	@echo " run     build & run beanstalkd        "
	@echo " test    run unit-tests                "
	@echo " testv   run unit-tests verbose        "
	@echo "---------------------------------------" 

build: clean fmt
	$(GO) build -o bin/$(BINARY) -v main.go

run: build
	bin/$(BINARY) standalone 3

test: clean fmt
	$(GO) test ./...

testv: clean fmt
	$(GO) test -v ./...

fmt:
	$(GO) fmt ./...

clean:
	rm -f bin/$(BINARY)
