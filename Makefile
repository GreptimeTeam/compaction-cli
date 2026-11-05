.PHONY: build test check lint fmt dist image

build:
	go build -trimpath -o greptime-compaction-cli .

test:
	go test -race ./...

check:
	test -z "$$(gofmt -l *.go)"
	go vet ./...
	go test -race ./...
	python3 -m unittest discover -s scripts -p '*_test.py'
	python3 -m py_compile e2e.py large_e2e.py analyze_run.py scripts/build.py

# Install the CI-pinned golangci-lint version on PATH, or set GOLANGCI_LINT.
GOLANGCI_LINT ?= golangci-lint
lint:
	$(GOLANGCI_LINT) run ./...

fmt:
	gofmt -w *.go

dist:
	python3 scripts/build.py

image:
	docker build -t compaction-cli:local .
