.PHONY: install test build build_http build_file

install:
	@go mod download

test:
	@go test -v -race ./...

build: build_file build_http


build_file:
	@go build -o ./targets/msk_sd_file github.com/statsbomb/prometheus-msk-discovery/cmd/msk_sd_file

build_http:
	@go build -o ./targets/msk_sd_http github.com/statsbomb/prometheus-msk-discovery/cmd/msk_sd_http
