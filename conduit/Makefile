.PHONY: build
build:
	go build -o custom-conduit main.go

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -tI % go install %
	@go mod tidy

.PHONY: fmt
fmt:
	gofumpt -l -w .
	gci write --skip-generated  .

.PHONY: lint
lint:
	golangci-lint run ./...
