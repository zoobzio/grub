.PHONY: test lint lint-fix coverage clean check ci help test-unit test-integration test-bench install-hooks install-tools

## Testing
test:              ## Run all tests
	go test -race ./...

test-unit:         ## Run unit tests only
	go test -race -short ./...

test-integration:  ## Run integration tests
	cd testing && go test -race ./integration/...

test-bench:        ## Run benchmarks
	cd testing && go test -bench=. -benchmem ./benchmarks/...

## Linting
lint:              ## Run linter
	golangci-lint run

lint-fix:          ## Run linter with auto-fix
	golangci-lint run --fix

## Coverage
coverage:          ## Generate coverage report
	go test -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html

## Tooling
install-hooks:     ## Install git hooks
	git config core.hooksPath .githooks
	chmod +x .githooks/*
	@echo "Git hooks installed"

install-tools:     ## Install development tools
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

## Maintenance
clean:             ## Remove generated files
	rm -f coverage.out coverage.html

## Workflow
check:             ## Quick validation (test + lint)
	$(MAKE) test
	$(MAKE) lint

ci:                ## Full CI simulation
	$(MAKE) clean
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) coverage

## Help
help:              ## Display this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
