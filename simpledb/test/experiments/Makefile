.DEFAULT_GOAL := help

help:
	@echo "usage:"
	@echo "  make help: display this help"
	@echo "  make build: build image 'simpledb'"
	@echo "  make run: run experiments"

build:
	{ cd ../..; docker build -t simpledb .; }

run:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/src/test/experiments/output" \
		   -v "$(shell pwd)/query_plans:/src/test/experiments/query_plans" \
		   simpledb

.PHONY: help build run
