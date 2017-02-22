.DEFAULT_GOAL := help

help:
	@echo "usage:"
	@echo "  make help: display this help"
	@echo "  make clean: remove output directories and images"
	@echo "  make build: build image 'imputedb'"
	@echo "  make run: run experiments"

clean:
	@docker rm $(shell docker ps -a -q -f ancestor=imputedb) 2>/dev/null \
		&& echo "Removed imputedb containers." \
		|| echo "No containers found."
	sudo rm -rf ./output
	sudo rm -rf ./query_plans

build:
	{ cd ../..; docker build -t imputedb .; }

run:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/src/test/experiments/output" \
		   -v "$(shell pwd)/query_plans:/src/test/experiments/query_plans" \
		   imputedb 

.PHONY: help build run
