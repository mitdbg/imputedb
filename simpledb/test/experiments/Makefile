.DEFAULT_GOAL := help

EXPERIMENT_SIZE = medium

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
	sudo rm -rf ./__pycache__

build:
	{ cd ../..; docker build -t imputedb .; }

run:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/src/test/experiments/output" \
		   -v "$(shell pwd)/query_plans:/src/test/experiments/query_plans" \
		   imputedb \
		   python run_experiments.py medium
	docker run -it --rm \
		   -v "$(shell pwd)/output:/data" \
		   -v "$(shell pwd):/src" \
		   --user root \
	           jupyter/datascience-notebook \
		   python /src/analyze_experiments.py /data/$(EXPERIMENT_SIZE)

explore:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/data" \
		   -v "$(shell pwd):/src" \
		   --user root \
	           jupyter/datascience-notebook

.PHONY: help build run
