.DEFAULT_GOAL := help

EXPERIMENT_SIZE = large

help:
	@echo "usage:"
	@echo "  make help: display this help"
	@echo "  make clean: remove output directories and images"
	@echo "  make build: build image 'imputedb'"
	@echo "  make run: run experiments"
	@echo "  make explore: explore the output data in a python container"

clean:
	@docker rm $(shell docker ps -a -q -f ancestor=imputedb) 2>/dev/null \
		&& echo "Removed imputedb containers." \
		|| echo "No containers found."
	sudo rm -rf ./output
	sudo rm -rf ./query_plans
	sudo rm -rf ./__pycache__

build:
	{ cd ../..; docker build -t imputedb .; }

run_and_analysis: run analysis
run:
	docker run -dit --rm \
		   -v "$(shell pwd):/src/test/experiments" \
		   --name="imputedb-run" \
		   imputedb \
		   python run_experiments.py $(EXPERIMENT_SIZE)
	docker logs --follow imputedb-run | tee output/$(EXPERIMENT_SIZE)/log

analysis:
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
	           jupyter/datascience-notebook python

.PHONY: help build run explore