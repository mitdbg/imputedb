.DEFAULT_GOAL := help

SIZE?=small

help:
	@echo "usage:"
	@echo "  make help: display this help"
	@echo "  make clean: remove output directories and images"
	@echo "  make clean_force: also *force*-kill containers"
	@echo "  make run SIZE=size: run experiments"
	@echo "  make analysis SIZE=size: compute results"
	@echo "  make run_and_analysis SIZE=size: take a wild guess"
	@echo "  make explore: explore the output data in a python container"

clean: clean_files clean_containers

clean_force: clean_files clean_containers_force

clean_files:
	sudo rm -rf ./output
	sudo rm -rf ./query_plans
	sudo rm -rf ./__pycache__

clean_containers:
	@docker rm $(shell docker ps -a -q -f ancestor=imputedb) 2>/dev/null \
		&& echo "Removed imputedb containers." \
		|| echo "No containers found."

clean_containers_force:
	@docker rm -f $(shell docker ps -a -q -f ancestor=imputedb) 2>/dev/null \
		&& echo "Removed imputedb containers." \
		|| echo "No containers found."

build:
	{ cd ../..; docker build -t imputedb .; }

run_and_analysis: run analysis

run:
	docker run -dit \
		   --name=imputedb-run-$(SIZE) \
		   -v "$(shell pwd):/src/test/experiments" \
		   --user root \
		   imputedb \
		   python run_experiments.py $(SIZE)
	docker logs --follow imputedb-run-$(SIZE) | sudo tee output/$(SIZE)/log


analysis:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/data" \
		   -v "$(shell pwd):/home/jovyan/work" \
		   --user root \
	           jupyter/datascience-notebook \
		   python analyze_experiments.py /data/$(SIZE)

explore:
	docker run -it --rm \
		   -v "$(shell pwd)/output:/data" \
		   -v "$(shell pwd):/home/jovyan/work" \
		   --user root \
	           jupyter/datascience-notebook python

.PHONY: help clean clean_force clean_files clean_containers clean_containers_force build run_and_analysis run analysis explore
