FROM openjdk:8

RUN apt-get update
RUN apt-get install -y ant
ADD . /src
WORKDIR /src
RUN ant dist
WORKDIR /src/test/experiments
CMD ["python", "run_experiments.py"]
