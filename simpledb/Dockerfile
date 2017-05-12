FROM openjdk:8

RUN apt-get update -y
RUN apt-get install -y ant
RUN apt-get install -y python-dev python-pip

RUN pip install --upgrade pip
RUN /usr/local/bin/pip install fire

ADD . /src
WORKDIR /src
RUN ant dist
WORKDIR /src/test/experiments

CMD ["python", "run_experiments.py"]
