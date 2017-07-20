# ImputeDB [![Build Status](https://travis-ci.org/jfeser/ImputeDB.svg?branch=master)](https://travis-ci.org/jfeser/ImputeDB)

ImputeDB is a SQL database which automatically imputes missing data on-the-fly.
Users can issue SQL queries over data with NULL values and ImputeDB will use a regression model to fill in the missing values during the execution of the query.
Designed to enable exploratory analysis of survey data, ImputeDB removes the cost of performing imputation manually, allowing users to get a quick and accurate view of their data.

## Building and Running #

To build ImputeDB, run:

``` shell
cd simpledb; ant
```

To create a database from the demo collection of CSV files, run:

``` shell
./imputedb load --db demo.db demo_data/*
```

This creates three tables:

* `demo`: demographics data from the CDC
* `labs`: laboratory data from the CDC
* `exams`: physical examination data from the CDC

and places their serialized representations in the `demo.db` folder,
along with a catalog describing the table schemas..

Then, to query the database, run:

``` shell
./imputedb query --db demo.db
```

This launches the ImputeDB interpreter with an `--alpha 0.0` parameter
as a default, which
means ImputeDB will optimize for data quality. You can modify this
by calling the interpreter with the `--alpha <double>` option.

For example,

``` shell
./imputedb query --db demo.db --alpha 1.0
```

launches an interpreter that optimizes for query execution speed.

## Experiments #

1. Build the Docker container for the experiments.

``` shell
cd simpledb/test/experiments
make build
```

2. Run the experiments.

TODO.

## Publications #

**Query Optimization for Dynamic Imputation**. José Cambronero\*, John K. Feser\*, Micah J. Smith\*, Samuel Madden. VLDB. (2017) To appear. [<a href="http://people.csail.mit.edu/feser/imputedb.pdf">pdf</a>]

*Authors contributed equally to this paper.

## License #

[MIT](https://opensource.org/licenses/MIT)
