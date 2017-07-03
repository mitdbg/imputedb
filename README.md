# ImputeDB [![Build Status](https://travis-ci.org/jfeser/ImputeDB.svg?branch=master)](https://travis-ci.org/jfeser/ImputeDB)

ImputeDB is a SQL database which automatically imputes missing data on-the-fly.
Users can issue SQL queries over data with NULL values and ImputeDB will use a regression model to fill in the missing values during the execution of the query.
Designed to enable exploratory analysis of survey data, ImputeDB removes the cost of performing imputation manually, allowing users to get a quick and accurate view of their data.

# Building and Running #

To build ImputeDB, run:

``` shell
cd simpledb; ant
```

To create a database from a collection of CSV files, run:

``` shell
./imputedb load --db demo.db data/demographic.csv data/labs.csv data/examination.csv
```

Then, to query the database, run:

``` shell
./imputedb query --db demo.db
```

# Experiments #

1. Build the Docker container for the experiments.

``` shell
cd simpledb/test/experiments
make build
```

2. Run the experiments.

TODO.

# Publications #

**Query Optimization for Dynamic Imputation**. José Cambronero\*, John K. Feser\*, Micah J. Smith\*, Samuel Madden. VLDB. (2017) To appear. [<a href="http://people.csail.mit.edu/feser/imputedb.pdf">pdf</a>]

*Authors contributed equally to this paper.

# License #

[MIT](https://opensource.org/licenses/MIT)
