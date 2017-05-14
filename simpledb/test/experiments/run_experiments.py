#!/usr/bin/env python

from __future__ import print_function
from generate_big_joins import create_join_workload
import os
import subprocess
import tempfile

executable_default    = ["java","-Xmx3200m","-jar","../../dist/simpledb.jar"]
executable_longimpute = ["java","-Xmx3200m","-Dsimpledb.ImputeSlow","-jar","../../dist/simpledb.jar"]
output_dir            = os.path.join(os.path.dirname(os.path.realpath(__file__)), "output")
catalog_default       = "../../catalog.txt"
queries_default       = "queries.txt"

def run_large05_experiment():

    iters     = 220
    min_alpha = 0.00
    max_alpha = 1.00
    step      = 0.499999

    queries = queries_default

    this_output_dir = os.path.join(output_dir, "regression_tree")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "REGRESSION_TREE")

    this_output_dir = os.path.join(output_dir, "mean")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "MEAN")

    this_output_dir = os.path.join(output_dir, "hot_deck")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "HOTDECK")

def run_large05_experiment_count():

    iters     = 220
    min_alpha = 0.00
    max_alpha = 1.00
    step      = 0.499999

    queries = "queries_count.txt"

    this_output_dir = os.path.join(output_dir, "regression_tree")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "REGRESSION_TREE")

    this_output_dir = os.path.join(output_dir, "mean")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "MEAN")

    this_output_dir = os.path.join(output_dir, "hot_deck")
    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries =
            queries, executable = executable_longimpute, imputationMethod =
            "HOTDECK")

def run_acs_experiment():
    catalog = catalog_default
    executable = executable_default

    this_output_dir = os.path.join(output_dir, "acs")

    # Impute on base table
    (f, acs_query) = tempfile.mkstemp()
    os.write(f, "SELECT AVG(c0) FROM acs_dirty;\n")

    iters = 1

    print("Running acs base...")
    executable_max_heap = [executable[0]] + ["-Xmx3200m", "-Dsimpledb.ImputeSlow"] + executable[1:]
    cmd = executable_max_heap + \
        ["experiment", catalog, acs_query, this_output_dir, str(iters), "--base"]
    print(cmd)
    subprocess.call(cmd)
    print("Running acs base...done.")

    # Impute using ImputeDB
    iters     = 220
    min_alpha = 0.00
    max_alpha = 1.00
    step      = 1.00

    subprocess.call(executable +
        ["experiment", catalog, acs_query, this_output_dir,
         str(iters), str(min_alpha), str(max_alpha), str(step)])

    os.close(f)

def run_join_experiments():
    join_output_dir = os.path.join(output_dir, "joins")

    iters     = 20
    min_alpha = 0.00
    max_alpha = 1.00
    step      = 1.00
    # parameters specific to join workload
    # number of queries to generate and evaluate per size of join
    nqueries   = 5
    # minimum number of joins
    min_njoins = 2
    # maximum number of joins
    max_njoins = 8

    # evaluate each size of join separately
    for njoins in range(min_njoins, max_njoins + 1):
      print("Running join experiments. N-joins %d" % njoins)
      # create sub directory for each size of joins
      this_output_dir = os.path.join(join_output_dir, str(njoins))
      # create workload, written out to base directory
      workload = create_join_workload(njoins, nqueries)
      # local file with queries
      queries = "joins_n%d_queries.txt" % njoins
      with open(queries, 'w') as f:
        f.write(workload)

      # execute this size of n joins
      run_experiment(this_output_dir, iters, min_alpha, max_alpha, step, queries = queries, plan_only = True)


def run_experiment(this_output_dir, iters, min_alpha, max_alpha, step,
        queries=None, executable=None, plan_only=False, imputationMethod=""):
    if not os.path.isdir(this_output_dir):
        os.makedirs(this_output_dir)

    if not queries:
        queries = queries_default

    if not executable:
        executable = executable_default


    catalog = catalog_default

    if imputationMethod:
        imputationMethodOpt = ["--imputationMethod={}".format(imputationMethod)]
    else:
        imputationMethodOpt = []

    planOnlyOpt = ["--planOnly={}".format(plan_only)]

    # Timing using ImputeDB
    subprocess.call(executable +
        ["experiment", catalog, queries, this_output_dir,
         str(iters), str(min_alpha), str(max_alpha), str(step)] + planOnlyOpt +
        imputationMethodOpt)

    # Timing using impute on base table
    if not plan_only:
      subprocess.call(executable + ["experiment", catalog, queries,
          this_output_dir, str(iters), "--base"] +
          imputationMethodOpt)

if __name__ == "__main__":
    import fire
    fire.Fire({
        "large05" : run_large05_experiment,
        "large05_count" : run_large05_experiment_count,
        "acs" : run_acs_experiment,
        "joins" : run_join_experiments,
    })
    print("Done.")
