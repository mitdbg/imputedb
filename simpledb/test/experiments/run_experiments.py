#!/usr/bin/env python

import os
import subprocess

executable = ["java","-jar","../../dist/simpledb.jar"]
output_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "output")
catalog   = "../../catalog.txt"
queries   = "queries.txt"

def run_small_experiment():
    this_output_dir = os.path.join(output_dir, "small")

    iters     = 3
    min_alpha = 0.01
    max_alpha = 0.99
    step      = 0.32

    run_experiment(this_output_dir, iters, min_alpha, max_alpha, step)

def run_experiment(this_output_dir, iters, min_alpha, max_alpha, step):
    if not os.path.isdir(this_output_dir):
        os.makedirs(this_output_dir)

    subprocess.call(executable +
        ["experiment", catalog, queries, this_output_dir,
         str(iters), str(min_alpha), str(max_alpha), str(step)])

if __name__ == "__main__":
    run_small_experiment()
