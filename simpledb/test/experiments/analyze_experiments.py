#!/usr/bin/env python

from __future__ import print_function

import pandas as pd
import numpy as np

import os
import glob
import sys

from collections import defaultdict

try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

# configure this
nqueries = 10
table_headers = [
    ['income', 'cuff_size'],                # query 0
    ['income', 'creatine'],                 # query 1
    ['blood_lead'],                         # query 2
    ['gender', 'blood_pressure_systolic'],  # query 3
    ['attendedbootcamp', 'income'],         # query 4
    ['commutetime'],                        # query 5
    ['schooldegree', 'studentdebtowe'],     # query 6
    ['attendedbootcamp', 'gdp_per_capita'], # query 7
    ['waist_circumference'],                # query 8
    ['income','cuff_size'],                 # query 9
    ['attendedbootcamp','income'],          # query 10
    ['commutetime'],                        # query 11
    ['gender','age'],                       # query 12
]

# basic utils
def drop_warmup(df, by, drop=20):
    gb = df.groupby(by)
    n  = max(gb.size())
    # have the same number in each cat, so this is okay
    return gb.tail(n-drop)

def summarize(df, by, a):
    ops = {'mean': np.mean, 'std': np.std}
    return df.groupby(by)[a].agg(ops).reset_index()
  
def get_query_results(experiments_dir, headers, restrict_alpha=False, base=False):
    # results has
    # - key: (query, alpha)
    # - value: df with results, col names specific to that query
    results = defaultdict(lambda: [])

    # Impute on base stored in base/ subdirectory, buth with same subdirectory
    # structure.
    if base:
        experiments_dir = os.path.join(experiments_dir, "base")

    # Iterate through queries, alpha, iters
    print("get_query_results")
    if restrict_alpha:
        globs = glob.glob(experiments_dir + os.path.sep + "q*/alpha000/it*/result.txt") + \
             glob.glob(experiments_dir + os.path.sep + "q*/alpha1000/it*/result.txt")
    else:
        globs = glob.glob(experiments_dir + os.path.sep + "q*/alpha*/it*/result.txt")

    for f in globs:
        print("Reading {}...".format(f))
        qi = f.rfind("/q")+len("/q")
        q = int(f[qi:qi+2])
        ai = f.rfind("/alpha") + len("/alpha")
        aj = f.find("/", ai)
        alpha = int(f[ai:aj])/1000.0
        df = pd.read_csv(f, header=None, names=headers[q])
        if len(df.columns) > 1:
            df = df.set_index(df.columns[0])
        results[(q, alpha)].append(df)
    
    return results
  
def get_rmse(refs, results):
    acc = []
    for res in results:
        for ref in refs:
            sqr_errors = ((res - ref) ** 2).values.flatten()
            acc.append(sqr_errors)
    acc = np.array(acc)
    flat_acc = acc.flatten()
    # the average number of missing entries
    missing = np.isnan(acc).sum(axis = 1).mean()
    return np.sqrt(np.nanmean(flat_acc)), missing
  
# symmetric mean absolute percentage error  
def get_smape(refs, results):
    metrics = []
    for res in results:
        for ref in refs:
            deviations = (np.abs(res - ref) / (res + ref)).values.flatten()
            metrics.append(np.nanmean(deviations))
    metrics = np.array(metrics)
    return np.nanmean(metrics), np.nanstd(metrics)

def get_timing_results(experiments_dir, restrict_alpha=False, base=False):
    df = pd.DataFrame(columns=["query","alpha","iter","plan_time","run_time","plan_hash"])

    # Impute on base stored in base/ subdirectory, buth with same subdirectory
    # structure.
    if base:
        experiments_dir = os.path.join(experiments_dir, "base")

    if restrict_alpha:
        globs = glob.glob(experiments_dir + os.path.sep + "q*/alpha000/timing.csv") + \
             glob.glob(experiments_dir + os.path.sep + "q*/alpha1000/timing.csv")
    else:
        globs = glob.glob(experiments_dir + os.path.sep + "q*/alpha*/timing.csv")

    # Iterate through queries, alpha, reading 'timing.csv' for each.
    for f in globs:
        print('Processing {}...'.format(f))
        df0 = pd.read_csv(f)
        df = df.append(df0)

    return df

def explore(experiments_dir, base=False):
    # time data
    data = get_timing_results(experiments_dir, base=base)

    if not base:
        data['is_experiment'] = True
    else:
        data['is_experiment'] = False
        data['alpha'] = 'Impute at base tables'

    data = drop_warmup(data, ['query', 'alpha'], drop=20)

    # time measures
    by = ['query', 'alpha']
    if base:
        name = 'base_tables'
    else:
        name = 'imputedb'
    planning_times = summarize(data, by, 'plan_time')
    running_times = summarize(data, by, 'run_time')

    return planning_times, running_times, data

def collapse_results(experiments_dir, output_dir, base=False):
    if not os.path.isdir(experiments_dir):
        raise FileNotFoundError

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # time data
    data = get_timing_results(experiments_dir, base=base)

    if not base:
        data['is_experiment'] = True
    else:
        data['is_experiment'] = False
        data['alpha'] = 'Impute at base tables'

    data = drop_warmup(data, ['query', 'alpha'], drop=20)

    # time measures
    by = ['query', 'alpha']
    if base:
        name = 'base_tables'
    else:
        name = 'imputedb'

    planning_times = summarize(data, by, 'plan_time')
    running_times = summarize(data, by, 'run_time')

    # save to CSV
    planning_times.to_csv(os.path.join(output_dir,
        'planning_times_{}.csv'.format(name)))
    running_times.to_csv(os.path.join(output_dir,
        'running_times_{}.csv'.format(name)))

def write_perf_summary(experiments_dir, output_dir):
    if not os.path.isdir(experiments_dir):
        raise FileNotFoundError

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    experiment_results = get_query_results(experiments_dir, table_headers)
    base_results = get_query_results(experiments_dir, table_headers, base=True)
    perf = []
    try:
        experiment_results_items = experiment_results.iteritems()
        base_results_items       = base_results.iteritems()
    except AttributeError:
        experiment_results_items = experiment_results.items()
        base_results_items       = base_results.items()
    for (query, alpha), res in experiment_results_items:
        print("Processing query {}, alpha {}...".format(query, alpha))
        refs = [df for (q, a), dfs in base_results_items for df in dfs if q == query]
        smape, std = get_smape(refs, res)
        perf.append((query, alpha, smape, std))

    perf_summary = pd.DataFrame(perf, columns = ['query', 'alpha', 'smape', 'std'])
    perf_summary = perf_summary.sort_values(['query', 'alpha'])
    perf_summary_latex = perf_summary.copy()[['query', 'alpha', 'smape']]
    perf_summary_latex['smape'] *= 100.0
    perf_summary_latex.columns = ['Query', r'\alpha', 'SMAPE']
    perf_summary_latex.to_latex(os.path.join(output_dir, 'perf_summary.tex'),
            float_format='%.2f', index=False)

    return perf

def write_counts_summary(experiments_dir, output_dir):
    if not os.path.isdir(experiments_dir):
        raise FileNotFoundError

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    experiment_results = get_query_results(experiments_dir, table_headers)
    base_results = get_query_results(experiments_dir, table_headers, base=True)

    def compute_average_count_items(d):
        try:
            myitems = d.iteritems()
        except AttributeError:
            myitems = d.items()

        df = pd.DataFrame(columns=["query","alpha","mean","std"])
        for (query, alpha), res in myitems:
            sums = []
            for df1 in res:
                s = df1.sum()
                if len(s) != 1:
                    print(len(s))
                    print(query, alpha)
                    raise Exception
                sums.append(s.values[0])
            avg = np.mean(sums)
            std = np.std(sums)
            df = df.append(pd.DataFrame(
                data={"query": [query],
                      "alpha": [alpha],
                      "mean": [avg],
                      "std": [std],
                }))

        return df

    df1 = compute_average_count_items(experiment_results)
    df2 = compute_average_count_items(base_results)

    df2["alpha"] = -1.0 # impute at base table

    # Write to file
    df = df1.append(df2).sort_values(["query"])

    print("Writing means...")
    dfmean = df.pivot(index="query",columns="alpha",values="mean") 
    dfmean.to_latex(os.path.join(output_dir, 'counts_mean.tex'),
            float_format='%.2f')

    print("Writing stds...")
    dfstd = df.pivot(index="query",columns="alpha",values="std")
    dfstd.to_latex(os.path.join(output_dir, 'counts_std.tex'),
            float_format='%.2f')

    dfmean["0.0 pct"] = dfmean[0.0]/dfmean[-1.0]
    dfmean["1.0 pct"] = dfmean[1.0]/dfmean[-1.0]

    print("Writing means (pct)...")
    dfmean[["0.0 pct","1.0 pct"]].to_latex(
        os.path.join(output_dir, 'counts_mean_pct.tex'), float_format='%.2f'
    )

    return df

def main(experiments_dir, output_dir):
    collapse_results(experiments_dir, output_dir)
    collapse_results(experiments_dir, output_dir, base=True)
    write_perf_summary(experiments_dir, output_dir)

if __name__ == "__main__":
    def print_usage_and_exit():
        print("usage: python analyze_experiments.py <experiment-output-dir>")
        sys.exit(1)

    if len(sys.argv) == 2:
        experiment_dir = sys.argv[1]
        main(experiment_dir, os.path.join(experiment_dir, "analysis"))
    else:
        print_usage_and_exit()
