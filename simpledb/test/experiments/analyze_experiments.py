#!/usr/bin/env python

from __future__ import print_function

import pandas as pd
import numpy as np

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
plt.ioff()

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
    ['income', 'weight'],                     # query 0
    ['income', 'cholesterol'],                # query 1
    ['blood_lead'],                           # query 2
    ['gender', 'blood_pressure_systolic'],    # query 3
    ['years_edu', 'head_circumference'],      # query 4
    ['attendedbootcamp', 'income'],           # query 5
    ['age'],                                  # query 6
    ['schooldegree', 'moneyforlearning'],     # query 7
    ['attendedbootcamp', 'gdp_per_capita'],   # query 8
    ['bootcamppostsalary', 'gdp_per_capita'], # query 9
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
  
def plot(df,**kwargs):
    _, ax = plt.subplots(1)
    for a in set(df['alpha']):
        data = df[df['alpha'] == a]
        try:
            data.plot(ax=ax, label=r'$\alpha=%.0f$' % a, **kwargs)
        except TypeError:
            data.plot(ax=ax, label=a, **kwargs)
        # get around bug in legend drawing in pandas
        plt.legend()
    return ax
  
def get_query_results(experiments_dir, headers, base=False):
    # results has
    # - key: (query, alpha)
    # - value: df with results, col names specific to that query
    results = defaultdict(lambda: [])

    # Impute on base stored in base/ subdirectory, buth with same subdirectory
    # structure.
    if base:
        experiments_dir = os.path.join(experiments_dir, "base")

    # Iterate through queries, alpha, iters
    for f in glob.glob(experiments_dir + os.path.sep + "q*/alpha*/it*/result.txt"):
        qi = f.rfind("/q")+len("/q")
        q = int(f[qi:qi+2])
        ai = f.rfind("/alpha") + len("/alpha")
        alpha = int(f[ai:ai+3])/1000.0
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

def get_timing_results(experiments_dir, base=False):
    df = pd.DataFrame(columns=["query","alpha","iter","plan_time","run_time","plan_hash"])

    # Impute on base stored in base/ subdirectory, buth with same subdirectory
    # structure.
    if base:
        experiments_dir = os.path.join(experiments_dir, "base")

    # Iterate through queries, alpha, reading 'timing.csv' for each.
    for f in glob.glob(experiments_dir + os.path.sep + "q*/alpha*/timing.csv"):
        # alpha = int(alpha_dir[len("alpha"):])/1000.0
        print('Processing {}...'.format(f))
        df0 = pd.read_csv(f)
        df = df.append(df0)

    return df

def explore(experiments_dir):
    if not os.path.isdir(experiments_dir):
        raise FileNotFoundError

    # experiments = get_timing_results(experiments_dir)
    base_tables = get_timing_results(experiments_dir, base=True)
    # experiments['is_experiment'] = True
    base_tables['is_experiment'] = False
    base_tables['alpha'] = 'Impute at base tables'

    # experiments = drop_warmup(experiments, ['query', 'alpha'], drop=20)
    base_tables = drop_warmup(base_tables, ['query', 'alpha'], drop=20)

    # time measures
    by = ['query', 'alpha']
    planning_times = {}
    # planning_times['imputedb'] = summarize(experiments, by, 'plan_time')
    planning_times['base_tables'] = summarize(base_tables, by, 'plan_time')

    running_times = {}
    # running_times['imputedb'] = summarize(experiments, by, 'run_time')
    running_times['base_tables'] = summarize(base_tables, by, 'run_time')

    print(planning_times)
    print(running_times)

    # return (planning_times, running_times, experiments, base_table)

def main(experiments_dir, output_dir):
    if not os.path.isdir(experiments_dir):
        raise FileNotFoundError

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    # time data
    experiments = get_timing_results(experiments_dir)
    base_tables = get_timing_results(experiments_dir, base=True)
    experiments['is_experiment'] = True
    base_tables['is_experiment'] = False
    base_tables['alpha'] = 'Impute at base tables'

    experiments = drop_warmup(experiments, ['query', 'alpha'], drop=20)
    base_tables = drop_warmup(base_tables, ['query', 'alpha'], drop=20)

    # time measures
    by = ['query', 'alpha']
    planning_times = {}
    planning_times['imputedb'] = summarize(experiments, by, 'plan_time')
    planning_times['base_tables'] = summarize(base_tables, by, 'plan_time')

    running_times = {}
    running_times['imputedb'] = summarize(experiments, by, 'run_time')
    running_times['base_tables'] = summarize(base_tables, by, 'run_time')
        
    # plots
    xticks = range(0, nqueries)
    xlabels = ["Query %i" % (q + 1) for q in xticks]

    try:
        planning_times_items = planning_times.iteritems()
        running_times_items = running_times.iteritems()
    except AttributeError:
        planning_times_items = planning_times.items()
        running_times_items = running_times.items()
        
    for name, df in planning_times_items:
        plot(df, x='query',y='mean',yerr='std',linestyle='none',marker='o')
        plt.xlim(xticks[0] - 1, xticks[-1] + 1)
        plt.xticks(xticks, xlabels)
        plt.xlabel('Query Name')
        plt.ylabel('Planning Time (ms)')
        plt.legend(loc='best')
        plt.savefig(os.path.join(output_dir, 'planning_times_%s.png' % name))
      
    for name, df in running_times_items:
        plot(df, x='query',y='mean',yerr='std',linestyle='none',marker='o')
        plt.xlim(xticks[0] - 1, xticks[-1] + 1)
        plt.xticks(xticks, xlabels)
        plt.xlabel('Query Name')
        plt.ylabel('Running Time (ms)')
        plt.legend(loc='best')
        plt.savefig(os.path.join(output_dir, 'running_times_%s.png' % name))

def write_perf_summary(experiments_dir, table_headers):
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

if __name__ == "__main__":
    def print_usage_and_exit():
        print("usage: python analyze_experiments.py [--explore] <experiment-output-dir>")
        sys.exit(1)

    if len(sys.argv) == 2:
        experiment_dir = sys.argv[1]
        main(experiment_dir, os.path.join(experiment_dir, "analysis"))
    elif len(sys.argv) == 3:
        # Doesn't do much
        try:
            j = sys.argv.index("--explore")  
            k = 1 + (j==1)
            experiments_dir = sys.argv[k]
            explore(experiments_dir)
        except ValueError:
            print_usage_and_exit()
    else:
        print_usage_and_exit()
