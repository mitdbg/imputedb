#!/usr/local/bin/python

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os.path
import sys
from StringIO import StringIO
from collections import defaultdict

if len(sys.argv) != 4:
  print "Usage: python analyze_experiments.py <experiment-results-dir> <base-results-dir> <output-dir>"
  sys.exit(1)

experiments_dir = sys.argv[1]
base_dir = sys.argv[2]
output_dir = sys.argv[3]

# basic utils
def drop_warmup(df, by, keep=5):
  return df.groupby(by).tail(keep)

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
  
def get_query_results(path, headers):
  lines = []
  with open(path, 'r') as f:
    lines = f.readlines()
  lines = [l.strip() for l in lines]
  lines = [l for l in lines if len(l) > 0]
  results = defaultdict(lambda: [])
  i = 0
  # consume all results
  while i < len(lines):
    line = lines[i]
    start_signal = 'alpha'
    end_signal = 'query'
    if line[:len(start_signal)].lower() == start_signal:
      alpha = float(line.split(' ')[1])
      query = int(lines[i-1].split(' ')[1])
      data = []
      # skip over Results:
      i += 2
      next_line = lines[i]
      # consume current result as df
      while len(next_line) > 0 and next_line[:len(end_signal)].lower() != end_signal:
        # append raw record
        data.append(next_line)
        i += 1
        if i < len(lines):
          next_line = lines[i]
        else:
          next_line = []
      if data:
        str_data = '\n'.join(data)
        buf = StringIO(str_data)
        df = pd.read_csv(buf, names=headers[query])
        if len(df.columns) > 1:
          df = df.set_index(df.columns[0])
        results[(query, alpha)].append(df)
    else:
      i += 1  
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


# time data
experiments = pd.read_csv(os.path.join(experiments_dir, 'times.csv'))
base_tables = pd.read_csv(os.path.join(base_dir, 'times.csv'))
experiments['is_experiment'] = True
base_tables['is_experiment'] = False
base_tables['alpha'] = 'Impute at base tables'

experiments = drop_warmup(experiments, ['query', 'alpha'], keep=5)
base_tables = drop_warmup(base_tables, ['query', 'alpha'], keep=5)

# time measures
by = ['query', 'alpha']
planning_times = {}
planning_times['imputedb'] = summarize(experiments, by, 'plan_time')
planning_times['base_tables'] = summarize(base_tables, by, 'plan_time')

running_times = {}
running_times['imputedb'] = summarize(experiments, by, 'run_time')
running_times['base_tables'] = summarize(base_tables, by, 'run_time')
    
# plots
xticks = range(0, 7)
xlabels = ["Query %i" % (q + 1) for q in xticks]

for name, df in planning_times.iteritems():
  plot(df, x='query',y='mean',yerr='std',linestyle='none',marker='o')
  plt.xlim(xticks[0] - 1, xticks[-1] + 1)
  plt.xticks(xticks, xlabels)
  plt.xlabel('Query Name')
  plt.ylabel('Planning Time (ms)')
  plt.legend(loc='best')
  plt.savefig(os.path.join(output_dir, 'planning_times_%s.png' % name))
  
for name, df in running_times.iteritems():
  plot(df, x='query',y='mean',yerr='std',linestyle='none',marker='o')
  plt.xlim(xticks[0] - 1, xticks[-1] + 1)
  plt.xticks(xticks, xlabels)
  plt.xlabel('Query Name')
  plt.ylabel('Running Time (ms)')
  plt.legend(loc='best')
  plt.savefig(os.path.join(output_dir, 'running_times_%s.png' % name))

# compare actual query results
table_headers = [
  ['income', 'weight'],
  ['income', 'cholesterol'],
  ['blood_lead'],
  ['attendedbootcamp', 'income'],
  ['age'],
  ['schooldegree', 'moneyforlearning'],
  ['attendedbootcamp', 'gdp_per_capita'],
]
experiment_results = get_query_results(os.path.join(experiments_dir, 'results.txt'), table_headers)
base_results = get_query_results(os.path.join(base_dir, 'results.txt'), table_headers)
perf = []
for (query, alpha), res in experiment_results.iteritems():
  refs = [df for (q, a), dfs in base_results.iteritems() for df in dfs if q == query]
  smape, std = get_smape(refs, res)
  perf.append((query, alpha, smape, std))

perf_summary = pd.DataFrame(perf, columns = ['query', 'alpha', 'smape', 'std'])
perf_summary = perf_summary.sort_values(['query', 'alpha'])
perf_summary_latex = perf_summary.copy()[['query', 'alpha', 'smape']]
perf_summary_latex['smape'] *= 100.0
perf_summary_latex.columns = ['Query', 'Alpha', 'SMAPE']
perf_summary_latex.to_latex(os.path.join(output_dir, 'perf_summary.tex'), float_format='%.2f', index=False)



