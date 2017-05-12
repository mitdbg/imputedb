import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
ANNOTATE = False

def plot_frontier():
  df = pd.read_csv('tables/pareto_frontiers.csv')
  df['norm_time'] = df.groupby('query')['time'].apply(lambda x: x / x.max())
  df['id'] = range(0, df.shape[0])
  interesting_queries = [1, 6] # 2 is also nice, but want one from each set of experiments
  # plans that are different mainly due to floating point precision, collapse
  collapse = [0, 1, 2, 4, 7] + [31, 33]
  fig, axes = plt.subplots(nrows=1, ncols=2, sharey=True, sharex=True, figsize=(8,4))
  plt.subplots_adjust(hspace=0.2)
  for i, q in enumerate(interesting_queries):
    data  = df[(df['query'] == q) & ~df['id'].isin(collapse)]
    ax = axes[i]
    data.plot(kind='scatter', x='norm_time', y = 'penalty', ax=ax, s=20)
    if ANNOTATE:
      for row in data.itertuples():
        ax.annotate('%d' % row.id, (row.norm_time, row.penalty), (row.norm_time * 1.01, row.penalty * 1.01))
    ax.set_title('Query %d' % q)
    # damn you matplotlib
    ax.set_xlabel('')
    ax.set_ylabel('')

    plt.subplots_adjust(bottom=0.2)
    fig.suptitle('Pareto Frontiers', size=16)
    fig.text(0.5, 0.04, 'Time estimate/Max(Time estimate)', ha='center')
    fig.text(0.04, 0.5, 'Penalty Estimate', va='center', rotation='vertical')
    fig.savefig('figures/pareto_frontiers_plot.png')


def plot_planning():
  df_approx = pd.read_csv('tables/approximate_pareto.csv')
  df_approx['plan_type'] = 'approximate'
  df_exact = pd.read_csv('tables/exact_pareto.csv')
  df_exact['plan_type'] = 'exact'
  df = pd.concat([df_approx, df_exact], axis=0)
  df['njoins'] = df['njoins'].astype(int)

  # drop warmups and calculate summary stats
  drop_warmup = lambda df: df.groupby(['plan_type', 'query', 'njoins', 'alpha']).tail(10)
  df = drop_warmup(df)

  means = df.groupby(['njoins', 'plan_type'])['plan_time'].mean().unstack()
  errors =  df.groupby(['njoins', 'plan_type'])['plan_time'].std().unstack()

  # approximate planning doesn't kick in until 6 joins (7 tables)
  # so values for less than that are really just jvm noise
  means.loc[means.index < 6, 'approximate'] = 0.0
  errors.loc[errors.index < 6, 'approximate'] = 0.0

  fig, bar_plot = plt.subplots(1, figsize=(6, 6))
  means.plot(kind='bar', yerr=errors, logy=True, rot=0, ax=bar_plot)
  bar_plot.set_xlabel("Number of joins")
  bar_plot.set_ylabel("Average planning time (ms)")
  bar_plot.legend(labels=['Approximate', 'Exact'], loc='best', title='Frontier type')
  bar_plot.get_figure().savefig('figures/planning_times_plot.png')


def help_msg():
  print "Usage: python pareto_plot.py <frontier|planning>"

if __name__ == '__main__':
  if len(sys.argv) != 2:
    help_msg()
    sys.exit(1)
  task = sys.argv[1]
  if task == "frontier":
    plot_frontier()
  elif task == "planning":
    plot_planning()
  else:
    raise ValueError('Undefined task: ' + task)
