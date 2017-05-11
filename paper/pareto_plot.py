import pandas as pd
import matplotlib.pyplot as plt
ANNOTATE = False

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

