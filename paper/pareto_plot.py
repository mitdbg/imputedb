import pandas as pd
import matplotlib.pyplot as plt
ANNOTATE = False

df = pd.read_csv('tables/pareto_frontiers.csv')  
df['norm_time'] = df.groupby('query')['time'].apply(lambda x: x / x.max())
df['id'] = range(0, df.shape[0])
interesting_queries = [1, 6] # 2 is also nice, but want one from each set of experiments
# plans that are different mainly due to floating point precision, collapse
collapse = [0, 1, 2, 4, 7] + [31, 33]
fig, axes = plt.subplots(2, sharex=True, figsize=(4.5, 6))
plt.subplots_adjust(hspace=0.2)
for i, q in enumerate(interesting_queries):
  data  = df[(df['query'] == q) & ~df['id'].isin(collapse)]
  ax = axes[i]
  data.plot(kind='scatter', x='norm_time', y = 'penalty', ax=ax, s=20)
  if ANNOTATE:
    for row in data.itertuples():
      ax.annotate('%d' % row.id, (row.norm_time, row.penalty), (row.norm_time * 1.01, row.penalty * 1.01))
  ax.set_title('Pareto Frontier for Query %d' % q)
  ax.set_xlabel('Time estimate/Max(Time estimate)')
  ax.set_ylabel('Penalty Estimate')
plt.subplots_adjust(left=0.2)
  
fig.savefig('figures/pareto_frontiers_plot.png')

