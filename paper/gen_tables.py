#!/usr/bin/env python3

import pandas as pd
import sys

def parse_time(t):
    return t[2:][:-1]

def err_to_str(err):
    if err == 'null':
        return r'$\bot$'
    else:
        return str(err)

def get_first_row(df, l):
    for (_, row) in df.iterrows():
        if l(row):
            return row

def writel(f, l):
    f.write(l + '\n')

def main():
    if len(sys.argv) != 2:
        print('Usage: gen_tables.py QUERIES')
        exit(-1)
        
    df = pd.read_csv(sys.argv[1])

    with open('queries.tex', 'w') as f:
        qidx = {}
        idxq = {}
        ctr = 0
        writel(f, r'\begin{tabular}{ll}')
        writel(f, r'\toprule')
        writel(f, r'\# & Query \\')
        writel(f, r'\midrule')
        for query in df['query'].unique():
            writel(f, r'%d & \verb|%s| \label{q%d} \\' % (ctr, query, ctr))
            qidx[query] = ctr
            idxq[ctr] = query
            ctr += 1
        writel(f, r'\bottomrule')
        f.write(r'\end{tabular}' + '\n')

    with open('runtimes.tex', 'w') as f:
        f.write(r'\begin{tabular}{llllllll}' + '\n')
        writel(f, r'\toprule')
        writel(f, r'\multicolumn{2}{c}{} & \multicolumn{3}{c}{Imputed ($\alpha=0.0$)} & \multicolumn{3}{c}{Imputed ($\alpha=1.0$)} \\')
        writel(f, r'\cmidrule(r){3-5}')
        writel(f, r'\cmidrule(l){6-8}')
        writel(f, r'Query \# & Base error & $\epsilon$ & $t_p$ & $t_r$ & $\epsilon$ & $t_p$ & $t_r$ \\')
        writel(f, r'\midrule')
        for query in df['query'].unique():
            ctr = qidx[query]
            
            a0_row = get_first_row(df, lambda row: row['query'] == query and row['alpha'] == 0.0)
            base_err = a0_row['base_err']
            a0_err = a0_row['impute_err']
            a0_ptime = parse_time(a0_row['plan_time'])
            a0_rtime = parse_time(a0_row['run_time'])

            a1_row = get_first_row(df, lambda row: row['query'] == query and row['alpha'] == 1.0)
            if a1_row is None:
                continue
            a1_err = a1_row['impute_err']
            a1_ptime = parse_time(a1_row['plan_time'])
            a1_rtime = parse_time(a1_row['run_time'])
            
            f.write(r'\ref{q%d} & %s & %s & %s & %s & %s & %s & %s \\' % \
                    (ctr,
                     err_to_str(base_err),
                     err_to_str(a0_err),
                     a0_ptime,
                     a0_rtime,
                     err_to_str(a1_err),
                     a1_ptime,
                     a1_rtime) \
                    + '\n')
        writel(f, r'\bottomrule')
        f.write(r'\end{tabular}' + '\n')

if __name__ == '__main__':
    main()
