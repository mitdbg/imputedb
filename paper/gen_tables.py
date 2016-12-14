#!/usr/bin/env python3

import pandas as pd
import sys

def parse_time(t):
    return t[2:][:-1]

def err_to_str(err):
    if err == 'null':
        return r'\multicolumn{1}{c}{--}'
    else:
        return '%.2e' % float(err)

def err_diff_to_str(err1, err2):
    if err1 == 'null' or err2 == 'null':
        return r'\multicolumn{1}{c}{--}'
    else:
        return '%.2e' % (float(err1) - float(err2))

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
        ctr = 1
        writel(f, r'\newcounter{queryno}')
        writel(f, r'\begin{tabular}{cl}')
        writel(f, r'\toprule')
        writel(f, r'\# & \multicolumn{1}{c}{Query} \\')
        writel(f, r'\midrule')
        for query in df['query'].unique():
            writel(f, r'%d & \verb|%s| \refstepcounter{queryno} \label{q%d} \\' % (ctr, query, ctr))
            qidx[query] = ctr
            idxq[ctr] = query
            ctr += 1
        writel(f, r'\bottomrule')
        f.write(r'\end{tabular}' + '\n')

    with open('runtimes.tex', 'w') as f:
        f.write(r'\begin{tabular}{cSSSSSSS}' + '\n')
        writel(f, r'\toprule')
        writel(f, r'\multicolumn{2}{c}{} & \multicolumn{2}{c}{Imputed ($\alpha=0.0$)} & \multicolumn{2}{c}{Imputed ($\alpha=1.0$)} \\')
        writel(f, r'\cmidrule(r){3-4}')
        writel(f, r'\cmidrule(l){5-6}')
        writel(f, r'\# & \multicolumn{1}{c}{Base error} & \multicolumn{1}{c}{Error} & \multicolumn{1}{c}{Time (s)} & \multicolumn{1}{c}{Error} & \multicolumn{1}{c}{Time (s)} \\')
        writel(f, r'\midrule')
        for query in df['query'].unique():
            ctr = qidx[query]
            
            a0_row = get_first_row(df, lambda row: row['query'] == query and row['alpha'] == 0.0)
            base_err = a0_row['base_err']
            a0_err = a0_row['impute_err']
            # a0_ptime = parse_time(a0_row['plan_time'])
            a0_rtime = parse_time(a0_row['run_time'])

            a1_row = get_first_row(df, lambda row: row['query'] == query and row['alpha'] == 1.0)
            if a1_row is None:
                continue
            a1_err = a1_row['impute_err']
            # a1_ptime = parse_time(a1_row['plan_time'])
            a1_rtime = parse_time(a1_row['run_time'])
            
            f.write(r'\ref{q%d} & %s & %s & %s & %s & %s \\' % \
                    (ctr,
                     err_to_str(base_err),
                     err_diff_to_str(a0_err, base_err),
                     # a0_ptime,
                     a0_rtime,
                     err_diff_to_str(a1_err, base_err),
                     # a1_ptime,
                     a1_rtime) \
                    + '\n')
        writel(f, r'\bottomrule')
        f.write(r'\end{tabular}' + '\n')

if __name__ == '__main__':
    main()
