# Generate some simple large join queries to evaluate imputedb
import random

# tables from cdc data, share id column for joins
tables = ['demo', 'labs', 'exams']
# use a single column from each table for projection
missing_col = {'demo': 'marital_status', 'labs': 'creatine', 'exams': 'waist_circumference'}

def join_query(tables):
  n = len(tables)
  tids = range(0, n)
  from_clause = ['%s t%d' % (tbl, id) for id, tbl in zip(tids, tables)]
  from_clause = ', '.join(from_clause)
  where_clause = ['t%d.id = t%d.id' % (t1, t2) for t1, t2 in zip(tids, tids[1:])]
  where_clause = " and ".join(where_clause)
  select_clause = ['t%d.%s' % (id, missing_col[table]) for id, table in zip(tids, tables)]
  select_clause = ', '.join(select_clause)
  return "select %s from %s where %s" % (select_clause, from_clause, where_clause)

def self_join(tbl, n):
  return join_query([tbl] * n)

def create_join_workload(n_tables, n_queries):
  random.seed(1)
  workload = []
  # n random queries with joins of given size
  for _ in range(0, n_queries):
    random_tables = [tables[random.randint(0, len(tables) - 1)] for _ in range(0, n_tables)]
    workload.append(join_query(random_tables))
  return '\n'.join([q + ';' for q in workload])
