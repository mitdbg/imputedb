#!/usr/local/bin/python

# Convert CDC and other data to simpledb format
# Make floating point columns integer
# Can also load into postgres instead

import argparse
import numpy as np
import os
import pandas as pd
import psycopg2 as db
from sqlalchemy import create_engine
import subprocess
import sys


###### CDC Data ##########
# data source
# https://www.kaggle.com/cdc/national-health-and-nutrition-examination-survey

# data dictionary
# https://wwwn.cdc.gov/nchs/nhanes/Search/Nhanes13_14.aspx

# subset of fields
# demographics info
demo_columns = {
  'SEQN'     : 'id',
  'RIAGENDR' : 'gender',               # categorical
  'RIDAGEYR' : 'age_yrs',              # numeric
  'RIDAGEMN' : 'age_months',           # numeric
  'DMDCITZN' : 'is_citizen',           # categorical
  'DMDYRSUS' : 'time_in_us',           # numeric
  'DMDEDUC3' : 'years_edu_children',   # categorical
  'INDHHIN2' : 'income',               # categorical
  'DMDHHSIZ' : 'num_people_household', # categorical
  'DMDMARTL' : 'marital_status'        # categorical
}

# physical examination columns
exams_columns = {
  'SEQN'     : 'id',    
  'PEASCTM1' : 'blood_pressure_secs',    # numeric
  'BMXWT'    : 'weight',                 # numeric
  'BMXHT'    : 'height',                 # numeric
  'BMXARMC'  : 'arm_circumference',      # numeric
  'BMXWAIST' : 'waist_circumference',    # numeric
  'BPACSZ'   : 'cuff_size',              # numeric      
  'BPXSY1'   : 'blood_pressure_systolic',# numeric   
  'BMXHEAD'  : 'head_circumference',     # numeric
  'BMXBMI'   : 'body_mass_index'         # numeric
}

# laboratory columns
labs_columns = {
  'SEQN'     : 'id', 
  'URXUCR'   : 'creatine',            # numeric
  'LBXTR'    : 'triglyceride',        # numeric
  'LBXTC'    : 'cholesterol',         # numeric
  'LBXBPB'   : 'blood_lead',          # numeric
  'URXUMA'   : 'albumin',             # numeric
  'LBXBSE'   : 'blood_selenium',      # numeric
  'LBXWBCSI' : 'white_blood_cell_ct', # numeric
  'LBXHCT'   : 'hematocrit',          # numeric
  'LBDB12'   : 'vitamin_b12'          # numeric
}

## FCC data
# https://github.com/FreeCodeCamp/2016-new-coder-survey/tree/master/clean-data
# Note that clean above doesn't mean without missing values, but
# they do perform some normalizations in category labels etc
fcc_columns = [
   'Age',
   'AttendedBootcamp',
   'BootcampFinish',
   'BootcampFullJobAfter',
   'BootcampPostSalary',
   'BootcampLoanYesNo',
   'CommuteTime',
   'ChildrenNumber',
   'CityPopulation',
   'CountryCitizen',
   'Gender',
   'HoursLearning',
   'Income',
   'MoneyForLearning',
   'MonthsProgramming',
   'StudentDebtOwe',
   'SchoolDegree'
 ]
fcc_columns = dict(zip(fcc_columns, map(lambda x: x.lower(), fcc_columns)))


def read_csv(path, col_map):
  # avoid deciding type before, these files are dirty and small
  df = pd.read_csv(path, low_memory=False)
  return df[col_map.keys()].rename(columns = col_map)

def get_float_cols(df):
  return df.columns[df.dtypes == float].tolist()

def float_fixed_precision(df, prec):
  df = df.copy()
  for col in get_float_cols(df):
    # check if the column actually uses decimal points
    vals = df[col]
    not_missing = vals[~np.isnan(vals)]
    if (not_missing != np.floor(not_missing)).any():
      df[col] = np.floor(np.round(df[col], prec) * 10 ** prec)
  return df
  
def quick_view(df, col):
  ndf = pd.isnull(df).any()
  print ndf[col]
  print df[col].value_counts(dropna = False)
  
def null_dist(df, nm = None):
  nulls = pd.isnull(df).mean().to_frame().reset_index()
  nulls.columns = ['Attribute', '% Missing']
  nulls['% Missing'] *= 100
  nulls = nulls.sort_values('Attribute', ascending = True)
  if not nm is None:
    nulls['Table'] = nm
  return nulls  
    
def explain(engine, query):
  ex = engine.execute("EXPLAIN " + query).fetchall()
  return ("\n".join(map(lambda x: x[0], ex))) + "\n\n"

def get_types(df):
  simple_db_map = {'int64':'int', 'float64': 'int', 'object': 'string'}
  return [simple_db_map[str(typ)] for typ in df.dtypes]

def get_schema(nm, df):
  types = get_types(df)
  schema_cols = ", ".join(["%s %s" % (col, typ) for col, typ in zip(df.columns, types)])
  return "%s(%s)" % (nm, schema_cols)

def string_to_integer(df):
  to_str = df.columns[df.dtypes == object].tolist()
  rewrite = {}
  for col in to_str:
    vals = sorted(list(set(df[col])))
    enum_map = {c : i for i, c in enumerate(vals)}
    df[col] = df[col].map(lambda x: enum_map[x])
    rewrite[col] = enum_map
  return df, rewrite
  
def enum_to_string(enum):
  msg = ""
  for col, mapping in enum.iteritems():
    msg += "Enumeration for %s\n" % col
    for s, i in mapping.iteritems():
      msg += "%s:%d\n" % (s, i)
  return msg
  
  
### Postgres
def add_to_postgres(db_name, dfs):
  master_db_con = db.connect(database = 'postgres')
  master_db_con.autocommit = True
  try:
    print "Creating db %s" % db_name
    master_db_con.cursor().execute('create database %s' % db_name)
  except db.ProgrammingError as e:
    print e.message
  cdc_engine = create_engine('postgresql://localhost/%s' % db_name)
  for nm, df in dfs.iteritems():
    df, _ = string_to_integer(df)
    df.to_sql(nm, cdc_engine)

def sample_queries(db_name):
  # couple of sample queries
  # cdc
  # average weight based on household income category
  query1 = "select income, avg(weight) from demo inner join exams using (id) group by income"

  # average cholesterol for people with low income/high income with above average weight
  query2 = """
  select income, count(*), avg(cholesterol) from 
  demo inner join exams using (id) 
       inner join labs using (id)
       where income = 15 or income = 13
       and weight >= 63
       group by income
  """

  # max blood lead for people with above average waist circumference
  query3 = "select max(blood_lead) from exams inner join labs using(id) where   waist_circumference >= 87"
  
  #fcc
  # what is the average income for participant who went
  # to a bootcamp vs not
  query4 = "select attendedbootcamp, avg(income) from fcc where attendedbootcamp <= 1 group by attendedbootcamp"
  
  # what is the average age for women who were learning to program
  # in the US (we replaced strings with codes)
  query5 = "select avg(age) from fcc where gender = 2 and countrycitizen = 158"
  

  # what learning budget to students who owe money in student debt
  # have? break it out by school degree
  query6 = "select schooldegree, avg(moneyforlearning) from fcc where studentdebtowe > 0 and schooldegree >= 0 group by schooldegree"
  
  example_engine = create_engine('postgresql://localhost/%s' % db_name)
  queries = [query1, query2, query3, query4, query5, query6]
  for i, q in enumerate(queries):
    print "Query %d" % i
    print explain(example_engine, q)
    print "========================"
    

### simpledb
def to_simpledb(dfs, prec, jar, path, suffix):
  schemas = []
  for name, df in dfs.iteritems():
    # make floats ints
    df = float_fixed_precision(df, prec)
    # make strings ints
    df, enum = string_to_integer(df)
    if len(enum):
      # need to write out the numeration
      enum_info = enum_to_string(enum)
      with open(os.path.join(path, name + suffix + '.catalog'), 'w') as f:
        f.write(enum_info)
    csv_path = os.path.join(path, name + suffix + '.csv')
    # no suffix for the dat files, since those need to match schema name
    dat_path = os.path.join(path, name + '.dat')
    df.to_csv(csv_path, float_format = '%.0f', header = False, index = False)
    with open(dat_path, 'w') as f:
      type_str = ','.join(get_types(df))
      p1 = subprocess.Popen(['java', '-jar', jar, 'convert', csv_path, str(df.shape[1]), type_str], stdout=f)
      p1.communicate()
    schemas.append(get_schema(name, df))
  with open('catalog.txt' , 'w') as f:
    f.write("\n".join(schemas))
  
def read_csv_data(path):
  info  = {
    'demo' : ('demographic.csv', demo_columns),      # cdc data
    'exams' : ('examination.csv', exams_columns),    # cdc data
    'labs' : ('labs.csv', labs_columns),             # cdc data
    'fcc'  : ('fcc.csv', fcc_columns)                # fcc data
  }
  dfs = {}
  for name, (file_nm, cols) in info.iteritems():
    df = read_csv(os.path.join(path, file_nm), cols)
    dfs[name] = df
  return dfs
 
def help(p, extra_msg = None):
  if not extra_msg is None:
    print extra_msg
  p.print_help()
  sys.exit(1) 
  
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('action', help = 'one of [postgres, simpledb]. postgres loads data into a postgres database, simpledb creates serialized versions of tables and schema info')
  parser.add_argument('input_path', help = 'path to read csvs and save down any new data')
  parser.add_argument('-p', "--precision", help = 'precision for floating point to integer (rounds to said precision and multiplies by 10^precision before truncating)', type = int)
  parser.add_argument('-j', '--jar', help = 'path to simpledb java jar')
  parser.add_argument('-s', '--suffix', help = 'suffix for files created')
  parser.add_argument('-d', '--database', help = 'optional name for postgres database')

  args = parser.parse_args()
  input_path = args.input_path

  if args.action == 'postgres':
    db_name = 'missing_values' if args.database is None else args.database
    add_to_postgres(db_name, read_csv_data(input_path))
    sample_queries(db_name)
  elif args.action == 'simpledb':
    jar = args.jar
    if jar == None:
      help(parser, 'Must provide --jar')
    precision = 0 if args.precision is None else args.precision
    suffix = '-integer' if args.suffix is None else args.suffix
    to_simpledb(read_csv_data(input_path), precision, jar, input_path, suffix)
  else:
    help(parser, 'Invalid action')
      
  
if __name__ == '__main__':
  main()  
