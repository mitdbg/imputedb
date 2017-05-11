#!/usr/bin/env python2

# Convert CDC and other data to simpledb format
# Make floating point columns integer
# Can also load into postgres instead

import argparse
from collections import defaultdict
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
  'DMDEDUC3' : 'years_edu',            # categorical
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

### World Bank Data GDP
# http://databank.worldbank.org/data/reports.aspx?Code=NY.GDP.PCAP.CD&id=af3ce82b&report_name=Popular_indicators&populartype=series&ispopular=y#

gdp_columns = {
  'Country Name'  : 'country',
  '2015 [YR2015]' : 'gdp_per_capita'
}

def read_csv(path, col_map):
  # avoid deciding type before, these files are dirty and small
  df = pd.read_csv(path, low_memory=False)
  if not col_map is None:
    df = df[col_map.keys()].rename(columns = col_map)
  else:
    # otherwise just name as c0 .... cn
    cols = ["c%i" % c for c in range(df.shape[1])]
    df.columns = cols
  return df

def add_nulls_if_none(df, pct):
  if pd.isnull(df).any().any():
    return df
  else:
    print "Adding %f pct nulls" % pct
    np.random.seed(1)
    vals = df.values.flatten().copy().astype(float)
    n = len(vals)
    ix = np.random.choice(n, size=int(pct * n), replace=False)
    vals[ix] = np.nan
    vals = vals.reshape(df.shape)
    return pd.DataFrame(vals, columns=df.columns)

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

def get_str_cols(df):
  return df.columns[df.dtypes == object].tolist()

def collect_enum(df, enum):
  ## collect enumeration values
  str_cols = get_str_cols(df)
  for col in str_cols:
    keys = set(df[col])
    # only enumerate non-missing strings NaNs remain NaNs
    missing = sorted([k for k in keys if not k in enum and not pd.isnull(k)])
    val = max(enum.values()) + 1 if len(enum) else 0
    for k in missing:
      enum[k] = val
      val += 1

def apply_enum(df, enum):
  # applies enum and filters out
  # any records that have a null in
  # the cols once the enum has been applied
  str_cols = get_str_cols(df)
  if len(enum) == 0 or len(str_cols) == 0:
    return df
  # entries not in the enum map to missing (NaN)
  robust_enum = defaultdict(lambda : np.nan, enum)
  df = df.copy()
  for col in str_cols:
    df[col] = df[col].map(robust_enum)
  return df.reset_index()
 
def write_out_enum(path, enums):
  if len(enums) == 0:
    return
  enums = sorted(enums.items(), key = lambda x:x[0])
  with open(os.path.join(path, 'enums.txt'), 'w') as f:
    f.write("Enumerated strings:\n")
    for s, i in enums:
      f.write("%s:%d\n" % (s, i))
   
### Postgres
def add_to_postgres(dfs, db_name, path):
  master_db_con = db.connect(database = 'postgres')
  master_db_con.autocommit = True
  try:
    print "Creating db %s" % db_name
    master_db_con.cursor().execute('create database %s' % db_name)
  except db.ProgrammingError as e:
    print e.message
  cdc_engine = create_engine('postgresql://localhost/%s' % db_name)
  enums = {}
  for df in dfs.itervalues():
    collect_enum(df, enums)
  write_out_enum(path, enums)
  for name, df in dfs.iteritems():
    df = apply_enum(df, enums)
    df.to_sql(name, cdc_engine)

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
  query4 = "select attendedbootcamp, avg(income) from fcc group by attendedbootcamp"
  
  # what is the average age for women who were learning to program
  # in the US (we replaced strings with codes)
  query5 = "select avg(age) from fcc where gender = 178 and countrycitizen = 158"
  
  # what learning budget to students who owe money in student debt
  # have? break it out by school degree
  query6 = "select schooldegree, avg(moneyforlearning) from fcc where studentdebtowe > 0 and schooldegree >= 0 group by schooldegree"
  
  ## a query joining with a reference table
  # what is the average GDP-per-capita for users
  # who decided to enroll in a bootcamp versus not
  query7 = """
  select 
  attendedbootcamp,
  avg(gdp_per_capita)
  from fcc, gdp where fcc.countrycitizen = gdp.country
  group by attendedbootcamp
  """
  
  example_engine = create_engine('postgresql://localhost/%s' % db_name)
  queries = [query1, query2, query3, query4, query5, query6, query7]
  for i, q in enumerate(queries):
    print "Query %d" % i
    print explain(example_engine, q)
    print "========================"
    

### simpledb
def to_simpledb(dfs, prec, jar, path, suffix):
  schemas = []
  enums = {}
  # collect possible enumerations
  for df in dfs.itervalues():
    collect_enum(df, enums)
  write_out_enum(path, enums)
  for name, df in dfs.iteritems():
    # make floats ints
    df = float_fixed_precision(df, prec)
    # apply enumerations
    df = apply_enum(df, enums)
    # if has no nulls naturally, then add some
    df = add_nulls_if_none(df, 0.4)
    csv_path_no_headers = os.path.join(path, name + suffix + '.csv')
    csv_path_with_headers = os.path.join(path, name + suffix + '_with_headers.csv')
    # no suffix for the dat files, since those need to match schema name
    dat_path = os.path.join(path, name + '.dat')
    df.to_csv(csv_path_no_headers, float_format = '%.0f', header = False, index = False)
    df.to_csv(csv_path_with_headers, float_format = '%.0f', header = True, index = False)
    with open(dat_path, 'w') as f:
      type_str = ','.join(get_types(df))
      p1 = subprocess.Popen(['java', '-jar', jar, 'convert', csv_path_no_headers, str(df.shape[1]), type_str], stdout=f)
      p1.communicate()
    schemas.append(get_schema(name, df))
  catalog_path = os.path.join(path, 'catalog.txt')
  with open(catalog_path , 'w') as f:
    f.write("\n".join(schemas))
  
def read_csv_data(path):
  info  = {
    'demo'      : ('demographic.csv', demo_columns),      # cdc data
    'exams'     : ('examination.csv', exams_columns),     # cdc data
    'labs'      : ('labs.csv', labs_columns),             # cdc data
    'fcc'       : ('fcc.csv', fcc_columns),               # fcc data
    'gdp'       : ('gdp.csv', gdp_columns),               # gdp data
    'acs_dirty' : ('acs_no_header.csv', None)             # acs data
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
    add_to_postgres(read_csv_data(input_path), db_name, input_path)
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
