# # install postgres
# brew install postgres
# # start server
# pg_ctl -D /usr/local/var/postgres -l /usr/local/var/postgres/server.log start
# after running this you can query by psql -d acs

import subprocess
import sys

def create_db(db):
  cmd = "CREATE DATABASE " + db
  # needs to run from base db postgres
  run_cmd("postgres", cmd) 

def create_table(db, tbl, cols, types):
  typed_cols = ", ".join([col + " " + type for col, type in zip(cols, types)])
  cmd = "CREATE TABLE %s(%s)" % (tbl, typed_cols)
  run_cmd(db, cmd)

def insert_data(db, tbl, file):
  cmd = "COPY %s FROM '%s' WITH HEADER CSV" % (tbl, file)
  run_cmd(db, cmd)

def run_cmd(db, cmd):
  try:
    subprocess.call(["psql", "-d", db, "-c", cmd ], shell = False)
  except OSError:
    print "Install postgres first"
    sys.exit(1)
  
def make_acs_db(file_name):  
  csv_data = open(file_name)
  columns_str = csv_data.readline()
  csv_data.close()
  columns = [col[1:-1].lower() for col in columns_str.strip().split(",")]
  create_db("acs")
  create_table("acs", "hh", columns, ['integer'] * len(columns))
  insert_data("acs", "hh", file_name)
  
if __name__ == "__main__":
  if(len(sys.argv) != 2):
    print "Usage: <full-path-file-name>"
    sys.exit(1)
  else:
    make_acs_db(sys.argv[1])
  
  
