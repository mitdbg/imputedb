// generate synthetic data uniformly between 0 and X, and dirty dpct of the fields
args:.Q.opt .z.x;
usage:"q generate_synthetic.q -nrow <int> -ncol <int> -range <int> -dpct <float>"
// set seed
\S 10
// defaults
RANGE:100;
NROW:10000;
NCOL:10;
DPCT:0.1;
// user provided
getarg:{[input;arg;def] def^first (type def)$input arg}
nrow:getarg[args;`nrow;NROW];
ncol:getarg[args;`ncol;NCOL];
dpct:getarg[args;`dpct;DPCT];
// generate data
n:nrow*ncol;
dirty:@[clean:n?RANGE;(neg floor dpct*n)?n;first 0#];
ts:`dirty`clean!ncol cut/:(dirty;clean);
`:clean.csv 0:csv 0:ts`clean;
`:dirty.csv 0:csv 0:ts`dirty;
exit 0;

/
Ran for paper with -nrow 10000 -ncol 10 -dpct 0.25 -range 100