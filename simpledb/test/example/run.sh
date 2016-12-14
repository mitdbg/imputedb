#!/usr/bin/env bash

# # convert dat to txt file so we can read it with other programs
# java -jar dist/simpledb.jar print test/testdata/acs.dat 37 > acs.txt

VERSION=3
julia dirty$VERSION.jl
java -jar ../../dist/simpledb.jar convert acs_dirty$VERSION.txt 37 > acs_dirty$VERSION.dat
echo "acs_dirty$VERSION (ST int, NP int, ACR int, AGS int, BATH int, BDSP int, BLD int, BUS int, REFR int, RMSP int, RWAT int, SINK int, STOV int, TEL int, TEN int, TOIL int, VEH int, YBL int, HHL int, HHT int, HUGCLNPP int, HUPAC int, HUPAOC int, HUPARC int, LNGI int, MULTG int, MV int, NR int, NRC int, PARTNER int, PSF int, R18 int, R65 int, SRNTVAL int, WIF int, WKEXREL int, WORKSTAT int)" \
    | cat catalog1.txt - > catalog$VERSION.txt
echo "SELECT polling.ST, AVG(acs_dirty$VERSION.TEL) FROM polling, acs_dirty$VERSION WHERE polling.ST = acs_dirty$VERSION.ST AND polling.ERROR > 50 GROUP BY polling.ST;" \
    > query$VERSION.txt
java -jar ../../dist/simpledb.jar draw catalog$VERSION.txt query$VERSION.txt e9 0.1 > e9.txt
java -jar ../../dist/simpledb.jar draw catalog$VERSION.txt query$VERSION.txt e10 1.0 > e10.txt
dot -Tpng e9_0.dot > e9_0.png
dot -Tpng e10_0.dot > e10_0.png
