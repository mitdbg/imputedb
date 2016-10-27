-- Sample queries to run over ACS data based on existing aggregates and our pre-defined aggregates


-------------------- Queries based on ACS own aggregate summaries -----------------
-- http://factfinder.census.gov/faces/tableservices/jsf/pages/productview.xhtml?pid=ACS_14_5YR_DP04&src=pt
-- SELECT COUNT(*) FROM acs GROUP BY <group>

SELECT BLD as units_in_structure, COUNT(*) as estimate FROM hh GROUP BY BLD;

-- Not sure if we're gonna get a good match between the queries they run on the data and what
-- we can do in simple db


---------------------------------- Ad-Hoc Queries -----------------------------------

-- Aggregates only:
-- Template: SELECT <aggregate> FROM acs [GROUP BY <group>]?
-- Possibilities: Imputation at base table or after grouping

-- Count: Counts shower/bathtub vs none 
SELECT BATH as has_bath, COUNT(*) as ct FROM hh GROUP BY BATH;

-- Avg: Average number of bedrooms by lotsize
SELECT ACR as lotsize, AVG(BDSP) as avg_num_bedrooms FROM hh GROUP BY ACR;

-- Sum: Total number of people who live in multi-household homes
SELECT PSF as has_sub_families, SUM(NP) as num_people FROM hh GROUP BY PSF; 

-- Avg: Average number of people in a household across all responses
SELECT AVG(NP) as avg_num_people FROM hh;


----------------------------------------------------------------------------

-- Aggregates with filtering:
-- Template: SELECT <aggregate> FROM acs WHERE <filter> [GROUP BY <group>]?
-- Possibilities: Imputation at base table, after filtering, or after grouping

-- Avg: Average lotsize by number of bedrooms for houses that have 2 or more vehicles
SELECT BDSP as num_bedrooms, AVG(ACR) as avg_lot_size FROM hh WHERE VEH >= 2 GROUP BY BDSP;

-- Max: Oldest construction for houses in the largest lot size bucket
SELECT MIN(YBL) as earliest_built_bucket FROM hh WHERE ACR = 3;

-- Min: Minimum number of rooms (not just bedrooms) for houses that don't have running hot/cold water
SELECT MIN(RMSP) as min_num_rooms FROM hh WHERE RWAT=2;

----------------------------------------------------------------------------
-- TODO: these don't actually work in simpledb, since we don't have expressions beyond aggreggates
-- They also generate huge tables so joins take forever....

-- Aggregates with joins and other filtering
-- Template: SELECT <aggregate> FROM acs {, acs}+ WHERE <filter> [GROUP BY <group>]?
-- Posibilities: Imputation at base tables, after pushed-down filters, after joining, after grouping
-- Since we only have one table, we perform self-joins

-- Average ratio of number of bedrooms in a house relative to houses in a different lot size
-- (limited to households with just 1 occupant)
-- SELECT 
-- a1.ACR as ref_lot_size, 
-- AVG(a1.BDSP / a2.BDSP) as ratio_avg_bedrooms 
-- FROM hh a1, hh a2 
-- WHERE a1.ACR <> a2.ACR -- join condition
-- AND a1.ACR = 1
-- GROUP BY a1.ACR;


-- Average ratio of number of people in a household relative to households with a larger number
-- of vehicles
-- SELECT
-- a1.VEH as num_vehicles,
-- AVG(a1.NP / a2.NP) as ratio_avg_people
-- FROM hh a1, hh a2
-- WHERE a1.VEH < a2.VEH AND a1.RMSP = 10 AND a2.RMSP = 10
-- GROUP BY a1.VEH;



----------------------------------------------------------------------------

-- Point/subset queries
-- Template: SELECT <attr> FROM acs WHERE <filter>
-- Posibilities: Imputation at base tables, or after filter

-- All records that have refrigerator, sink, stove, telephone, but not flush toilet
SELECT * FROM hh WHERE REFR = 1 AND STOV = 1 AND TEL = 1 AND TOIL = 2;

-- All records that have larger number of cars than rooms (limited to 1 - 5 cars, as 6 is really 6+)
SELECT * FROM hh WHERE VEH >= 1 AND VEH <= 5 AND RMSP > VEH;












