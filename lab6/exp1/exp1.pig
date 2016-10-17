-- Load the data from 'gaz_tracts_national.txt'
input_lines = LOAD '/class/s14419x/lab6/gaz_tracts_national.txt' AS (usps:chararray,geoid:chararray,
pop10:int,hu10:int,aland:double,awater:double,aland_sqmi:double,awater_sqmi:double,intptlat:double,intptlong:double); 


-- Project on usps and aland fields
gaz_usps_aland = FOREACH input_lines GENERATE usps,aland;

-- Remove the title (first row)
filtered_gaz = FILTER gaz_usps_aland BY usps!='USPS';

-- Group by the usps field
group_gaz = GROUP filtered_gaz BY usps;

-- Calculate the sum of aland field in each group;
sum_gaz = FOREACH group_gaz GENERATE group, SUM(filtered_gaz.aland);

-- Sort the sum_gaz by the $1 field
ordered_gaz = ORDER sum_gaz BY $1 DESC; 

-- Limit to first two tuples 
result = Limit ordered_gaz 2;

-- Store the output
STORE result INTO '/scr/yuz1988/lab6/exp1/output/';

