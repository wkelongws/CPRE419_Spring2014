-- Load the data from 'network_trace' using space to separate fields 
input_lines = LOAD '/class/s14419x/lab6/network_trace' USING PigStorage(' ') AS (time:chararray,
ip:chararray,src_ip:chararray,symbol:chararray,dst_ip:chararray,protocol:chararray,data:int);

-- Remove tuples using protocol other than 'tcp'
filtered_input = FILTER input_lines BY protocol=='tcp';

-- Project on the fields src_ip and dst_ip and change the ip addresses to the formal expression
src_dst = FOREACH filtered_input GENERATE SUBSTRING(src_ip,0,LAST_INDEX_OF(src_ip,'.')) AS src_ip, 
SUBSTRING(dst_ip,0,LAST_INDEX_OF(dst_ip,'.')) AS dst_ip;

-- Group the relation src_dst by the first field src_ip
group_ip = GROUP src_dst BY src_ip;

-- For each source ip find the number of distinct destination ip addresses
distinct_ip = FOREACH group_ip {
PA = src_dst.dst_ip; 
DA = DISTINCT PA;
GENERATE group, COUNT(DA);
}

-- Sort the distinct_ip by the count field
ordered_ip = ORDER distinct_ip BY $1 DESC;

-- Limit to first ten tuples
result = LIMIT ordered_ip 10;

-- Store the output
STORE result INTO '/scr/yuz1988/lab6/exp2/output/';
