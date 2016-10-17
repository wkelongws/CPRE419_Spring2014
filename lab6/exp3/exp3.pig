-- Load the data from 'ip_trace' using space to separate fields 
input_ip_trace = LOAD '/class/s14419x/lab6/ip_trace' USING PigStorage(' ') AS (time:chararray,
conn_id_trace:long,src_ip:chararray,symbol:chararray,dst_ip:chararray,protocol:chararray,data:int);

-- Load the data from 'raw_block' using space to separate fields
input_raw_block = LOAD '/class/s14419x/lab6/raw_block' USING PigStorage(' ') AS 
(conn_id_block:long,action_taken:chararray);

-- Project the relation 'input_ip_trace' on the fields time,connection_id,src_ip and dst_ip
ip_trace = FOREACH input_ip_trace GENERATE time,conn_id_trace,src_ip,dst_ip;
-- Filter the relation 'input_raw_block', keep tuples with 'Blocked'
raw_block = FILTER input_raw_block BY action_taken=='Blocked';

-- Join the projected and filtered relations
join_result = JOIN ip_trace BY conn_id_trace, raw_block BY conn_id_block;

-- Project the join result to be the firewall format
firewall = FOREACH join_result GENERATE ip_trace::time AS time,ip_trace::conn_id_trace AS conn_id,
ip_trace::src_ip AS src,ip_trace::dst_ip AS dst,raw_block::action_taken AS action;

-- Store the output
STORE firewall INTO '/scr/yuz1988/lab6/exp3/firewall/';

-- Group the relation firewall by the field ip_trace::src_ip
group_result = GROUP firewall BY src;

-- For each src_ip count the number of times it is blocked
count_result = FOREACH group_result GENERATE group,COUNT(firewall);

-- Sort the count_result by the number of count
sort_result = ORDER count_result BY $1 DESC;

-- Store the output
STORE sort_result INTO '/scr/yuz1988/lab6/exp3/output/';

