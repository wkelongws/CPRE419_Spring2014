REGISTER '/home/yuz1988/lab7/exp1/GROWTH.jar';

--Load data
input_lines = LOAD '/class/s14419x/lab7/historicaldata' USING PigStorage(',') 
AS (ticker:chararray,date:int,open:double,high:double,low:double,close:double,volume:long);

-- Keep only the opening price field
project_open = FOREACH input_lines GENERATE ticker,date,open;

-- Filter the stock_prices to two time periods: 19900101-20000103 and 20050102-20140131
stock_price1 = FILTER project_open BY date>=19900101 and date<=20000103;
stock_price2 = FILTER project_open BY date>=20050102 and date<=20140131;


-- Group by the company
group_company1 = GROUP stock_price1 BY ticker;
group_company2 = GROUP stock_price2 BY ticker;

-- Generate growth factor
growth_factor1 = FOREACH group_company1 GENERATE group,GROWTH(stock_price1);
growth_factor2 = FOREACH group_company2 GENERATE group,GROWTH(stock_price2);

-- Remove the tuple that growth factor is null
-- Company doesn't exist for more than 1 day
filter_growth1 = FILTER growth_factor1 BY $1 is not null;
filter_growth2 = FILTER growth_factor2 BY $1 is not null;

-- Sort the result
order_growth1 = ORDER filter_growth1 BY $1 DESC;
order_growth2 = ORDER filter_growth2 BY $1 DESC;

-- Pick up the top 10 companies
top_companies1 = limit order_growth1 10;
top_companies2 = limit order_growth2 10;

--Store the result
STORE top_companies1 INTO '/scr/yuz1988/lab7/exp1/output1/';
STORE top_companies2 INTO '/scr/yuz1988/lab7/exp1/output2/';


