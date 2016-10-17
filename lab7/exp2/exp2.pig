REGISTER '/home/yuz1988/lab7/exp2/MOVAVG.jar';

--Load data
input_lines = LOAD '/class/s14419x/lab7/historicaldata' USING PigStorage(',') 
AS (ticker:chararray,date:int,open:double,high:double,low:double,close:double,volume:long);

-- Keep only the opening price field
project_open = FOREACH input_lines GENERATE ticker,date,open;

-- Filter the stock_prices to time periods: 20130801-20131031
stock_price = FILTER project_open BY date>=20130801 and date<=20131031;

-- Group the company by ticker
group_stock = GROUP stock_price BY ticker;


-- For each company, generate the moving average
movavg_stock = FOREACH group_stock GENERATE group AS ticker:chararray, MOVAVG(group,stock_price) AS mov_avg;


-- Filter the movavg_company to General Electric, IBM, Intel, Microsoft, Google and Apple
filter_movavg = FILTER movavg_stock BY (ticker=='GOOG') OR (ticker=='AAPL');
-- filter_movavg = FILTER movavg_stock BY (ticker=='GE') OR (ticker=='IBM')
-- OR (ticker=='MSFT') OR (ticker=='GOOG') OR (ticker=='AAPL');
 
-- Flatten filter result (optional)
flatten_movavg = FOREACH filter_movavg GENERATE FLATTEN(mov_avg);


--Store the result
STORE flatten_movavg INTO '/scr/yuz1988/lab7/exp2/output/';
