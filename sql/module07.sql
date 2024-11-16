-- solutions
-- https://github.com/ClickHouse/clickhouse-academy/blob/main/developer/07_aggregations_in_mvs/lab_7.1.sql


-- pre lab 7
USE test;

create table uk_price_paid (
  price UInt32,
  date Date,
  postcode1 LowCardinality(String),
  postcode2 LowCardinality(String),
  type Enum('terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4, 'other' = 0),
  is_new UInt8,
  duration Enum('freehold' = 1, 'leasehold' = 2, 'unknown' = 0),
  addr1 String,
  addr2 String,
  street String,
  locality LowCardinality(String),
  town LowCardinality(String),
  district LowCardinality(String),
  county LowCardinality(String)
)
engine = MergeTree
PRIMARY KEY(postcode1, postcode2, date);


INSERT INTO uk_price_paid
SELECT *
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');
;

--TRUNCATE uk_price_paid;

SELECT count()
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet')
-- 28_634_236
;

SELECT count()
FROM uk_price_paid
-- 28_634_236
;

SELECT *
FROM uk_price_paid
;


-- lab 1
-- 1.1
SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
GROUP BY town
ORDER BY sum_price DESC;

-- 1.2
-- CREATE TARGET TABLE
-- CREATE MVIEW
-- INSERT DATA IN TARGET TABLE

CREATE TABLE prices_sum_dest (
  town LowCardinality(String),
  price UInt64 -- NOT 32
  --sum_price AggregateFunction(sum, UInt32)
)
ENGINE = SummingMergeTree
PRIMARY KEY(town);

TRUNCATE TABLE prices_sum_dest;

SELECT * 
FROM prices_sum_dest
WHERE town = 'LONDON';

CREATE MATERIALIZED VIEW prices_sum_view
TO prices_sum_dest
AS
SELECT town,
	   SUM(price) AS price
FROM uk_price_paid
GROUP BY town
;	

SELECT * FROM prices_sum_view;

INSERT INTO prices_sum_dest
SELECT 
	town,
	SUM(price) AS price
	--price
FROM uk_price_paid
GROUP BY town
;

OPTIMIZE TABLE prices_sum_dest;

-- 1.3
SELECT count()
FROM prices_sum_dest
-- 1172
;

SELECT *
FROM prices_sum_dest;
;

DROP TABLE prices_sum_view;
DROP TABLE prices_sum_dest;

-- 1.4
SELECT 
    town,
    sum(price) AS sum_price,
    formatReadableQuantity(sum_price)
FROM uk_price_paid
WHERE town = 'LONDON'
GROUP BY town;

SELECT
    town,
    price AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON';

------------------------------------------
-- right query. need sum and group by in case there are multiple rows in table
SELECT
    town,
    sum(price) AS sum,
    formatReadableQuantity(sum)
FROM prices_sum_dest
WHERE town = 'LONDON'
GROUP BY town;


DESCRIBE uk_price_paid;
------------------------------------------

-- insert and re-run above
INSERT INTO uk_price_paid (price, date, town, street)
VALUES
    (4294967295, toDate('2024-01-01'), 'LONDON', 'My Street1');
   
SELECT *
FROM uk_price_paid
WHERE street = 'My Street1';

-- 1.5
SELECT 
	town,
	SUM(sum_price) AS sum	
FROM prices_sum_dest
GROUP BY town
ORDER BY sum DESC
LIMIT 10;

SELECT 
	town,
	SUM(price) AS sum	
FROM uk_price_paid
GROUP BY town
ORDER BY sum DESC
LIMIT 10;

SELECT *
FROM prices_sum_dest;

SELECT count(DISTINCT town)
FROM uk_price_paid;

-- lab 2
-- 2.1
WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    min(price) AS min_price,
    max(price) AS max_price
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    avg(price)
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

WITH
    toStartOfMonth(date) AS month
SELECT 
    month,
    count()
FROM uk_price_paid
GROUP BY month 
ORDER BY month DESC;

-- 2.2
CREATE TABLE uk_prices_aggs_dest (
  month Date,
  min_price SimpleAggregateFunction(min, UInt32),
  max_price SimpleAggregateFunction(max, UInt32),
  avg_price AggregateFunction(avg, UInt32),
  cnt AggregateFunction(count, UInt32)
)
ENGINE = AggregatingMergeTree
PRIMARY KEY(month);

DROP TABLE uk_prices_aggs_dest;
DROP TABLE uk_prices_aggs_view;


CREATE MATERIALIZED VIEW uk_prices_aggs_view 
TO uk_prices_aggs_dest
AS
WITH
  toStartOfMonth(date) AS month
SELECT
  month,
  minSimpleState(price) AS min_price,
  maxSimpleState(price) AS max_price,
  avgState(price) AS avg_price,
  countState(*) AS cnt
FROM uk_price_paid
GROUP BY month;


INSERT INTO uk_prices_aggs_dest
WITH
  toStartOfMonth(date) AS MONTH
SELECT 
  MONTH,
  minSimpleState(price) min_price,
  maxSimpleState(price) max_price,
  avgState(price) avg_price,
  countState(*) AS cnt
FROM uk_price_paid
WHERE date < toDate('2024-01-01')
GROUP BY MONTH;

DESCRIBE uk_prices_aggs_dest;


-- 2.3 - DOESN'T WORK
--SELECT * FROM uk_prices_aggs_dest;

SELECT 
	month,
	min(min_price),
	max(max_price),
	avgMerge(avg_price),
	count(*) cnt
FROM uk_prices_aggs_dest
GROUP BY month;

-- 2.4
SELECT 
	month,
	min(min_price),
	max(max_price)
FROM uk_prices_aggs_dest
WHERE toYear(month) = 2023
GROUP BY month;
--2023-01-01  100  140000000

SELECT 
	min(price),
	max(price)
FROM uk_price_paid
WHERE toYYYYMM(date) = 202301;
-- correct

-- 2.5
SELECT 	
	avgMerge(avg_price)
FROM uk_prices_aggs_dest
WHERE toYear(month) >= 2022
-- 395060.925136278
;

-- 2.6
SELECT 	
	countMerge(cnt)
FROM uk_prices_aggs_dest
WHERE toYear(month) = 2020
-- 886642
;

SELECT count() cnt
FROM uk_price_paid
WHERE toYear(date) = 2020
-- correct
;

-- 2.7
INSERT INTO uk_price_paid (date, price, town) VALUES
    ('2024-08-01', 10000, 'Little Whinging'),
    ('2024-08-01', 1, 'Little Whinging');
    
SELECT *
FROM uk_price_paid
WHERE toYear(date) = 2024;

-- 2.8
SELECT 
    month,
    countMerge(cnt),
    min(min_price),
    max(max_price)
FROM uk_prices_aggs_dest
WHERE toYYYYMM(month) = '202408'
GROUP BY month;
