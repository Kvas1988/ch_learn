DESCRIBE URL('https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/clickhouse_hacker_news.csv');

CREATE table hackernews (
  id Int64,
  deleted Int64,
  type String,
  by String,
  time DateTime64,
  text String,
  dead Int64,
  parent Int64,
  poll Int64,
  kids Array(Int64),
  url String,
  score Int64,
  titile String,
  parts String,
  descendants Int64
)
ENGINE=MergeTree 
PRIMARY KEY(type, time);

INSERT INTO hackernews
SELECT * 
FROM url('https://datasets-documentation.s3.eu-west-3.amazonaws.com/hackernews/clickhouse_hacker_news.csv');


-- lab 2

DESCRIBE s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/uk_property_prices.snappy.parquet');

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

SELECT avg(price) price
FROM uk_price_paid
WHERE postcode1='LU1'
  AND postcode2='5FT';

--  73461.87334593573
-- withoud LU1 - 229927.6735011679

SELECT avg(price) price
FROM uk_price_paid
WHERE town='YORK';
-- 208573.36750813763

-- lab 3
SET format_csv_delimiter = '~';
select count() from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');

select sum(actual_amount) actual_amount 
from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');
-- 8163564603.14

select sum(approved_amount) approved_amount 
from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');
-- column is String

describe s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');

select sum(toUInt32OrZero(approved_amount)) app,
       sum(toUInt32OrZero(recommended_amount)) rec,
       sum(toUInt32OrZero(approved_amount) + toUInt32OrZero(recommended_amount)) app_rec
from s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv');

--   ┌─────────app─┬────────rec─┬─────app_rec─┐
--1. │ 10011902489 │ 9983205735 │ 19995108224 │


SELECT 
    formatReadableQuantity(sum(approved_amount)),
    formatReadableQuantity(sum(recommended_amount))
FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~',
schema_inference_hints='approved_amount UInt32, recommended_amount UInt32';
-- can't parse it properly

CREATE TABLE operating_budget (
  fiscal_year LowCardinality(String),
  service LowCardinality(String),
  department LowCardinality(String),
  program LowCardinality(String),
  item_category LowCardinality(String),
  fund LowCardinality(String),
  description String,
  approved_amount UInt32,
  recommended_amount UInt32,
  program_code LowCardinality(String),
  actual_amount Decimal(12, 2),
  fund_type Enum('GENERAL FUNDS' = 1, 'FEDERAL FUNDS' = 2, 'OTHER FUNDS' = 3),
)
ENGINE = MergeTree
PRIMARY KEY(fiscal_year, program);


--SET input_format_csv_skip_first_lines = 1;
INSERT INTO operating_budget
SELECT fiscal_year,
  service,
  department,
  splitByString(' (', cast(program, 'String'))[1] as program_,
  item_category,
  fund,
  description,
  toUInt32OrZero(approved_amount) as approved_amount,
  toUInt32OrZero(recommended_amount) as recommended_amount,
  extract(program, '\d+') as program_code,
  actual_amount,
  fund_type

FROM s3('https://learn-clickhouse.s3.us-east-2.amazonaws.com/operating_budget.csv')
SETTINGS 
format_csv_delimiter='~';

SELECT sum(approved_amount)
FROM operating_budget
WHERE fiscal_year='2022';
--  5086410509

SELECT sum(actual_amount)
FROM operating_budget
WHERE program_code='031'
  AND fiscal_year='2022';
-- 8058173.43
