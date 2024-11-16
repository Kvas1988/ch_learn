USE test;
-- module 11
-- lab 1

-- 1.1
SELECT
    formatReadableSize(sum(data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio,
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;

-- 1.2 
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'uk_price_paid' AND active = 1
GROUP BY column;

-- 1.4
CREATE TABLE prices_1
(
    `price` UInt32,
    `date` Date,
    `postcode1` LowCardinality(String) ,
    `postcode2` LowCardinality(String),
    `type` Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    `is_new` UInt8,
    `duration` Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    `addr1` String,
    `addr2` String,
    `street` LowCardinality(String),
    `locality` LowCardinality(String),
    `town` LowCardinality(String),
    `district` LowCardinality(String),
    `county` LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, date)
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

INSERT INTO prices_1
    SELECT * FROM uk_price_paid;
    
-- 1.5
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_1' AND active = 1
GROUP BY column;   

-- 1.8
CREATE TABLE prices_2
(
    `price` UInt32 CODEC(T64, LZ4),
    `date` Date CODEC(DoubleDelta, ZSTD),
    `postcode1` String ,
    `postcode2` String,
    `is_new` UInt8 CODEC(LZ4HC)
)
ENGINE = MergeTree
PRIMARY KEY date
SETTINGS min_rows_for_wide_part=0,min_bytes_for_wide_part=0;

-- 1.9
INSERT INTO prices_2
    SELECT price, date, postcode1, postcode2, is_new 
    FROM uk_price_paid;
    
-- 1.10 
SELECT
    column,
    formatReadableSize(sum(column_data_uncompressed_bytes) AS u) AS uncompressed,
    formatReadableSize(sum(column_data_compressed_bytes) AS c) AS compressed,
    round(u / c, 2) AS compression_ratio
FROM system.parts_columns
WHERE table = 'prices_2' AND active = 1
GROUP BY column;      


-- lab 2
-- 2.1
CREATE TABLE ttl_demo (
  key UInt32,
  value String,
  timestamp DateTime
)
ENGINE = MergeTree
PRIMARY KEY (key)
TTL timestamp + INTERVAL 60 SECOND -- TTL = TimeToLive
--TTL now() + INTERVAL 60 SECOND -- doesn't work in v24.0+: ttl must depends on column
--SETTINGS compatibility = '23.11' --allow_suspicious_ttl_expressions = 1
;
--ALTER TABLE ttl_demo
--MODIFY TTL timestamp + INTERVAL 60 SECOND;
--;

-- 2.2
INSERT INTO ttl_demo VALUES 
    (1, 'row1', now()),
    (2, 'row2', now());
   
-- 2.3
SELECT * FROM ttl_demo;

-- 2.5
ALTER TABLE ttl_demo MATERIALIZE TTL;

-- 2.7
-- Clean only 'value' column
ALTER TABLE ttl_demo
  MODIFY COLUMN 
  value String TTL timestamp + INTERVAL 15 SECOND;

 -- 2.8
INSERT INTO ttl_demo VALUES 
    (1, 'row1', now()),
    (2, 'row2', now());
   
-- 2.9
ALTER TABLE ttl_demo
MATERIALIZE TTL;   

-- 2.10
SELECT * FROM ttl_demo;

-- 2.12
SELECT *
FROM prices_1;

ALTER TABLE prices_1
MODIFY TTL date + INTERVAL 5 YEAR;

-- 2.13
ALTER TABLE prices_1
MATERIALIZE TTL;   

-- 2.13
SELECT *
FROM prices_1
WHERE date < now() - INTERVAL 5 YEAR
;
