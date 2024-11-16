SELECT uniqExact(COUNTRY_CODE)
FROM pypi;

CREATE TABLE pypi3 (
	TIMESTAMP DateTime64,
	COUNTRY_CODE LowCardinality(String),
	URL String,
	PROJECT LowCardinality(String)
)

ENGINE = MergeTree()
PRIMARY KEY (PROJECT, TIMESTAMP)
;

INSERT INTO pypi3
SELECT * FROM pypi;


SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%')
GROUP BY table;


SELECT
    toStartOfMonth(TIMESTAMP) AS month,
    count() AS count
FROM pypi2
WHERE COUNTRY_CODE = 'US'
GROUP BY
    month
ORDER BY
    month ASC,
    count DESC;


-- lab 2    

DESCRIBE s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

CREATE TABLE crypto_prices (
  trade_date Date,
  crypto_name LowCardinality(String),
  volume Float32,
  price Float32,
  market_cap Float32,
  change_1_day Float32
)

ENGINE = MergeTree
PRIMARY KEY(crypto_name, trade_date);


INSERT INTO crypto_prices 
SELECT * 
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

SELECT count() FROM crypto_prices;

SELECT count() FROM crypto_prices WHERE volume >= 1000000;
-- 311 090

SELECT avg(price) FROM crypto_prices WHERE crypto_name='Bitcoin';
-- 5631.871522324464

SELECT avg(price) FROM crypto_prices WHERE crypto_name LIKE 'B%';
-- 72.04511933246548
