USE test;
-- module 12
-- lab 1

-- 1.1
SELECT DISTINCT county
FROM uk_price_paid;

-- 1.2
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' 
  AND date < toDate('2024-01-01');
  
-- 1.3
ALTER TABLE uk_price_paid
  ADD INDEX county_index county
  TYPE SET(10)
  GRANULARITY 5;
  
-- 1.4
ALTER TABLE uk_price_paid 
MATERIALIZE INDEX county_index;

-- 1.5
SELECT *
FROM system.mutations;

-- 1.6
SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

-- 1.7
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' 
  AND date < toDate('2024-01-01');
  
-- 1.9
ALTER TABLE uk_price_paid
DROP INDEX county_index;

-- 1.10
ALTER TABLE uk_price_paid
  ADD INDEX county_index county
  TYPE SET(10)
  GRANULARITY 1;
  
-- 1.11
ALTER TABLE uk_price_paid 
MATERIALIZE INDEX county_index;

SELECT *
FROM system.mutations
ORDER BY create_time DESC;

-- 1.12
SELECT
    table,
    formatReadableSize(data_compressed_bytes) as data_compressed,
    formatReadableSize(secondary_indices_compressed_bytes) as index_compressed,
    formatReadableSize(primary_key_size) as primary_key
FROM
    system.parts
ORDER BY secondary_indices_uncompressed_bytes DESC
LIMIT 5;

-- 1.13
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' 
  AND date < toDate('2024-01-01');
  
-- 1.14
EXPLAIN indexes=1
SELECT
    formatReadableQuantity(count()),
    avg(price)
FROM uk_price_paid
WHERE county = 'GREATER LONDON' 
  AND date < toDate('2024-01-01'); 
  

-- lab 2
-- 2.1
 SELECT 
    toYear(date) AS year,
    count(),
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL'
GROUP BY year
ORDER BY year DESC;
-- 28.63 mln rows

-- 2.2
SELECT
    formatReadableSize(sum(bytes_on_disk)),
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;

-- 2.3
ALTER TABLE uk_price_paid
  ADD PROJECTION town_date_projection (
    SELECT town, date, price
    ORDER BY town, date
  );
  
-- 2.4
ALTER TABLE uk_price_paid
  MATERIALIZE PROJECTION town_date_projection;
  
-- 2.5
  SELECT 
    toYear(date) AS year,
    count(),
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL'
GROUP BY year
ORDER BY year DESC;
-- 311.30 thousand rows

-- 2.6
SELECT
    formatReadableSize(sum(bytes_on_disk)),
    count() AS num_of_parts
FROM system.parts
WHERE table = 'uk_price_paid' AND active = 1;
-- 350.97 mb

-- 2.7
ALTER TABLE uk_price_paid
  ADD PROJECTION handy_aggs_projection (
    SELECT avg(price), max(price), min(price)
    GROUP BY town
  );
  
-- 2.8
ALTER TABLE uk_price_paid
  MATERIALIZE PROJECTION handy_aggs_projection;
  
-- 2.9
 SELECT 
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL';
--  1,172 ???

-- 2.10
EXPLAIN
SELECT 
    avg(price),
    max(price),
    formatReadableQuantity(sum(price))
FROM uk_price_paid
WHERE town = 'LIVERPOOL';
