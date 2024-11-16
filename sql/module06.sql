-- lab 6.1
CREATE VIEW london_properties_view AS
SELECT date,
	   price,
	   addr1,
	   addr2,
	   street
FROM uk_price_paid
WHERE town = 'LONDON';

SELECT avg(price) p
FROM london_properties_view
WHERE toYear(date) = 2022
-- 1010696.6690683541
;

SELECT count()
FROM london_properties_view
-- 2188031
;

SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';


EXPLAIN SELECT count() 
FROM london_properties_view;

EXPLAIN SELECT count() 
FROM uk_price_paid
WHERE town = 'LONDON';


CREATE VIEW properties_by_town_view AS
SELECT date,
	   price,
	   addr1,
	   addr2,
	   street
FROM uk_price_paid
WHERE town={town_filter:String};

DESCRIBE uk_price_paid;

SELECT max(price) p,
	   argMax(street, price)
FROM properties_by_town_view(town_filter='LIVERPOOL')
-- 300_000_000	SEFTON STREET
; 


-- lab 6.2
SELECT count() n,
	   avg(price) p
FROM uk_price_paid
WHERE toYear(date) = 2020
-- 886642	378060.000030452
;

SELECT toYear(date) dt,
	   count() n,
	   avg(price) p
FROM uk_price_paid
GROUP BY toYear(date);


CREATE OR REPLACE TABLE prices_by_year_dest (
  date Date,
  price UInt32,
  addr1 String,
  addr2 String,
  street String,
  town LowCardinality(String),
  district LowCardinality(String),
  county LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYear(date)
PRIMARY KEY(town, date);

--TRUNCATE TABLE prices_by_year_dest; -- delete all rows

CREATE MATERIALIZED VIEW prices_by_year_view 
TO prices_by_year_dest
AS
SELECT date,
	   price,
	   addr1,
	   addr2,
	   street,
	   town,
	   district,
	   county
FROM uk_price_paid;


INSERT INTO prices_by_year_dest
SELECT date,
	   price,
	   addr1,
	   addr2,
	   street,
	   town,
	   district,
	   county
FROM uk_price_paid;	   

SELECT count()
FROM prices_by_year_dest;
-- 7
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';
-- 8
SELECT * FROM system.parts
WHERE table='uk_price_paid';

-- 10
SELECT count() n,
	   avg(price) p
FROM prices_by_year_dest
WHERE toYear(date) = 2020
-- 886642	378060.000030452
;

-- 11
SELECT count(),
		max(price) max_p,
		avg(price) avg_p,
		quantile(0.9)(price) q90_p
FROM prices_by_year_dest
WHERE toYYYYMM(date) = 200506
 AND county = upper('Staffordshire')

--count(): 1322
--max_p:   745000
--avg_p:   160241.94402420576
--q90_p:   269670.00000000006
;

-- 12
INSERT INTO uk_price_paid VALUES
    (125000, '2024-03-07', 'B77', '4JT', 'semi-detached', 0, 'freehold', 10,'',	'CRIGDON','WILNECOTE','TAMWORTH','TAMWORTH','STAFFORDSHIRE'),
    (440000000, '2024-07-29', 'WC1B', '4JB', 'other', 0, 'freehold', 'VICTORIA HOUSE', '', 'SOUTHAMPTON ROW', '','LONDON','CAMDEN', 'GREATER LONDON'),
    (2000000, '2024-01-22','BS40', '5QL', 'detached', 0, 'freehold', 'WEBBSBROOK HOUSE','', 'SILVER STREET', 'WRINGTON', 'BRISTOL', 'NORTH SOMERSET', 'NORTH SOMERSET');

-- 13
SELECT *
FROM prices_by_year_dest
WHERE toYear(date) = 2024;

-- 14
SELECT * FROM system.parts
WHERE table='prices_by_year_dest';
