SELECT *
FROM uk_price_paid;

-- 1.1
SELECT *
FROM uk_price_paid
WHERE price > 100_000_000
ORDER BY price DESC;

-- 1.2
SELECT count()
FROM uk_price_paid
WHERE price >= 1_000_000
  AND toYear(date)=2022 
  -- 37576
;

-- 1.3 
select uniq(town)
FROM uk_price_paid
-- 1172
;

-- 1.4
SELECT town,
	count() as c
FROM uk_price_paid
GROUP BY town
ORDER BY c DESC
-- LONDON
;

-- 1.5
SELECT arrayJoin(topK(10)(town))
FROM uk_price_paid
WHERE town != 'LONDON'
;

-- 1.6
SELECT town,
	avg(price) as p
FROM uk_price_paid
GROUP BY town
ORDER BY p DESC
;

-- 1.7
SELECT addr1,
	addr2,
	street,
	town,
	price
FROM uk_price_paid
--WHERE price = (SELECT max(price) FROM uk_price_paid);
ORDER BY price DESC
LIMIT 5
-- 55	UNIT 53	BAKER STREET	LONDON	594300000
;

-- 1.8
SELECT type,
	avg(price)
FROM uk_price_paid
GROUP BY type
;

-- 1.9
SELECT sum(price) p
FROM uk_price_paid
WHERE county in ('AVON', 'ESSEX', 'DEVON', 'KENT', 'CORNWALL')
  AND toYear(date) = 2020
--GROUP BY county
-- 29935920858
;  

-- 1.10
SELECT avg(price)
FROM uk_price_paid
WHERE toYear(date) BETWEEN 2005 AND 2010
-- 210982.00559183
;

-- 1.11
SELECT date,
	count()
FROM uk_price_paid
WHERE town = 'LIVERPOOL'
  AND toYear(date)=2020
GROUP BY date
ORDER BY date;

-- 1.12
SELECT DISTINCT town,
	max(price) OVER (PARTITION BY town) as tp,
	max(price) OVER () as mp,
	tp / mp as v
FROM uk_price_paid
ORDER BY v DESC;

SELECT toYYYYMM(toDate('2024-04-15'))
-- 202404
