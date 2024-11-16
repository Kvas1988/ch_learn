USE test;
-- module 9
-- 1
SELECT *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv');

-- 2
CREATE DICTIONARY test.uk_mortgage_rates (
  date DateTime64,
  variable Decimal32(2),
  fixed Decimal32(2),
  bank Decimal32(2)
)
PRIMARY KEY date
SOURCE(
	HTTP(
	 	url 'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv'
	 	format 'CSV'
	)
)
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(2628000000)
SETTINGS (date_time_input_format = 'best_effort');

--DROP DICTIONARY default.uk_mortgage_rates; 

-- 3
SELECT *
FROM uk_mortgage_rates
ORDER BY date DESC;
-- 220

-- 4
SELECT
  toLastDayOfMonth(date) md,
  count() cnt,
  dictGet('uk_mortgage_rates', 'variable', md) AS variable
FROM uk_price_paid
WHERE variable > 0
GROUP BY md
ORDER BY md DESC;

-- 5
SELECT
  toLastDayOfMonth(date) md,
  count() cnt,
  dictGet('uk_mortgage_rates', 'variable', md) AS variable
FROM uk_price_paid
WHERE variable > 0
GROUP BY md
ORDER BY cnt DESC;

-- 6
WITH q AS (
	SELECT
	  toLastDayOfMonth(date) md,
	  count() cnt,
	  dictGet('uk_mortgage_rates', 'variable', md) AS variable
	FROM uk_price_paid
	WHERE variable > 0
	GROUP BY md
)
SELECT corr(CAST(cnt, 'Float32'), CAST(variable, 'Float32'))
FROM q
-- 0.29808125
;

-- 6
WITH q AS (
	SELECT
	  toLastDayOfMonth(date) md,
	  count() cnt,
	  dictGet('uk_mortgage_rates', 'fixed', md) AS fixed
	FROM uk_price_paid
	WHERE fixed > 0
	GROUP BY md
)
SELECT corr(CAST(cnt, 'Float32'), CAST(fixed, 'Float32'))
FROM q
-- -0.2543798
;

