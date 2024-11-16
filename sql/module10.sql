USE test;
-- module 10
-- lab 1

-- 1.1
CREATE TABLE rates_monthly (
	month Date,
	variable Decimal32(2),
	fixed Decimal32(2),
	bank Decimal32(2)
)
ENGINE = ReplacingMergeTree()
PRIMARY KEY month;

-- 1.2
INSERT INTO rates_monthly
    SELECT
        parseDateTime(date, '%d/%m/%Y') AS month,
        variable,
        fixed,
        bank
    FROM s3(
        'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/mortgage_rates.csv',
        'CSVWithNames');
        
-- 1.3       
SELECT *
FROM rates_monthly
-- 220 rows
;

-- 1.4
SELECT * 
FROM rates_monthly 
WHERE month = '2022-05-31';

-- 1.5
INSERT INTO rates_monthly
VALUES('2022-05-31', 3.2, 3.0, 1.1);

-- 1.6
SELECT * 
FROM rates_monthly 
WHERE month = '2022-05-31';

-- 1.7
SELECT * 
FROM rates_monthly FINAL
WHERE month = '2022-05-31'
;

-- 1.8
CREATE TABLE rates_monthly2 (
	month Date,
	variable Decimal32(2),
	fixed Decimal32(2),
	bank Decimal32(2),
	ver UInt32
)
ENGINE = ReplacingMergeTree(ver)
PRIMARY KEY month;

-- 1.9
INSERT INTO rates_monthly2(month, variable, fixed, bank, ver)
SELECT 
	month,
	variable,
	fixed,
	bank,
	1 as ver
FROM rates_monthly FINAL;

SELECT * 
FROM rates_monthly2; 

-- 1.10
INSERT INTO rates_monthly2 VALUES 
    ('2022-04-30', 3.1, 2.6, 1.1, 10);

INSERT INTO rates_monthly2 VALUES 
    ('2022-04-30', 2.9, 2.4, 0.9, 5);
    
-- 1.11   
SELECT * 
FROM rates_monthly2 FINAL
WHERE month = '2022-04-30';   

-- 1.12
OPTIMIZE TABLE rates_monthly2;

SELECT * 
FROM rates_monthly2
WHERE month = '2022-04-30';      

-- lab 2
-- 2.1
CREATE TABLE messages (
  id UInt32,
  day Date,
  message String,
  sign Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY id;

-- 2.2
INSERT INTO messages VALUES 
   (1, '2024-07-04', 'Hello', 1),
   (2, '2024-07-04', 'Hi', 1),
   (3, '2024-07-04', 'Bonjour', 1);
  
-- 2.3
SELECT * FROM messages;  

-- 2.4
INSERT INTO messages VALUES
  (2, '2024-07-05', 'Hi', -1),
  (2, '2024-07-05', 'Goodbye', 1);

SELECT * 
FROM messages
WHERE id = 2;   

SELECT * 
FROM messages FINAL
WHERE id = 2;   

-- 2.5
INSERT INTO messages VALUES
  (3, '2024-07-04', 'Bonjour', -1);

 
-- 2.6 
SELECT * FROM messages;  

-- 2.7
SELECT * FROM messages FINAL; 

-- 2.8
INSERT INTO messages VALUES 
   (1, '2024-07-03', 'Adios', 1);

SELECT * FROM messages FINAL;

-- quiz
SELECT * FROM system.mutations;
