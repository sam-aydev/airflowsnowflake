-- 1. Create the Database and Table
CREATE DATABASE IF NOT EXISTS streaming_db;
CREATE SCHEMA IF NOT EXISTS streaming_db.public;


USE ROLE SECURITYADMIN;

-- Allow the robot to create tables in the Public schema
GRANT CREATE TABLE ON SCHEMA streaming_db.public TO ROLE kafka_role;


-- Just to be safe, ensure it can create stages/pipes if needed later
GRANT CREATE STAGE ON SCHEMA streaming_db.public TO ROLE kafka_role;
GRANT CREATE PIPE ON SCHEMA streaming_db.public TO ROLE kafka_role;



-- We use VARIANT to store the raw JSON exactly as it comes from Binance
CREATE TABLE IF NOT EXISTS streaming_db.public.stock_trades_raw (
    record_content VARIANT
);

-- 2. Create the User and Role for the Connector
CREATE OR REPLACE USER kafka_user PASSWORD = 'StrongPassword123';
CREATE ROLE IF NOT EXISTS kafka_role;

-- 3. Grant Permissions (Crucial!)
GRANT ROLE kafka_role TO USER kafka_user;
GRANT ROLE kafka_role TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE streaming_db TO ROLE kafka_role;
GRANT USAGE ON SCHEMA streaming_db.public TO ROLE kafka_role;
GRANT INSERT ON TABLE streaming_db.public.stock_trades_raw TO ROLE kafka_role;



ALTER USER kafka_user SET RSA_PUBLIC_KEY='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAqTSGhrI/VlmUApuzpfE6
ySzezKOGp0aHoVmkNuHT/c/qmL+d3t38pXEVQFakr0ECuSSjyq2tYKNuwaNOTzXz
jyrtS9Xq26G3v5N6BVbtsHUBR8fOKN1YaevGDEParo9+tnAF6kGdoP8WTJFBYtOT
Su6uBMbVdZrY8E5Vy3+bn/EC1+7UWFbjNmuuLBAGD9laHqF09a7YK7A+lJH3iYN4
Q6Vtlyn2PAnsdXnkTKQjuDsfzoQcnRDWci44oZpisNEq/0a+QFOecpEzGNZTCTe0
Z5RgmCM68+p2j30xRmHAXHQ0ZepN7LaIH/62wGLFmy36HOL3gxhs8aJ2ny9ALhZc
fwIDAQAB';





SELECT * FROM streaming_db.public.stock_trades_raw;

DROP TABLE streaming_db.public.stock_trades_raw;

SHOW TABLES IN SCHEMA streaming_db.public;




SELECT 
    -- We use the colon syntax (:) to reach inside the JSON blob
    RECORD_CONTENT:ticker::string as Ticker,
    RECORD_CONTENT:price::float as Price,
    RECORD_CONTENT:quantity::float as Size,
    -- Convert the millisecond timestamp to a readable date
    TO_TIMESTAMP_NTZ(RECORD_CONTENT:timestamp::int, 3) as Trade_Time
FROM STOCK_TRADES_RAW
ORDER BY Trade_Time DESC
LIMIT 50;

SELECT * FROM STOCK_TRADES_RAW

SELECT * FROM FACT_TRADES

SELECT 
    s.trade_time AS trade_happened_at,
    d.valid_from AS dimension_started_at,
    d.valid_to AS dimension_ended_at,
    s.ticker,
    d.ticker AS dim_ticker
FROM STREAMING_DB.PUBLIC.SILVER_TRANSFORMATIONS s
-- We use a simpler JOIN just on Name to see the dates
LEFT JOIN STREAMING_DB.PUBLIC.DIM_SYMBOL d 
    ON s.ticker = d.ticker
ORDER BY s.trade_time DESC
LIMIT 10;

-- Force the Dimension to be valid from the past ('1970-01-01')
-- This ensures ALL historical trades will match this row.
UPDATE STREAMING_DB.PUBLIC.DIM_SYMBOL_SNAPSHOT
SET dbt_valid_from = '1970-01-01'
WHERE dbt_valid_from IS NOT NULL;


-- 1. Switch to a powerful role (Security Admin or Account Admin)
USE ROLE SECURITYADMIN;

-- 2. Grant permissions to create Views and Tables in the Public schema
GRANT USAGE ON DATABASE streaming_db TO ROLE kafka_role;
GRANT USAGE ON SCHEMA streaming_db.public TO ROLE kafka_role;

-- CRITICAL: This allows dbt to create "Views" (BRONZE Layer)
GRANT CREATE VIEW ON SCHEMA streaming_db.public TO ROLE kafka_role;

-- CRITICAL: This allows dbt to create "Tables" (Gold and silver Layer)
GRANT CREATE TABLE ON SCHEMA streaming_db.public TO ROLE kafka_role;

-- OPTIONAL: Allow it to clear out old stuff
GRANT MODIFY ON SCHEMA streaming_db.public TO ROLE kafka_role;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE kafka_role;

ALTER USER kafka_user SET DEFAULT_WAREHOUSE = COMPUTE_WH;

GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE kafka_role;