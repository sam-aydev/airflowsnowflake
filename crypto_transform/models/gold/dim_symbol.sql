SELECT
    -- Generate a unique Surrogate Key (INT) for the Fact table to use
    -- We use md5 hash of ticker + valid_from to ensure uniqueness for every version
    MD5(ticker || dbt_valid_from) as symbol_key,
    
    ticker,
    sector,
    risk_level,
    
    -- SCD2 Metadata
    dbt_valid_from as valid_from,
    COALESCE(dbt_valid_to, '9999-12-31'::timestamp) as valid_to,
    
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END as is_current

FROM {{ ref('dim_symbol_snapshot') }}