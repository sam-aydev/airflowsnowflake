{{ config(
    materialized='incremental',     
    unique_key='trade_id',          
    incremental_strategy='merge'    
) }}

WITH source_data AS (
    SELECT * FROM {{ ref('bronze_data') }} 
),

deduplicated_data AS (
    SELECT 
        *,

        ROW_NUMBER() OVER (PARTITION BY trade_id ORDER BY Trade_Time DESC) as row_num
    FROM source_data
    WHERE 
        -- Data Quality Checks (Transformations):
        price > 0                   -- Filter out bad prints
        AND quantity > 0            -- Filter out system errors
        AND ticker IS NOT NULL      -- Filter out corrupted messages
        
        {% if is_incremental() %}
        -- INCREMENTAL LOGIC (The most important part):
        -- "Only give me rows that are NEWER than the max time currently in this table"
        AND Trade_Time > (SELECT MAX(Trade_Time) FROM {{ this }})
        {% endif %}
)

SELECT
    ticker,
    price,
    quantity,
    -- Transformation: Calculate total transaction value
    (price * quantity) as trade_value_usd, 
    Trade_Time,
    trade_id,
    buyer_order_id,
    seller_order_id,
    -- Transformation: Tag High Value Trades for analytics
    CASE 
        WHEN (price * quantity) > 10000 THEN 'WHALE' 
        ELSE 'RETAIL' 
    END as trade_category,
    current_timestamp() as dbt_updated_at
FROM deduplicated_data
WHERE row_num = 1 