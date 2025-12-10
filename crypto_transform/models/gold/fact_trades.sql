{{ config(
    materialized='incremental',
    unique_key='fact_id'
) }}

SELECT
    -- Create a deterministic unique ID for the Fact
    MD5(s.trade_id || s.trade_time) as fact_id,
    
    -- Foreign Key to the Dimension (SCD2 Link)
    d.symbol_key,
    
    -- Fact Measures
    s.trade_time,
    s.price,
    s.quantity,
    (s.price * s.quantity) as total_amount_usd,
    
    -- Degenerate Dimensions (IDs usually left in Fact)
    s.trade_id,
    s.buyer_order_id,
    s.seller_order_id

FROM {{ ref('silver_transformations') }} s
-- JOIN Logic for SCD Type 2:
-- Match Ticker AND ensure Trade Time falls between the Dimension's validity window
LEFT JOIN {{ ref('dim_symbol') }} d 
    ON s.ticker = d.ticker 
    AND s.trade_time >= d.valid_from 
    AND s.trade_time < d.valid_to

{% if is_incremental() %}
WHERE s.trade_time > (SELECT MAX(trade_time) FROM {{ this }})
{% endif %}