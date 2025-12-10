{{ config(materialized='view') }}

SELECT
    -- 1. Fact Data (Measures)
    f.trade_time,
    f.price,
    f.quantity,
    f.total_amount_usd,
    
    -- 2. Dimension Data (Context)
    -- We act as if these columns always belonged to the trade
    d.sector,
    d.risk_level,
    
    -- 3. Metadata (Optional)
    f.fact_id,
    f.buyer_order_id,
    f.seller_order_id

FROM {{ ref('fact_trades') }} f
-- The Join is already done HERE so users don't have to do it
LEFT JOIN {{ ref('dim_symbol') }} d 
    ON f.symbol_key = d.symbol_key