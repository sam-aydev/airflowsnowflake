SELECT
    RECORD_CONTENT:ticker::string AS ticker,           
    RECORD_CONTENT:price::float AS price,             
    RECORD_CONTENT:quantity::float AS quantity,          
    TO_TIMESTAMP(RECORD_CONTENT:timestamp::int, 3) as Trade_Time, 
    RECORD_CONTENT:trade_id::int AS trade_id,            
    RECORD_CONTENT:buyer_order_id::int AS buyer_order_id,      
    RECORD_CONTENT:seller_order_id::int AS seller_order_id  
FROM streaming_db.public.stock_trades_raw