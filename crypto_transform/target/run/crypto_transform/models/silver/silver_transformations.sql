-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into streaming_db.public.silver_transformations as DBT_INTERNAL_DEST
        using streaming_db.public.silver_transformations__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.trade_id = DBT_INTERNAL_DEST.trade_id
            )

    
    when matched then update set
        "TICKER" = DBT_INTERNAL_SOURCE."TICKER","PRICE" = DBT_INTERNAL_SOURCE."PRICE","QUANTITY" = DBT_INTERNAL_SOURCE."QUANTITY","TRADE_VALUE_USD" = DBT_INTERNAL_SOURCE."TRADE_VALUE_USD","TRADE_TIME" = DBT_INTERNAL_SOURCE."TRADE_TIME","TRADE_ID" = DBT_INTERNAL_SOURCE."TRADE_ID","BUYER_ORDER_ID" = DBT_INTERNAL_SOURCE."BUYER_ORDER_ID","SELLER_ORDER_ID" = DBT_INTERNAL_SOURCE."SELLER_ORDER_ID","TRADE_CATEGORY" = DBT_INTERNAL_SOURCE."TRADE_CATEGORY","DBT_UPDATED_AT" = DBT_INTERNAL_SOURCE."DBT_UPDATED_AT"
    

    when not matched then insert
        ("TICKER", "PRICE", "QUANTITY", "TRADE_VALUE_USD", "TRADE_TIME", "TRADE_ID", "BUYER_ORDER_ID", "SELLER_ORDER_ID", "TRADE_CATEGORY", "DBT_UPDATED_AT")
    values
        ("TICKER", "PRICE", "QUANTITY", "TRADE_VALUE_USD", "TRADE_TIME", "TRADE_ID", "BUYER_ORDER_ID", "SELLER_ORDER_ID", "TRADE_CATEGORY", "DBT_UPDATED_AT")

;
    commit;