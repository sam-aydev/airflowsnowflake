-- back compat for old kwarg name
  
  begin;
    
        
            
            
        
    

    

    merge into streaming_db.public.fact_trades as DBT_INTERNAL_DEST
        using streaming_db.public.fact_trades__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.fact_id = DBT_INTERNAL_DEST.fact_id
            )

    
    when matched then update set
        "FACT_ID" = DBT_INTERNAL_SOURCE."FACT_ID","SYMBOL_KEY" = DBT_INTERNAL_SOURCE."SYMBOL_KEY","TRADE_TIME" = DBT_INTERNAL_SOURCE."TRADE_TIME","PRICE" = DBT_INTERNAL_SOURCE."PRICE","QUANTITY" = DBT_INTERNAL_SOURCE."QUANTITY","TOTAL_AMOUNT_USD" = DBT_INTERNAL_SOURCE."TOTAL_AMOUNT_USD","TRADE_ID" = DBT_INTERNAL_SOURCE."TRADE_ID","BUYER_ORDER_ID" = DBT_INTERNAL_SOURCE."BUYER_ORDER_ID","SELLER_ORDER_ID" = DBT_INTERNAL_SOURCE."SELLER_ORDER_ID"
    

    when not matched then insert
        ("FACT_ID", "SYMBOL_KEY", "TRADE_TIME", "PRICE", "QUANTITY", "TOTAL_AMOUNT_USD", "TRADE_ID", "BUYER_ORDER_ID", "SELLER_ORDER_ID")
    values
        ("FACT_ID", "SYMBOL_KEY", "TRADE_TIME", "PRICE", "QUANTITY", "TOTAL_AMOUNT_USD", "TRADE_ID", "BUYER_ORDER_ID", "SELLER_ORDER_ID")

;
    commit;