{% snapshot dim_symbol_snapshot %}

{{
    config(
      target_schema='public',
      unique_key='ticker',
      strategy='check',
      check_cols=['sector', 'risk_level']
    )
}}

SELECT * FROM {{ ref('crypto_metadata') }}

{% endsnapshot %}