-- this is our staging model

---- with the following line, `dbt run` looks up tpch.orders in tpch_sources.yml.
---- Then translates to `snowflake_sample_data.tpch_sf1.orders`
---- Create a view named as stg_tpch_orders  (+materialized: view)
---- With also the 
select 
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as status_code,
    o_totalprice as total_price,
    o_orderdate as order_date
from 
    {{ source('tpch', 'orders') }}  