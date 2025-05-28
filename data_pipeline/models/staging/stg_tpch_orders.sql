-- this is our staging model

select * from {{ source('tpch', 'orders') }}