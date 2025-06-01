SELECT 
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
from 
    {{ref('stg_tpch_orders')}} as orders  -- here ref() takes the output of stg_tpch_orders as a table
JOIN
    {{ref('int_order_items_summary')}} as order_item_summary
        on orders.order_key = order_item_summary.order_key
ORDER BY order_date