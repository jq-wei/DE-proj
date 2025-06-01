-- singular test

SELECT
    *
FROM
    {{ref('fct_orders')}}
WHERE
    item_discount_amount > 0  -- < 0   -- FAIL 1478523 