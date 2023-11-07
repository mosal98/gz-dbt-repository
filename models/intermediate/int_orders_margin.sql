with joined_data as (
  select
    s.date_date,
    s.orders_id,
    s.products_id,
    s.revenue,
    s.quantity,
    p.purchase_price
  from {{ ref('stg_raw__sales') }} AS s
  join {{ ref('stg_raw__product') }} AS p
  USING (products_id)
)

select
  orders_id,
  date_date,
  ROUND(SUM(revenue), 1) as revenue,
  SUM(quantity) as quantity,
  ROUND(SUM(quantity * CAST(purchase_price AS FLOAT64)), 1) as purchase_cost,
  ROUND(SUM(revenue - (quantity * CAST(purchase_price AS FLOAT64))), 1) as margin
from joined_data
GROUP BY
orders_id,
date_date