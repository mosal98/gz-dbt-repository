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
  date_date,
  orders_id,
  products_id,
  revenue,
  quantity,
  purchase_price,
  quantity * CAST(purchase_price AS FLOAT64) as purchase_cost,
  ROUND(revenue - (quantity * CAST(purchase_price AS FLOAT64)), 1) as margin
from joined_data 