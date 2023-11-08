with join_tables as(
    select
        s.orders_id,
        s.date_date,
        s.revenue,
        s.quantity,
        s.purchase_cost,
        s.margin,
        sh.shipping_fee,
        sh.logcost,
        sh.ship_cost
    from {{ ref('int_orders_margin') }} AS s
    join {{ ref('stg_raw__ship') }} AS sh
    using (orders_id)
)

select
  orders_id,
  date_date,
  ROUND(SUM((margin + shipping_fee - logcost - CAST(ship_cost as FLOAT64))), 2) as operational_margin,
  SUM(quantity) as quantity,
  SUM(purchase_cost) as purchase_cost,
  SUM(margin) as margin,
from join_tables
GROUP BY
orders_id,
date_date