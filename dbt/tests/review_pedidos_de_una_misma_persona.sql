-- Todos los pedidos de una review son de la misma persona (ADR 0034): es lo
-- que permite que fct_reviews tenga un único cliente_sk. Devuelve las reviews
-- que no cumplen.
select r.review_id
from {{ ref('stg_order_reviews') }} as r
inner join {{ ref('stg_orders') }} as o on r.order_id = o.order_id
inner join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id
group by r.review_id
having count(distinct c.customer_unique_id) > 1
