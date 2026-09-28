-- fct_pedidos y fct_pagos apuntan a la versión de dim_cliente vigente al
-- instante de la compra, y de la misma persona (ADR 0030). Devuelve las filas
-- que no cumplen.
with
    hechos as (

        select 'fct_pedidos' as hecho, order_id, cliente_sk
        from {{ ref('fct_pedidos') }}
        union all
        select 'fct_pagos' as hecho, order_id, cliente_sk
        from {{ ref('fct_pagos') }}

    )

select h.hecho, h.order_id, h.cliente_sk
from hechos as h
inner join {{ ref('stg_orders') }} as o on h.order_id = o.order_id
inner join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id
left join {{ ref('dim_cliente') }} as v on h.cliente_sk = v.cliente_sk
where
    v.cliente_sk is null
    or v.customer_unique_id <> c.customer_unique_id
    or o.order_purchase_timestamp < v.valid_from
    or o.order_purchase_timestamp >= v.valid_to
