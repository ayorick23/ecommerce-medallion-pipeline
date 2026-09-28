-- fct_reviews apunta a la versión vigente a la creación de la review o, si la
-- review es anterior a la primera versión de la persona (review adelantada,
-- ADR 0020), a la primera (ADR 0034). Devuelve las filas que no cumplen.
with
    reviews as (

        select
            f.review_id,
            f.cliente_sk,
            any_value(r.review_creation_date) as review_creation_date,
            any_value(c.customer_unique_id) as customer_unique_id
        from {{ ref('fct_reviews') }} as f
        inner join {{ ref('stg_order_reviews') }} as r on f.review_id = r.review_id
        inner join {{ ref('stg_orders') }} as o on r.order_id = o.order_id
        inner join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id
        group by f.review_id, f.cliente_sk

    ),

    primeras as (

        select customer_unique_id, min(valid_from) as primer_valid_from
        from {{ ref('dim_cliente') }}
        group by customer_unique_id

    )

select r.review_id, r.cliente_sk
from reviews as r
inner join primeras as p on r.customer_unique_id = p.customer_unique_id
left join {{ ref('dim_cliente') }} as v on r.cliente_sk = v.cliente_sk
where
    v.cliente_sk is null
    or v.customer_unique_id <> r.customer_unique_id
    or (
        -- review adelantada: debe apuntar a la primera versión
        r.review_creation_date < p.primer_valid_from and v.valid_from <> p.primer_valid_from
    )
    or (
        -- caso normal: la versión vigente a la creación
        r.review_creation_date >= p.primer_valid_from
        and (r.review_creation_date < v.valid_from or r.review_creation_date >= v.valid_to)
    )
