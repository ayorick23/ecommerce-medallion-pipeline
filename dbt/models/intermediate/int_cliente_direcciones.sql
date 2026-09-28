-- Versiones de dirección de cada persona (ADR 0030). Una fila por customer_id,
-- que en Olist es uno por pedido, con la versión a la que pertenece.
--
-- Se parte de los pedidos visibles en Silver(D) y no de customers sola, que es
-- una referencia sin enmascarar y conoce clientes de compras futuras.
with
    pedidos as (

        select
            c.customer_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix,
            c.customer_city,
            c.customer_state,
            o.order_purchase_timestamp
        from {{ ref('stg_orders') }} as o
        inner join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id

    ),

    marcados as (

        -- Se abre una versión cuando la dirección difiere de la del pedido
        -- anterior de la persona: volver a una dirección vieja es versión nueva.
        select
            *,
            coalesce(
                customer_zip_code_prefix is distinct from lag(customer_zip_code_prefix) over persona
                or customer_city is distinct from lag(customer_city) over persona
                or customer_state is distinct from lag(customer_state) over persona,
                true
            ) as abre_version
        from pedidos
        window
            persona as (
                partition by customer_unique_id order by order_purchase_timestamp, customer_id
            )

    ),

    numerados as (

        select
            *,
            sum(case when abre_version then 1 else 0 end) over (
                partition by customer_unique_id
                order by order_purchase_timestamp, customer_id
                rows between unbounded preceding and current row
            ) as numero_version
        from marcados

    ),

    versiones as (

        select
            *,
            min(order_purchase_timestamp) over (
                partition by customer_unique_id, numero_version
            ) as valid_from
        from numerados

    )

select
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id', 'valid_from']) }} as cliente_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    order_purchase_timestamp,
    numero_version,
    valid_from
from versiones
