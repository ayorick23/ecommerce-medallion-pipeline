-- Pagos: llegan con la compra y no cambian (ADR 0018), así que solo se agregan
-- (ADR 0029). La marca de agua es la compra del pedido; todos los pagos de un
-- pedido entran juntos, por eso volver a correr el mismo D no duplica.
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        on_schema_change='fail'
    )
}}

select
    p.order_id,
    p.payment_sequential,
    c.cliente_sk,
    cast(o.order_purchase_timestamp as date) as fecha_compra,
    p.payment_type,
    p.payment_installments,
    p.payment_value,
    o.order_purchase_timestamp as _visible_desde
from {{ ref('stg_order_payments') }} as p
inner join {{ ref('stg_orders') }} as o on p.order_id = o.order_id
left join {{ ref('int_cliente_direcciones') }} as c on o.customer_id = c.customer_id
{% if is_incremental() %} where o.order_purchase_timestamp > {{ marca_de_agua() }} {% endif %}
