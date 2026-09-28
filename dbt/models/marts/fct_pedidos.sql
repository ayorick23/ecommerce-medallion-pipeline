-- Accumulating snapshot por línea de pedido (ADR 0033), incremental con merge
-- (ADR 0029): en cada corrida se reprocesan las líneas de los pedidos cuyo
-- último evento visible es posterior a la marca de agua ya cargada.
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id', 'order_item_id'],
        on_schema_change='fail'
    )
}}

with
    pedidos as (

        select
            o.order_id,
            o.customer_id,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,
            e.estado,
            e.ultimo_evento_at
        from {{ ref('stg_orders') }} as o
        inner join {{ ref('int_pedido_estado') }} as e on o.order_id = e.order_id
        {% if is_incremental() %}
            where e.ultimo_evento_at > (select max(_visible_desde) from {{ this }})
        {% endif %}

    )

select
    i.order_id,
    i.order_item_id,
    c.cliente_sk,
    i.product_id,
    i.seller_id,
    p.estado,
    cast(p.order_purchase_timestamp as date) as fecha_compra,
    cast(p.order_approved_at as date) as fecha_aprobacion,
    cast(p.order_delivered_carrier_date as date) as fecha_despacho,
    cast(p.order_delivered_customer_date as date) as fecha_entrega,
    cast(p.order_estimated_delivery_date as date) as fecha_entrega_estimada,
    cast(i.shipping_limit_date as date) as fecha_limite_envio,
    i.price,
    i.freight_value,
    date_diff('second', p.order_purchase_timestamp, p.order_approved_at)
    / 86400.0 as dias_hasta_aprobacion,
    date_diff('second', p.order_purchase_timestamp, p.order_delivered_customer_date)
    / 86400.0 as dias_hasta_entrega,
    date_diff('second', p.order_estimated_delivery_date, p.order_delivered_customer_date)
    / 86400.0 as dias_retraso,
    p.order_delivered_customer_date > p.order_estimated_delivery_date as es_entrega_tardia,
    p.ultimo_evento_at as _visible_desde
from {{ ref('stg_order_items') }} as i
inner join pedidos as p on i.order_id = p.order_id
left join {{ ref('int_cliente_direcciones') }} as c on p.customer_id = c.customer_id
