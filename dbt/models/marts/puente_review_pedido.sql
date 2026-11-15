-- Vínculos review–pedido (ADR 0034). Incremental con merge por el par: un
-- pedido que llega después agrega su fila sin tocar las anteriores.
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['review_id', 'order_id'],
        on_schema_change='fail'
    )
}}

select
    r.review_id,
    r.order_id,
    greatest(r.review_creation_date, o.order_purchase_timestamp) as _visible_desde
from {{ ref('stg_order_reviews') }} as r
inner join {{ ref('stg_orders') }} as o on r.order_id = o.order_id
{% if is_incremental() %}
    where greatest(r.review_creation_date, o.order_purchase_timestamp) > {{ marca_de_agua() }}
{% endif %}
