-- Una fila por review (ADR 0034): COUNT(*) y AVG(review_score) son correctos
-- sin DISTINCT. Incremental con merge por review_id (ADR 0029).
--
-- La marca de agua no puede ser solo la creación: una review adelantada
-- (ADR 0020) entra a Silver recién cuando llega su pedido, con una creación
-- anterior a lo ya cargado. Se toma el mayor de los instantes que la hacen
-- visible o la cambian: creación, compra de su primer pedido y respuesta.
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='review_id',
        on_schema_change='fail'
    )
}}

with
    vinculos as (

        select
            r.review_id,
            r.review_score,
            r.review_comment_title,
            r.review_comment_message,
            r.review_creation_date,
            r.review_answer_timestamp,
            o.order_purchase_timestamp,
            c.customer_unique_id
        from {{ ref('stg_order_reviews') }} as r
        inner join {{ ref('stg_orders') }} as o on r.order_id = o.order_id
        inner join {{ ref('int_cliente_direcciones') }} as c on o.customer_id = c.customer_id

    ),

    reviews as (

        -- Los atributos de la review son iguales en todos sus vínculos, y todos
        -- sus pedidos son de la misma persona (tests singulares).
        select
            review_id,
            any_value(review_score) as review_score,
            any_value(review_comment_title) as review_comment_title,
            any_value(review_comment_message) as review_comment_message,
            any_value(review_creation_date) as review_creation_date,
            any_value(review_answer_timestamp) as review_answer_timestamp,
            any_value(customer_unique_id) as customer_unique_id,
            greatest(
                any_value(review_creation_date),
                min(order_purchase_timestamp),
                any_value(review_answer_timestamp)
            ) as _visible_desde
        from vinculos
        group by review_id

    ),

    candidatas as (

        -- En incremental se filtra antes de los joins con dim_cliente, no después.
        select *
        from reviews
        {% if is_incremental() %} where _visible_desde > {{ marca_de_agua() }} {% endif %}

    ),

    primeras_versiones as (

        select distinct customer_unique_id, cliente_sk
        from {{ ref('int_cliente_direcciones') }}
        where numero_version = 1

    )

select
    r.review_id,
    -- Versión vigente a la creación; una review anterior a la primera versión
    -- de la persona (review adelantada) toma la primera.
    coalesce(v.cliente_sk, pv.cliente_sk) as cliente_sk,
    cast(r.review_creation_date as date) as fecha_creacion,
    cast(r.review_answer_timestamp as date) as fecha_respuesta,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    coalesce(trim(r.review_comment_message) <> '', false) as tiene_comentario,
    date_diff('second', r.review_creation_date, r.review_answer_timestamp)
    / 86400.0 as dias_hasta_respuesta,
    r._visible_desde
from candidatas as r
-- valid_to nulo va con coalesce y no con "is null or": un OR en la condición
-- impide el hash join por persona y DuckDB cae en un nested loop (23 s frente
-- a 0.4 s sobre Olist completo).
left join
    {{ ref('dim_cliente') }} as v
    on r.customer_unique_id = v.customer_unique_id
    and r.review_creation_date >= v.valid_from
    and r.review_creation_date < coalesce(v.valid_to, cast('9999-12-31' as timestamp))
left join primeras_versiones as pv on r.customer_unique_id = pv.customer_unique_id
