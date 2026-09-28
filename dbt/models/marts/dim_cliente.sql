-- SCD2 de persona (ADR 0030): una fila por versión de dirección, con
-- intervalos semiabiertos [valid_from, valid_to) como el SCD2 de Silver.
with
    versiones as (

        select distinct
            cliente_sk,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            valid_from
        from {{ ref('int_cliente_direcciones') }}

    )

select
    v.cliente_sk,
    v.customer_unique_id,
    v.customer_zip_code_prefix,
    v.customer_city,
    v.customer_state,
    g.geolocation_lat,
    g.geolocation_lng,
    v.valid_from,
    lead(v.valid_from) over (partition by v.customer_unique_id order by v.valid_from) as valid_to,
    lead(v.valid_from) over (partition by v.customer_unique_id order by v.valid_from)
    is null as is_current
from versiones as v
left join
    {{ ref('stg_geolocation') }} as g on v.customer_zip_code_prefix = g.geolocation_zip_code_prefix
