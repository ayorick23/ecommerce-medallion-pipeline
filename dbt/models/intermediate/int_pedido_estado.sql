-- Estado de cada pedido a la fecha D (ADR 0031): la cancelación y la no
-- disponibilidad salen de order_status y ganan siempre; el resto de la
-- progresión, de la fila vigente del SCD2. invoiced y processing no tienen
-- timestamp, así que quedan en la etapa que diga el SCD2 (normalmente aprobado).
--
-- ultimo_evento_at es el valid_from de la fila vigente: como valid_from nunca
-- retrocede (ADR 0019), es el instante del último evento visible del pedido,
-- la marca de agua de fct_pedidos (ADR 0029).
select
    o.order_id,
    case
        o.order_status
        when 'canceled'
        then 'cancelado'
        when 'unavailable'
        then 'no_disponible'
        else h.status_event
    end as estado,
    h.valid_from as ultimo_evento_at
from {{ ref('stg_orders') }} as o
left join {{ ref('stg_order_status_history') }} as h on o.order_id = h.order_id and h.is_current
