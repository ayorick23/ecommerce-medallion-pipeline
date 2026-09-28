select
    order_id,
    status_event,
    order_status_raw,
    event_timestamp,
    valid_from,
    valid_to,
    is_current,
    is_adjusted
from {{ source('silver', 'order_status_history') }}
