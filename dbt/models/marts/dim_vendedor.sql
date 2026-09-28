-- Las coordenadas pueden faltar: no todo código postal tiene geolocalización
-- y eso no es un error (ADR 0035).
select
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat,
    g.geolocation_lng
from {{ ref('stg_sellers') }} as s
left join
    {{ ref('stg_geolocation') }} as g on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
