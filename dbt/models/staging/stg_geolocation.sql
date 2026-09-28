select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng
from {{ source('silver', 'geolocation_agg') }}
