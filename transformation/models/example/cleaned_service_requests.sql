select
  unique_key,
  created_date,
  agency
from {{ source('nyc_311_service_requests', 'service_requests') }}
