select
  region_en,
  count(*) as site_count
from {{ ref('stg_unesco_sites') }}
where category = 'Cultural'
group by 1
order by site_count desc
