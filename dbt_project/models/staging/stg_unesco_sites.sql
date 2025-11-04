select
  id_number,
  name_en,
  category,
  region_en,
  date_inscribed::int as year_inscribed,
  short_description_en,
  longitude,
  latitude
from public.unesco_raw