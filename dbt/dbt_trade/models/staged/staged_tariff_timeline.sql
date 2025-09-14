{{ config(
    materialized='view',
    on_schema_change = 'ignore',
    schema='staged',
    database = 'nessie'
) }}
select 
    cast(hts6 as int) as hts6,
    "month" as date_tariff,
    base_mfn_rate,
    surcharge_rate,
    effective_ad_val_rate
from nessie.silver.raw_tariff_timeline at BRANCH main