{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver',
    partition_by=['YEAR(date_tariff)'],
) }}
select 
    cast(hts6 as int) as hts6,
    "month" as date_tariff,
    base_mfn_rate,
    surcharge_rate,
    effective_ad_val_rate
from nessie.tariff_timeline