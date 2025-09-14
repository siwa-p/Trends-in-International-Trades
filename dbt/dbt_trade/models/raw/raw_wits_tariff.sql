{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver',
    partition_by=['bucket(32, "PARTNER")', 'bucket(200, "INDICATOR")', 'TIME_PERIOD']
) }}
select *
from "wits-data"."wits_tariff_data.parquet"