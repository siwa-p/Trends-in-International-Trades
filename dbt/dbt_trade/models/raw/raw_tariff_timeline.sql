{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver'
) }}
select *
from "wits-data"."tariff_timeline.parquet"