{{ config(
    materialized='table',
    format='iceberg',
    schema='silver',
    on_schema_change='ignore',
    partition_by=['bucket(32, CTY_NAME)', 'bucket(200, E_COMMODITY)', '"YEAR"']
) }}
select *
from "wits-data".export_hs
