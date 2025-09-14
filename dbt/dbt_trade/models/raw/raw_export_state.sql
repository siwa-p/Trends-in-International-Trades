{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver',
    partition_by=['bucket(32, CTY_NAME)', 'bucket(200, E_COMMODITY)', '"YEAR"']
) }}

select *
from "wits-data".export_statehs