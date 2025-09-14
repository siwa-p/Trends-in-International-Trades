{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver',
    partition_by=['bucket(32, CTY_NAME)', 'bucket(200, I_COMMODITY)', '"YEAR"']
) }}
select *
from "wits-data".import_data_port