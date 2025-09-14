{{ config(
    materialized='view',
    on_schema_change = 'ignore',
    schema='staged',
    database = 'nessie'
) }}

SELECT
    FREQ,
    REPORTER,
    "PARTNER" AS PARTNER_CTY,
    PRODUCTCODE,
    "INDICATOR" AS TARIFF_INDICATOR,
    CAST(TIME_PERIOD AS INT) AS TIME_PERIOD,
    DATASOURCE,
    OBS_VALUE

FROM nessie.silver.raw_wits_tariff at BRANCH main