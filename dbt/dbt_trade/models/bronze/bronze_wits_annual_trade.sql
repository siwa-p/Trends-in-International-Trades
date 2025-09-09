{{ config(
    materialized='table',
    on_schema_change = 'ignore',
    format='iceberg',
    schema='silver'
) }}

SELECT
    FREQ,
    REPORTER,
    PARTNER,
    PRODUCTCODE,
    "INDICATOR" AS TRADE_INDICATOR,
    CAST(TIME_PERIOD AS INT) AS TIME_PERIOD,
    DATASOURCE,
    OBS_VALUE

FROM trade.wits.annual_trade