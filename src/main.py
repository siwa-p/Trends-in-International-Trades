import os
import sys
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from pyspark.sql import SparkSession
import numpy as np
import asyncio

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from src.utils.logger_config import logger
from src.utils.utilities import create_spark_session
from datetime import datetime

load_dotenv(override=True)

app = FastAPI()

spark = create_spark_session()


@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok", "datetime": datetime.utcnow().isoformat()}


@app.get("/wits_tariff_trade")
async def read_wits_trade_tariff(
    data_type: str = "annual_trade",
    offset: int = 0,
    limit: int = 10,
    partner: str = None,
    year: int = None
):
    try:
        filters = []
        if partner:
            filters.append(f"PARTNER_CTY = '{partner}'")
        if year:
            filters.append(f"TIME_PERIOD = {year}")

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        query = f"""
            SELECT *
            FROM nessie.silver.bronze_wits_{data_type}
            {where_clause}
            LIMIT {limit} OFFSET {offset}
        """
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading WITS {data_type} data: {e}")
        return {"error": str(e)}

@app.get("/port_import_export")
async def read_port_data(
    data_type: str = "import",
    country_name: str = None,
    year: int = None,
    limit: int = 10,
    offset: int = 0
    ):
    try:
        filters = []
        if country_name:
            filters.append(f"CTY_NAME = '{country_name}'")
        if year:
            if data_type == "export":
                filters.append(f"EXPORT_YEAR = {year}")
            else:
                filters.append(f"IMPORT_YEAR = {year}")
        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)
        query = f"""SELECT * FROM nessie.silver.bronze_port_{data_type} 
                    {where_clause}
                    LIMIT {limit} OFFSET {offset}
                """
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        tariff_pd = tariff_pd.fillna('-')
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading bronze port level {data_type} data: {e}")
        return {"error": str(e)}

@app.get("/state_import_export")
async def read_state_data(
    data_type: str = "import",
    country_name: str = None,
    year: int = None,
    state: str = None,
    limit: int = 10,
    offset: int = 0
    ):
    try:
        filters = []
        if country_name:
            filters.append(f"CTY_NAME = '{country_name}'")
        if state:
            filters.append(f"STATE_ID = '{state}'")
        if year:
            if data_type == "export":
                filters.append(f"EXPORT_YEAR = {year}")
            else:
                filters.append(f"IMPORT_YEAR = {year}")
        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)
        query = f"""SELECT * FROM nessie.silver.bronze_state_{data_type} 
                    {where_clause}
                    LIMIT {limit} OFFSET {offset}
                """
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        tariff_pd = tariff_pd.fillna('-')
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading bronze state level {data_type} data: {e}")
        return {"error": str(e)}

if __name__ == '__main__':
    import time
    start = time.time()
    asyncio.run(read_wits_trade_tariff())
    # asyncio.run(read_port_data())
    end = time.time()
    print(f"Time taken: {end - start} seconds")