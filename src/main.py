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


@app.get("/wits_tariff")
async def read_wits_tariff(limit: int = 10):
    try:
        query = f"SELECT * FROM nessie.silver.bronze_wits_tariff LIMIT {limit}"
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading WITS tariff data: {e}")
        return {"error": str(e)}

@app.get("/port_import")
async def read_port_data(limit: int = 10):
    try:
        query = f"SELECT * FROM nessie.silver.bronze_port_import LIMIT {limit}"
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        tariff_pd = tariff_pd.fillna('-')
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading bronze port level data: {e}")
        return {"error": str(e)}

if __name__ == '__main__':
    import time
    start = time.time()
    asyncio.run(read_port_data())
    end = time.time()
    print(f"Time taken: {end - start} seconds")