import os
import sys
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI
from pyspark.sql import SparkSession

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from utils.logger_config import logger
from utils.utilities import create_spark_session
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
        query = f"SELECT * FROM nessie.wits.tariff LIMIT {limit}"
        tariff = spark.sql(query)
        tariff_pd = tariff.toPandas()
        return tariff_pd.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error reading WITS tariff data: {e}")
        return {"error": str(e)}
