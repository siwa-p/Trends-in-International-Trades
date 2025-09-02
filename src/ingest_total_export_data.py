import requests
from utils.logger_config import logger
import os
from dotenv import load_dotenv
from utils.spark_connection import get_spark_session
import tempfile
import csv
load_dotenv()
API_KEY = os.getenv("CENSUS_API_KEY")
# total export value by port for the month of January 2025
url = "https://api.census.gov/data/timeseries/intltrade/exports/porths"
def get_daily_data(year, month):
    try:
        params = {
            "YEAR": str(year),
            "MONTH": str(month).zfill(2),
            "get": "CTY_CODE,CTY_NAME,PORT,PORT_NAME,ALL_VAL_MO",
            "key": API_KEY
        }
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        logger.info("Data fetched successfully")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return None

def ingest_data(spark):
    for year in [2020, 2021, 2022, 2023, 2024, 2025]:
        for month in range(1, 13):
            data = get_daily_data(year, month)
            if not data:
                logger.warning(f"No data for {year}-{str(month).zfill(2)}")
                return
            write_iceberg(data, spark)
            logger.info(f"Data for {year}-{str(month).zfill(2)} ingested successfully")
    return logger.info("Data ingestion completed.")
                
def write_iceberg(data, spark):
    with tempfile.NamedTemporaryFile(mode="w", delete=False, newline="") as tmpfile:
        writer = csv.writer(tmpfile)
        writer.writerows(data)
        tmp_path = tmpfile.name
    spark_df = spark.read.csv(tmp_path, header=True, inferSchema=True)
    spark_df.createOrReplaceTempView("temp_new_data")
    spark.sql("""
    MERGE INTO nessie.total_export_value_by_port t
    USING temp_new_data n
    ON t.year = n.year AND t.month = n.month AND t.port = n.port AND t.cty_code = n.cty_code
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if __name__ == "__main__":
    spark = get_spark_session()
    data = ingest_data(spark)
    if data is not None:
        write_iceberg(data, spark)
    spark.stop()
    logger.info("Spark Session stopped.")


