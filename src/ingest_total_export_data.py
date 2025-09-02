import requests
from utils.logger_config import logger
import os
from dotenv import load_dotenv
from utils.spark_connection import get_spark_session
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
    for year in range(2015, 2021):
        for month in range(1, 13):
            data = get_daily_data(year, month)
            if not data:
                logger.warning(f"No data for {year}-{str(month).zfill(2)}")
                return
            write_iceberg(data, spark)
            logger.info(f"Data for {year}-{str(month).zfill(2)} ingested successfully")
    return logger.info("Data ingestion completed.")

def create_iceberg_table(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.total_export_value_by_port (
        YEAR INT,
        MONTH INT,
        CTY_CODE INT,
        CTY_NAME STRING,
        PORT STRING,
        PORT_NAME STRING,
        ALL_VAL_MO LONG
    ) USING ICEBERG
    PARTITIONED BY (YEAR, MONTH)
    """)
    logger.info("Iceberg table 'total_export_value_by_port' created or already exists.")
             
def write_iceberg(data, spark):
    # data[0] contains column names, data[1:] contains rows
    columns = data[0]
    rows = data[1:]
    spark_df = spark.createDataFrame(rows, schema=columns)
    spark_df = spark_df.withColumn("YEAR", spark_df["YEAR"].cast("int")) \
                       .withColumn("MONTH", spark_df["MONTH"].cast("int")) \
                       .withColumn("CTY_CODE", spark_df["CTY_CODE"].cast("int")) \
                       .withColumn("ALL_VAL_MO", spark_df["ALL_VAL_MO"].cast("long"))
    spark_df.createOrReplaceTempView("temp_new_data")
    spark.sql("""
    MERGE INTO nessie.total_export_value_by_port t
    USING temp_new_data n
    ON t.YEAR = n.YEAR AND t.MONTH = n.MONTH AND t.PORT = n.PORT AND t.CTY_CODE = n.CTY_CODE
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

if __name__ == "__main__":
    spark = get_spark_session()
    create_iceberg_table(spark)
    data = ingest_data(spark)
    if data is not None:
        write_iceberg(data, spark)
    spark.stop()
    logger.info("Spark Session stopped.")


