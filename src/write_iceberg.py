from utils.logger_config import logger
from utils.spark_connection import get_spark_session
from pyspark.sql.functions import sha2, concat_ws
from minio import Minio
import io
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv(override=True)

def get_data_from_lake(minio_client, bucket_name, object_name):
    try:
        response = minio_client.get_object(bucket_name, object_name)
        data = pd.read_parquet(io.BytesIO(response.read()))
        logger.info(f"Data fetched from MinIO bucket {bucket_name} as {object_name}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch data from MinIO: {e}")
        raise

def get_list_of_objects(minio_client,bucket_name):
    try:
        objects = minio_client.list_objects(bucket_name)
        object_list = [obj.object_name for obj in objects]
        logger.info(f"List of objects fetched from MinIO bucket {bucket_name}")
        return object_list
    except Exception as e:
        logger.error(f"Failed to list objects from MinIO: {e}")
        raise
        
def create_iceberg_tables(spark):
    spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.wits_trade_data (
        FREQ STRING,
        REPORTER STRING,
        PARTNER STRING,
        PRODUCTCODE STRING,
        INDICATOR STRING,
        TIME_PERIOD INT,
        DATASOURCE STRING,
        OBS_VALUE LONG,
        ROW_HASH STRING
    ) USING ICEBERG
    PARTITIONED BY (TIME_PERIOD)
    """)
    spark.sql("""
              CREATE TABLE IF NOT EXISTS nessie.wits_tariff_data (
                    FREQ STRING,
                    REPORTER STRING,
                    PARTNER STRING,
                    PRODUCTCODE STRING,
                    INDICATOR STRING,
                    TIME_PERIOD INT,
                    DATASOURCE STRING,
                    OBS_VALUE LONG,
                    ROW_HASH STRING
                ) USING ICEBERG
                PARTITIONED BY (TIME_PERIOD)
              """
              )
    
    logger.info("Iceberg table 'wits_trade_data' created or already exists.")
    logger.info("Iceberg table 'wits_tariff_data' created or already exists.")         
def write_trade_iceberg(data, spark):
    spark_data = spark.createDataFrame(data)
    spark_data = spark_data.withColumn("ROW_HASH", sha2(concat_ws("||", *spark_data.columns), 256))
    spark_data = spark_data.withColumn("TIME_PERIOD", spark_data["TIME_PERIOD"].cast("int")) \
                .withColumn("OBS_VALUE", spark_data["OBS_VALUE"].cast("long"))
    spark_data = spark_data.dropDuplicates(["REPORTER","PARTNER", "INDICATOR", "PRODUCTCODE","TIME_PERIOD", "DATASOURCE"])
    spark_data.createOrReplaceTempView("temp_new_data")
    spark.sql("""
    MERGE INTO nessie.wits_trade_data t
    USING temp_new_data n
    ON t.ROW_HASH = n.ROW_HASH
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)    
    
def write_tarrif_iceberg(data, spark):
    spark_data = spark.createDataFrame(data)
    spark_data = spark_data.withColumn("ROW_HASH", sha2(concat_ws("||", *spark_data.columns), 256))
    spark_data = spark_data.withColumn("TIME_PERIOD", spark_data["TIME_PERIOD"].cast("int")) \
                .withColumn("OBS_VALUE", spark_data["OBS_VALUE"].cast("long"))
    spark_data = spark_data.dropDuplicates(["REPORTER", "PARTNER", "INDICATOR", "PRODUCTCODE", "TIME_PERIOD", "DATASOURCE"])
    spark_data.createOrReplaceTempView("temp_new_data")
    spark.sql("""
    MERGE INTO nessie.wits_tariff_data t
    USING temp_new_data n
    ON t.ROW_HASH = n.ROW_HASH
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)  
    
def main():
    spark = get_spark_session()
    create_iceberg_tables(spark)
    minio_client = Minio(
        os.getenv("MINIO_EXTERNAL_URL"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    objects_list = get_list_of_objects(minio_client, os.getenv("MINIO_BUCKET_NAME"))
    trade_data_objects = [object.split('.')[0] for object in objects_list if 'trade' in object]
    tariff_data_objects = [object.split('.')[0] for object in objects_list if 'tariff' in object]
    for trade_object in trade_data_objects:
        trade_data = get_data_from_lake(minio_client, os.getenv("MINIO_BUCKET_NAME"), f"{trade_object}.parquet")
        write_trade_iceberg(trade_data, spark)
        logger.info(f"Trade data from {trade_object}.parquet ingested into Iceberg table successfully")
    for tariff_object in tariff_data_objects:
        tariff_data = get_data_from_lake(minio_client, os.getenv("MINIO_BUCKET_NAME"), f"{tariff_object}.parquet")
        write_tarrif_iceberg(tariff_data, spark)
        logger.info(f"Tariff data from {tariff_object}.parquet ingested into Iceberg table successfully")
    spark.stop()
    
if __name__ == "__main__":
    main()